import os
import tempfile
import time

import boto3
from kubernetes import client, config

config.load_incluster_config()
core_api = client.CoreV1Api()
batch_api = client.BatchV1Api()

buckets = os.environ.get("BUCKET_LIST")
s3_buckets = buckets.split(",")

sync_buckets = os.environ.get("SYNC_LIST")
sync_buckets = [b.strip() for b in sync_buckets if b.strip()]

meyrin_s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ["INVENIO_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["INVENIO_S3_SECRET_KEY"],
    endpoint_url=os.environ["RCLONE_CONFIG_MEYRIN_ENDPOINT"],
)


def cleanup_configmap(config_map):
    try:
        core_api.delete_namespaced_config_map(
            name=config_map,
            namespace=os.environ["NAMESPACE"],
            body=client.V1DeleteOptions(),
        )
    except client.rest.ApiException as e:
        if e.status == 404:
            return

def parse_bucket(bucket_string: str):
    """Split bucket entry into bucket_name and optional prefix."""
    if "/" in bucket_string:
        bucket_name, prefix = bucket_string.split("/", 1)
        return bucket_name, prefix
    return bucket_string, None

def backup(bucket_name, prefix=None):
    # paginate over S3 Objects
    paginator = meyrin_s3.get_paginator("list_objects_v2")
    paginate_args = {"Bucket": bucket_name, "PaginationConfig": {"PageSize": 1000}}
    if prefix:
        paginate_args["Prefix"] = prefix
    page_iterator = iter(
        paginator.paginate(**paginate_args)
    )

    page_num = 0

    while True:
        jobs = batch_api.list_namespaced_job(
            namespace=os.environ["NAMESPACE"],
        ).items

        # list all running "backup" jobs
        # get the jobs by parent id
        running_jobs = [job for job in jobs if job.status.active is not None and job.metadata.owner_references and job.metadata.owner_references[0].name == os.environ["PARENT_NAME"]]
        #print("running jobs: ")
        # for j in running_jobs:
        #     print(j.metadata.name)
        # list all completed jobs
        completed_jobs = [
            job
            for job in jobs
            if job.status.succeeded is not None or job.status.failed is not None
        ]

        # cleanup completed jobs and pods and configMaps
        for jobs in completed_jobs:
            # cm and jobs have same name
            cleanup_configmap(jobs.metadata.name)
            print(f"{jobs.metadata.name} is completed - deleting")
            label_selector = f"job-name={jobs.metadata.name}"
            try:
                batch_api.delete_namespaced_job(
                    name=jobs.metadata.name, namespace=os.environ["NAMESPACE"]
                )
            except client.rest.ApiException as e:
                if e.status == 404:
                    print(f"{jobs.metadata.name} deleted")
            try:
                pod = core_api.list_namespaced_pod(
                    namespace=os.environ["NAMESPACE"], label_selector=label_selector
                ).items
                print(f"pod length: {len(pod)}")
                for p in pod:
                    print(f"{p.metadata.name} pod is being deleted")
                # delete the pod
                for p in pod:
                    core_api.delete_namespaced_pod(
                        name=p.metadata.name, namespace=p.metadata.namespace
                    )
                    print(f"{p.metadata.name} is deleted")
            except client.rest.ApiException as e:
                print(e)
                if e.status == 404:
                    print("deleted")
        completed_jobs.clear()
        # wait if running jobs exceeds the max parallel jobs
        if len(running_jobs) - 1 >= int(os.environ["TOTAL_JOBS"]):
            time.sleep(5)
            continue

        # spawn new job
        page = next(page_iterator, None)
        if not page:
            break

        job_name = f"backup-job-{bucket_name}-page-{page_num}"
        print(f"Spawning a new job {job_name}")
        object_names = [obj["Key"] for obj in page.get("Contents", [])]
        # create a temporary file containing the list of files to copy
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.writelines([f"{name}\n" for name in object_names])
            file_path = f.name
        config_map = f"backup-job-{bucket_name}-page-{page_num}"
        #cleanup_configmap(config_map)
        config_map_data = {os.path.basename(file_path): open(file_path).read()}
        metadata = client.V1ObjectMeta(name=config_map)
        config_map = client.V1ConfigMap(data=config_map_data, metadata=metadata)
        core_api.create_namespaced_config_map(
            namespace=os.environ["NAMESPACE"], body=config_map
        )

        action = "sync" if bucket_name in sync_buckets else "copy"
        print("action: ", action)
        print("bucket_name: ", bucket_name)
        print("sync_buckets: ", sync_buckets)
        command = [
            "/bin/sh",
            "-c",
            f"rclone {action} meyrin:{bucket_name} s3:{bucket_name} --files-from={file_path}",
        ]
        if os.environ["DRY_RUN"] == "true":
            command = [
                "/bin/sh",
                "-c",
                f"rclone {action} --dry-run meyrin:{bucket_name} s3:{bucket_name} --files-from={file_path}",
            ]
        container = client.V1Container(
            name="backup-container",
            image="rclone/rclone:1.56",
            command=command,
            # resources=client.V1ResourceRequirements(
            #     limits={
            #         "cpu": os.environ["CPU_LIMIT"],
            #         "memory": os.environ["MEMORY_LIMIT"],
            #     },
            #     requests={
            #         "cpu": os.environ["CPU_REQUEST"],
            #         "memory": os.environ["MEMORY_REQUEST"],
            #     },
            # ),
            volume_mounts=[
                client.V1VolumeMount(
                    name="files-volume",
                    mount_path=f"{file_path}",
                    sub_path=f"{os.path.basename(file_path)}",
                )
            ],
            # set the env for rclone
            env=[
                client.V1EnvVar(
                    name="RCLONE_CONFIG_MEYRIN_TYPE",
                    value=os.environ["RCLONE_CONFIG_MEYRIN_TYPE"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_MEYRIN_PROVIDER",
                    value=os.environ["RCLONE_CONFIG_MEYRIN_PROVIDER"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_MEYRIN_ENDPOINT",
                    value=os.environ["RCLONE_CONFIG_MEYRIN_ENDPOINT"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_S3_TYPE",
                    value=os.environ["RCLONE_CONFIG_S3_TYPE"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_S3_PROVIDER",
                    value=os.environ["RCLONE_CONFIG_S3_PROVIDER"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_S3_ENDPOINT",
                    value=os.environ["RCLONE_CONFIG_S3_ENDPOINT"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_MEYRIN_ACCESS_KEY_ID",
                    value=os.environ["INVENIO_S3_ACCESS_KEY"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_MEYRIN_SECRET_ACCESS_KEY",
                    value=os.environ["INVENIO_S3_SECRET_KEY"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_S3_ACCESS_KEY_ID",
                    value=os.environ["RCLONE_CONFIG_S3_ACCESS_KEY_ID"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_S3_SECRET_ACCESS_KEY",
                    value=os.environ["RCLONE_CONFIG_S3_SECRET_ACCESS_KEY"],
                ),
            ],
        )
        volume = client.V1Volume(
            name="files-volume",
            config_map=client.V1ConfigMapVolumeSource(
                name=f"backup-job-{bucket_name}-page-{page_num}"
            ),
        )

        template = client.V1PodTemplateSpec(
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                volumes=[volume],
            ),
        )

        # get cronjob uid
        cronjob = batch_api.read_namespaced_cron_job(
            name=os.environ["PARENT_NAME"], namespace=os.environ["NAMESPACE"]
        )
        cronjob_uid = cronjob.metadata.uid

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                owner_references=[
                    client.V1OwnerReference(
                        api_version="batch/v1beta1",
                        kind="CronJob",
                        name=os.environ["PARENT_NAME"],
                        controller=True,
                        block_owner_deletion=True,
                        uid=cronjob_uid,
                    )
                ],
            ),
            spec=client.V1JobSpec(
                template=template,
                backoff_limit=0,
            ),
        )
        # wait until the job is created
        batch_api.create_namespaced_job(namespace=os.environ["NAMESPACE"], body=job)
        time.sleep(2)
        page_num += 1

# backup all 16 buckets
for bucket in s3_buckets:
    bucket_name, prefix = parse_bucket(bucket)
    print(bucket_name)
    backup(bucket_name, prefix)
