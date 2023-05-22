import os
import tempfile
import time

import boto3
from kubernetes import client, config

config.load_incluster_config()
api = client.BatchV1beta1Api()
secretAPI = client.CoreV1Api()
batchAPI = client.BatchV1Api()
# get buckets from the cronjob specs
buckets = os.environ.get("BUCKET_LIST")
s3_buckets = buckets.split(",")
test_bucket = s3_buckets[0]

# use aws client library to list buckets from meyrin site
meyrin_s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ["INVENIO_S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["INVENIO_S3_SECRET_KEY"],
    endpoint_url=os.environ["RCLONE_CONFIG_MEYRIN_ENDPOINT"],
)


def cleanup_configmap(config_map):
    # print(f"cleaning up config map - {config_map}")
    try:
        secretAPI.delete_namespaced_config_map(
            name=config_map,
            namespace=os.environ["NAMESPACE"],
            body=client.V1DeleteOptions(),
        )
        # print(f"{config_map} is deleted")
    except client.rest.ApiException as e:
        if e.status == 404:
            print(f"{config_map} not found")
            return


def backup(bucket_name):
    # paginate over S3 Objects
    paginator = meyrin_s3.get_paginator("list_objects_v2")
    page_iterator = iter(
        paginator.paginate(Bucket=bucket_name, PaginationConfig={"PageSize": 1000})
    )

    page_num = 0

    while True:
        jobs = batchAPI.list_namespaced_job(
            namespace=os.environ["NAMESPACE"],
        ).items

        # list all running jobs
        running_jobs = [job for job in jobs if job.status.active is not None]
        # list all completed jobs
        completed_jobs = [
            job
            for job in jobs
            if job.status.succeeded is not None or job.status.failed is not None
        ]

        # cleanup completed jobs and pods
        for jobs in completed_jobs:
            print(f"{jobs.metadata.name} is completed - deleting")
            label_selector = f"job-name={jobs.metadata.name}"
            try:
                batchAPI.delete_namespaced_job(
                    name=jobs.metadata.name, namespace=os.environ["NAMESPACE"]
                )
            except client.rest.ApiException as e:
                if e.status == 404:
                    print(f"{jobs.metadata.name} deleted")
            try:
                pod = secretAPI.list_namespaced_pod(
                    namespace=os.environ["NAMESPACE"], label_selector=label_selector
                ).items
                print(f"pod length: {len(pod)}")
                for p in pod:
                    print(f"{p.metadata.name} pod is being deleted")
                # delete the pod
                secretAPI.delete_namespaced_pod(
                    name=pod.metadata.name, namespace=pod.metadata.namespace
                )
                print(f"{pod.metadata.name} is deleted")
            except client.rest.ApiException as e:
                print(e)
                if e.status == 404:
                    print(f"{pod.metadata.name} delete")
        completed_jobs.clear()
        # wait if running jobs exceeds the max parallel jobs
        if len(running_jobs) >= int(os.environ["TOTAL_JOBS"]):
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
        config_map = f"backup-job-cfg-{bucket_name}-{page_num}"
        cleanup_configmap(config_map)
        config_map_data = {os.path.basename(file_path): open(file_path).read()}
        metadata = client.V1ObjectMeta(name=config_map)
        config_map = client.V1ConfigMap(data=config_map_data, metadata=metadata)
        secretAPI.create_namespaced_config_map(
            namespace=os.environ["NAMESPACE"], body=config_map
        )

        command = [
            "/bin/sh",
            "-c",
            f"rclone copy meyrin:{bucket_name} s3:{bucket_name} --files-from={file_path}",
        ]
        if os.environ["DRY_RUN"] == "true":
            command = [
                "/bin/sh",
                "-c",
                f"rclone copy --dry-run meyrin:{bucket_name} s3:{bucket_name} --files-from={file_path}",
            ]
        container = client.V1Container(
            name="backup-container",
            image="rclone/rclone:1.56",
            command=command,
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
                    name="RCLONE_CONFIG_MEYRIN_ACCESS_KEY",
                    value=os.environ["INVENIO_S3_ACCESS_KEY"],
                ),
                client.V1EnvVar(
                    name="RCLONE_CONFIG_MEYRIN_SECRET_KEY",
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
                name=f"backup-job-cfg-{bucket_name}-{page_num}"
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
        cronjob = api.read_namespaced_cron_job(
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
        batchAPI.create_namespaced_job(namespace=os.environ["NAMESPACE"], body=job)
        page_num += 1


# backup all 16 buckets
for bucket in s3_buckets:
    backup(bucket)
