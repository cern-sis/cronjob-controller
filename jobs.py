# this job should list the buckets contents and create 64 jobs each job backing up files
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
    try:
        secretAPI.delete_namespaced_config_map(
            name=config_map,
            namespace=os.environ["NAMESPACE"],
            body=client.V1DeleteOptions(),
        )
    except client.rest.ApiException as e:
        if e.status == 404:
            return


# approach II
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
        running_jobs = [job for job in jobs if job.status.active is not None]
        completed_jobs = [
            job
            for job in jobs
            if job.status.succeeded is not None or job.status.failed is not None
        ]
        if len(running_jobs) >= 64:
            time.sleep(5)
            continue

        page = next(page_iterator, None)
        if not page:
            break

        job_name = f"backup-job-{bucket_name}-page-{page_num}"
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
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                template=template,
                backoff_limit=0,
            ),
        )
        batchAPI.create_namespaced_job(namespace=os.environ["NAMESPACE"], body=job)

        if len(running_jobs) + 1 > 64 and completed_jobs:
            oldest_job = min(
                completed_jobs, key=lambda job: job.metadata.creation_timestamp
            )
            batchAPI.delete_namespaced_job(
                name=oldest_job.metadata.name, namespace=os.environ["NAMESPACE"]
            )
        page_num += 1


def backup_files(bucket_name, job_num):
    # paginate over S3 Objects
    paginator = meyrin_s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket_name, PaginationConfig={"PageSize": 1000}
    )

    # divide the backup job into 64 sub jobs
    pages_per_job = 64
    # track the current page number
    page_num = 0
    for page in page_iterator:
        if page_num % pages_per_job == job_num:
            job_name = f"backup-job-{bucket_name}-page-{page_num}"
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
            job = client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=client.V1ObjectMeta(name=job_name),
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
    # for job in range(64):
    #     backup_files(bucket, job)
