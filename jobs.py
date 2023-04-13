# this job should list the buckets contents and create 64 jobs each job backing up files
import os
import tempfile

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


def backup_files(bucket_name, job_num):
    # paginate over S3 Objects
    paginator = meyrin_s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket_name, PaginationConfig={"PageSize": 1000}
    )

    pages_per_job = 64
    page_num = 0
    for page in page_iterator:
        if page_num % pages_per_job == job_num:
            job_name = f"backup-job-{bucket_name}-page-{page_num}"
            object_names = [obj["Key"] for obj in page.get("Contents", [])]

            # create a temporary file containing the list of files to copy
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                f.writelines([f"{name}\n" for name in object_names])
                file_path = f.name

            print(f"{file_path} is created")
            command = ["/bin/sh", "-c", f"rclone ls meyrin:{bucket_name}"]
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
                        name="RCLONE_CONFIG_S3_PROVIDER",
                        value=os.environ["RCLONE_CONFIG_S3_PROVIDER"],
                    ),
                    client.V1EnvVar(
                        name="INVENIO_S3_ACCESS_KEY",
                        value=os.environ["INVENIO_S3_ACCESS_KEY"],
                    ),
                    client.V1EnvVar(
                        name="INVENIO_S3_SECRET_KEY",
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
            template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(name=job_name),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[container],
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
            batchAPI.create_namespaced_job(namespace="jimil-test", body=job)
        page_num += 1


# backup all 16 buckets
for bucket in s3_buckets:
    # spawn 64 jobs
    for job in range(64):
        backup_files(bucket, job)
