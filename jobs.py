# this job should list the buckets contents and create 64 jobs each job backing up files
import logging
import os

import boto3
from kubernetes import client, config

# list_object lists from s3 bucket
# def list_object(bucket):
#     # use aws client library to list buckets from meyrin site
#     s3 = boto3.client(
#         's3',
#         aws_access_key_id=meyrin_access_key,
#         aws_secret_access_key=meyrin_secret_key,
#         endpoint_url=os.environ["RCLONE_CONFIG_MEYRIN_ENDPOINT"]
#     )
#     continuation_token = None
#     while True:
#         response = s3.list_objects_v2(Bucket=bucket, ContinuationToken=continuation_token)
#         for obj in response.get('Contents', []):
#             yield obj['Key']
#         if not response['IsTruncated']:
#             break
#         continuation_token = response['NextContinuationToken']


config.load_incluster_config()
api = client.BatchV1beta1Api()
secretAPI = client.CoreV1Api()

# get buckets from the cronjob specs
buckets = os.environ.get("BUCKET_LIST")
s3_buckets = buckets.split(",")

# get meyrin bucket creds
meyrin_secret_name = os.environ["MEYRIN_SECRET_KEY"]
meyrin_access_key_field = "INVENIO_S3_ACCESS_KEY"
meyrin_secret_key_field = "INVENIO_S3_SECRET_KEY"

meyrin_secret = secretAPI.read_namespaced_secret(meyrin_secret_name, "jimil-test")
meyrin_access_key = meyrin_secret.data[meyrin_access_key_field]
meyrin_secret_key = meyrin_secret.data[meyrin_secret_key_field]

# get prevessin bucket creds
prevessin_secret_name = os.environ["PREVESSIN_SECRET_KEY"]
prevessin_access_key_field = "RCLONE_CONFIG_S3_ACCESS_KEY_ID"
prevessin_secret_key_field = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY"

prevessin_secret = secretAPI.read_namespaced_secret(prevessin_secret_name, "jimil-test")
prevessin_access_key = meyrin_secret.data[prevessin_access_key_field]
prevessin_secret_key = meyrin_secret.data[prevessin_secret_key_field]


test_bucket = s3_buckets[0]
# use aws client library to list buckets from meyrin site
s3 = boto3.client(
    "s3",
    aws_access_key_id=meyrin_access_key,
    aws_secret_access_key=meyrin_secret_key,
    endpoint_url=os.environ["RCLONE_CONFIG_MEYRIN_ENDPOINT"],
)

paginator = s3.get_paginator("list_object_v2")
page_iterator = paginator.paginate(
    Bucket=test_bucket, PaginationConfig={"PageSize": 1000}
)

for page in page_iterator:
    for obj in page["Contents"]:
        print(obj["Key"])

# Approach could be like - here take query 1k files and spawn a job for it. do it for 64 jobs by putting parallelism to 64
# do it for 16 buckets using for loop.
#
# since it is for this image that spawns the job - should the parallelism be set? because job is spawned for every 1k files fetched. also need to fetch the bucket names.
# job template should be set in this code.
# for bucket in s3_buckets:
#     files = [obj for obj in list_object(bucket)]
#     # divide these files into 64 "chunks"
#     chunk_size = len(files)//64+1
#     file_chunks = [files[i:i+chunk_size] for i in range(0, len(files), chunk_size)]

#     for i, chunk in enumerate(file_chunks):
#         destination_bucket = s3_buckets[i%len(destination_bucket)]

#         # here goes the job manifest
#         job_yaml = {

#         }

#         api.create_namespaced_job(namespace='', body=job_yaml)
