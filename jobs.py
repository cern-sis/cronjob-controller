# this job should list the buckets contents and create 64 jobs each job backing up files
import logging
import os

from kubernetes import client, config

config.load_incluster_config()
api = client.BatchV1beta1Api()

# get source buckets from the cronjob specs
source_buckets = os.environ.get("BUCKET_LIST")
bucket = source_buckets.split(",")
logging.info(bucket)
logging.info("job is created!!")
