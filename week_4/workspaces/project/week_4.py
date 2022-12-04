from dagster import asset, with_resources


@asset
def get_s3_data():
    pass


@asset
def process_data():
    pass


@asset
def put_redis_data():
    pass


@asset
def put_s3_data():
    pass


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources()
