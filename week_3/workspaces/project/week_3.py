from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    String,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
    ScheduleEvaluationContext,
)
from workspaces.types import Aggregation, Stock
from workspaces.project.sensors import get_s3_keys
from typing import List
from datetime import datetime
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    out={"StockList": Out(dagster_type=List[Stock], description="List of stocks from csv file. ")},
)
def get_s3_data(context) -> List[Stock]:
    """
    This function gets data from an S3 bucket using a given key.

    :param context: Execution context for the operation.
    :type context: DagsterContext
    :returns: A list of Stock objects.
    :rtype: List[Stock]
    :raises: Dagster S3ResourceError if S3 resource is not found.
    """
    s3_key = context.op_config["s3_key"]
    result = [Stock.from_list(row) for row in context.resources.s3.get_data(s3_key)]
    return result


@op(ins={"StockList": In(dagster_type=List[Stock], description="List of stocks from data source")})
def process_data(StockList: List[Stock]) -> Aggregation:
    """
    This function takes in a list of stocks and returns the highest price out of the list.

    Parameters:
        StockList (List[Stock]): List of stocks from data source.

    Returns:
        Aggregation: Object with highest stock price and associated date.
    """
    highest = Aggregation(date=datetime.now(), high=0)
    for item in StockList:
        if item.high > highest.high:
            highest = Aggregation(date=item.date, high=item.high)
    return highest


@op(
    required_resource_keys={"redis"},
    ins={"stock": In(dagster_type=Aggregation, description="The stock item with the highest value.")},
)
def put_redis_data(context, stock: Aggregation) -> Nothing:

    """
    Put data into redis

    Args:
        context (Context): The context object for the execution of a pipeline execution.
        stock (Aggregation): The stock item with the highest value.

    Returns:
        Nothing
    """
    context.resources.redis.put_data(name=str(stock.date), value=str(stock.high))


@op(
    required_resource_keys={"s3"},
    ins={"stock": In(dagster_type=Aggregation, description="The stock item with the highest value.")},
)
def put_s3_data(context, stock: Aggregation):
    """
    This function puts a stock item with the highest value to an S3 bucket.

    Args:
        context (dagster.Context): The system context.
        stock (Aggregation): The stock item with the highest value.

    Returns:
        None
    """
    context.resources.s3.put_data(key_name=str(stock.date), data=stock)


@graph
def week_3_pipeline():
    stockList = get_s3_data()
    highestStock = process_data(stockList)
    put_redis_data(highestStock)
    put_s3_data(highestStock)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(partition_key: str):
    partition_config = docker.copy()
    partition_config["ops"] = {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}}
    return partition_config


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)

# Create a Schedule
week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local, cron_schedule="*/15 * * * *")


@schedule(job=week_3_pipeline_docker, cron_schedule="0 * * * *")
def week_3_schedule_docker(context: ScheduleEvaluationContext):
    """Define a schedule for the week_3_pipeline_docker pipeline in the Dagster framework.

    Args:
        context (ScheduleEvaluationContext): The context in which the schedule is being evaluated.

    Returns:
        RunRequest: A RunRequest object specifying the run key and run configuration to use when running the pipeline.
    """
    return RunRequest(
        run_key=context.scheduled_execution_time.strftime("%Y-%m-%d %H:%M:%S"), run_config=docker_config()
    )


@sensor(job=week_3_pipeline_docker, minimum_interval_seconds=30)
def week_3_sensor_docker(context):
    """Define a sensor for the week_3_pipeline_docker pipeline in the Dagster framework.

    The sensor checks for new files in an S3 bucket, and if any are found, it yields a RunRequest object for each file,
    specifying the run key and run configuration to use when running the pipeline. If no new files are found, the sensor
    yields a SkipReason object.

    Args:
        context: The context in which the sensor is being evaluated.

    Yields:
        RunRequest: A RunRequest object specifying the run key and run configuration to use when running the pipeline.
        SkipReason: A SkipReason object indicating that no new files were found in the S3 bucket.
    """
    new_files_in_s3 = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")
    sensor_docker_config = docker.copy()
    if not new_files_in_s3:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for file in new_files_in_s3:
        sensor_docker_config["ops"]["get_s3_data"]["config"]["s3_key"] = f"{file}"
        yield RunRequest(run_key=file, run_config=sensor_docker_config)
