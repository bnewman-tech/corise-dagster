from dagster import (
    asset,
    with_resources,
    Nothing,
    String,
)
from workspaces.types import Aggregation, Stock
from typing import List
from datetime import datetime
from workspaces.resources import redis_resource, s3_resource


@asset(config_schema={"s3_key": String}, required_resource_keys={"s3"}, group_name="corise")
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


@asset(group_name="corise")
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


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
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


@asset(
    required_resource_keys={"s3"},
    group_name="corise",
)
def put_s3_data(context, stock: Aggregation):
    """
    Uploads data to an S3 bucket.

    :param context: The context object, used to access resources.
    :type context: object
    :param stock: Aggregated data to be uploaded to S3.
    :type stock: Aggregation
    :returns: None
    """
    context.resources.s3.put_data(key_name=str(stock.date), data=stock)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
)
