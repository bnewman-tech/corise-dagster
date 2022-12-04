from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.types import Aggregation, Stock
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
def process_data(context, StockList: List[Stock]) -> Aggregation:
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
def week_2_pipeline():
    stockList = get_s3_data()
    highestStock = process_data(stockList)
    put_redis_data(highestStock)
    put_s3_data(highestStock)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker", config=docker, resource_defs={"s3": s3_resource, "redis": redis_resource}
)
