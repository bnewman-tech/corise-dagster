import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String},
    out={"StockList": Out(dagster_type=List[Stock], description="List of stocks from csv file. ")},
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return list(csv_helper(s3_key))


@op(ins={"StockList": In(dagster_type=List[Stock], description="List of stocks from data source")})
def process_data(context, StockList: List[Stock]) -> Aggregation:
    highest = Aggregation(date=datetime.now(), high=0)
    for item in StockList:
        if item.high > highest.high:
            highest = Aggregation(date=item.date, high=item.high)
    return highest


@op(ins={"stock": In(dagster_type=Aggregation, description="The stock item with the highest value.")})
def put_redis_data(context, stock: Aggregation) -> Nothing:
    print("Writing the Highest Stock Level")
    print(stock)


@job
def week_1_pipeline():
    stockList = get_s3_data()
    highestStock = process_data(stockList)
    put_redis_data(highestStock)
