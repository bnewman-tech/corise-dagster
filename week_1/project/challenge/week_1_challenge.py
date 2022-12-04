import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    Int,
    In,
    Nothing,
    Out,
    Output,
    String,
    job,
    op,
    usable_as_dagster_type,
)
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
    out={
        "stockList": Out(is_required=False, dagster_type=List[Stock]),
        "empty_stocks": Out(is_required=False, dagster_type=Any),
    },
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    stockList = list(csv_helper(s3_key))
    if len(stockList) == 0:
        yield Output(None, "empty_stocks")
    else:
        yield Output(stockList, "stockList")


@op(
    ins={"StockList": In(dagster_type=List[Stock], description="List of stocks from data source")},
    config_schema={"nlargest": Int},
    out=DynamicOut(),
)
def process_data(context, StockList: List[Stock]) -> Aggregation:
    nlargest = context.op_config["nlargest"]
    StockList.sort(key=lambda x: x.high, reverse=True)
    for stock in StockList[0:nlargest]:
        print(stock)
        yield DynamicOutput(Aggregation(date=stock.date, high=stock.high), mapping_key=str(int(stock.high)))


@op(ins={"stock": In(dagster_type=Aggregation, description="The stock item with the highest value.")})
def put_redis_data(context, stock: Aggregation) -> Nothing:
    print("Writing the Highest Stock Level")
    print(stock)


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    description="Notifiy if stock list is empty",
)
def empty_stock_notify(context, empty_stocks) -> Nothing:
    context.log.info("No stocks returned")


@job
def week_1_challenge():
    stockList, empty_stocks = get_s3_data()
    empty_stock_notify(empty_stocks)
    highestStock = process_data(stockList)
    highestStock.map(put_redis_data)
