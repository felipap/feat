
import os
import sys
import pickle
import asyncio
import json
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

import pandas as pd
from timeit import default_timer as timer

df_path = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(df_path)
import feat

TYPE_CONFIG = {
  "customer": {
    "key": "id",
    "date_key": "__date__",
  },
  "order": {
    "key": "id",
    "pointers": {
      "customer": "customer.id",
    },
  },
  "order_item": {
    "key": "id",
    "pointers": {
      "order": "order.id",
      "product": "product.id",
    },
  },
  "product": {
    "key": "id",
  },
}

FEATURES = [
  # "ACCUMULATE(Order_item.SUM(quantity|order.customer,DATE(order.date)),4)",
  # "ACCUMULATE(Order_item.SUM(quantity|order.customer,DATE(order.date)))",
  # "FUTURE_WITHIN(Order.COUNT(id|customer,DATE(date)),1)",
  # "FUTURE_WITHIN(Order.COUNT(id|customer,DATE(date)),1)",
  "FUTURE_WITHIN(Order.COUNT(id|customer,DATE(date)), 2)",
  # "DT_DAY_OF_THE_MONTH(__date__)",
  # "DT_MONTH_OF_THE_YEAR(__date__)",
  # "RANK(TIME_SINCE(Customer{customer=id}.created))",
  # "MATH_DIVIDE(ACCUMULATE(Order_item.SUM(quantity|order.customer,DATE(order.date))), TIME_SINCE(Customer{customer=id}.created_at))",
]

async def main():
  dataframes = pickle.load(open('/Users/felipe/Desktop/dataframes.pickle', 'rb'))

  import ptvsd
  ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
  ptvsd.wait_for_attach()
  # breakpoint()

  next_week = datetime.now() + timedelta(6 - datetime.now().weekday())
  
  frame = feat.assemble(
    FEATURES,
    dataframes,
    TYPE_CONFIG,
    dict(
      customer='customer.id',
      __date__=['2017-11-05', '{:%Y-%m-%d}'.format(next_week)],
    ),
    'week',
  )

  print(frame)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
