
import os
import sys
import pickle
import tempfile
import pyjson5
import traceback
import asyncio
import json

import pandas as pd
import xgboost as xgb
from timeit import default_timer as timer

df_path = os.path.join(os.path.dirname(__file__), '../..')
sys.path.append(df_path)
import assembler


def load_namespace_from_file(filepath):
  print(f"Loading namespace from file ${filepath}")
  filedata = json.load(open(filepath))

  dfs = {}
  for utype, typerows in filedata.items():
    if 'snapshots' in typerows[0]:
      # It's live data, flatten to multiple rows.
      rows = []
      for obj in typerows:
        rows.extend(dict(__ts__=ts, **snap) for (ts, snap) in obj['snapshots'].items())
      dfs[utype] = pd.DataFrame(rows)
    else:
      dfs[utype] = pd.DataFrame(typerows)
  return dfs


TYPES = {
  "customer": {
    "column_cast": {
      "created": "datetime64[ns]",
    },
    "is_live": True,
    "pivots": ["id", "CMONTH(date)"]
  },
  "order": {
    "column_cast": {
      "date": "datetime64[ns]"
    },
    "pivots": ["id"],
    "pointers": {
      "customer": "customer.id",
    },
  },
  "order_item": {
    "pivots": ["id"],
    "pointers": {
      "order": "order.id",
      "product": "product.id",
    },
  },
  "text": {
    "pivots": ["id"],
    "pointers": {
      "customer": "customer.id",
    },
  },
  "product": {
    "pivots": ["id"],
  },
}


FEATURES = [
  "Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer)",
  # "JSON_GET(customer.flex_plan,\"['items'][0]['id']\")",
  # "CP_CHANGED(JSON_GET(customer.flex_plan,\"['items'][0]['id']\"))", 
  # "Order.LATEST(created|customer,CMONTH(date))",
  # "Order.LATEST(DT_DAY_OF_THE_WEEK(created)|customer,CMONTH(date))",
  # "Order.LATEST(DT_DAY_OF_THE_MONTH(created)|customer,CMONTH(date))",
  # "Order.LATEST(DT_MONTH_OF_THE_YEAR(created)|customer,CMONTH(date))",
  # "DT_DAY_OF_THE_MONTH(Customer{customer=id}.created)",
  # "DT_DAY_OF_THE_WEEK(Customer{customer=id}.created)",
  # "DT_MONTH_OF_THE_YEAR(Customer{customer=id}.created)",
  # "customer.flex_status",
  # "JSON_GET(customer.flex_plans,\"['discounts'][0]\")"
  # "TIME_SINCE(Customer{customer=id}.created)",
  # "Text{CMONTH(date)=CMONTH(timestamp)}.COUNT(id|CMONTH(timestamp),customer)",
  # "Order.SUM(JSON_GET(paid,\"['subtotal']\")|customer,CMONTH(date))",
  # "Order.LATEST(order_type|customer,CMONTH(date))",
  # "GREATERTHAN(Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),0)",
  # "JSON_GET(customer.shipping_address,\"['state']\")",
  # "STREND(Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date)))",
  # "ACC(Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date)))",
  # "MAGIC(Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date)))"
  # "SHIFT(Order{CMONTH(date)=CMONTH(date);customer=customer}.COUNT(id|customer,CMONTH(date)), 1)"
  # "Order.COUNT(id|customer,CMONTH(date))",
  # "CSINCE(Order.COUNT(id|customer,CMONTH(date)))",
  # "Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.TSINCESEEN(CMONTH(order.date),CMONTH(order.date)|order.customer,CMONTH(order.date))"
]


async def main():
  # dataframes = load_namespace_from_file('/Users/felipe/data.json')
  # dataframes['user'].rename(columns={ '__ts__': 'CMONTH(date)' }, inplace=True)
  # pickle.dump(dataframes, open('./neuron_trash_assemblertest.pickle', 'wb'))
  # dataframes = pickle.load(open('./neuron_trash_assemblertest.pickle', 'rb'))
  dataframes = pickle.load(open('./neuron_trash_jobs_first.pickle', 'rb'))

  shape = {
    'features': FEATURES,
    "output": {
      'date_block': 'CMONTH(date)',
      'pivots': ['CMONTH(date)', 'customer'],
      'pointers': {'customer': 'customer.id'}
    },
    "date_range": ["2017-11", "2019-7"],
  }
  
  result = assembler.assemble(shape, TYPES, dataframes)
  pickle.dump(result, open('./neuron_trash_assemblertest_result.pickle', 'wb'))
  result = pickle.load(open('./neuron_trash_assemblertest_result.pickle', 'rb'))

  # print(result[result['customer']=='5b69c4240998ba2b42de9531'])


loop = asyncio.get_event_loop()
loop.run_until_complete(main())