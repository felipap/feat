
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
  "user": {
    "is_live": True,
    "pivots": ["id", "CMONTH(date)"]
  },
  "order": {
    "pivots": ["id"],
    "pointers": {
      "customer": "user.id",
    },
  },
  "order_item": {
    "pivots": ["id"],
    "pointers": {
      "order": "order.id",
      "product": "product.id",
    },
  },
  "product": {
    "pivots": ["id"],
  },
}


FEATURES = [
  # "JSON_GET(customer.shipping_address,\"['state']\")",
  "ACCUMULATE(Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date)))",
  "ACC(Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date)))",
  # "MAGIC(Order_item{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date)))"
]


async def main():
  dataframes = load_namespace_from_file('/Users/felipe/data.json')
  dataframes['user'].rename(columns={ '__ts__': 'CMONTH(date)' }, inplace=True)
  pickle.dump(dataframes, open('/Users/felipe/dev/assemblertest.pickle', 'wb'))
  dataframes = pickle.load(open('/Users/felipe/dev/assemblertest.pickle', 'rb'))

  shape = {
    'features': FEATURES,
    "output": {
      'date_block': 'CMONTH(date)',
      'pivots': ['CMONTH(date)', 'customer'],
      'pointers': {'customer': 'user.id'}
    },
    "date_range": ["2017-11", "2019-7"],
  }
  
  result = assembler.assemble(shape, TYPES, dataframes)
  pickle.dump(result, open('/Users/felipe/dev/assemblertest_result.pickle', 'wb'))
  result = pickle.load(open('/Users/felipe/dev/assemblertest_result.pickle', 'rb'))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
