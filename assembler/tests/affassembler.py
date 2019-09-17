
import os
import sys
import pickle
import tempfile
# import pyjson5
import traceback
import asyncio
import json

import pandas as pd
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
    "types": {
      "created": "datetime64[ns]",
    },
    "is_live": True,
    "pivots": ["id", "CMONTH(date)"]
  },
  "order": {
    "types": {
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
  # "text": {
  #   "pivots": ["id"],
  #   "pointers": {
  #     "customer": "customer.id",
  #   },
  # },
  "product": {
    "pivots": ["id"],
  },
}


FEATURES = [
  "Order_item{__date__=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer)",
]


async def main():
  # dataframes = load_namespace_from_file('/Users/felipe/data.json')
  # dataframes['user'].rename(columns={ '__ts__': 'CMONTH(date)' }, inplace=True)
  # pickle.dump(dataframes, open('./neuron_trash_assemblertest.pickle', 'wb'))
  # dataframes = pickle.load(open('./neuron_trash_assemblertest.pickle', 'rb'))
  dataframes = pickle.load(open('./neuron_trash_jobs_first.pickle', 'rb'))
  # dataframes = pickle.load(open('./neuron_assembler_small_df.pickle', 'rb'))

  config = {
    # 'pivots': ['CMONTH(date)', 'customer'],
    'date_block': 'CMONTH(date)',
    'pointers': {'customer': 'customer.id'},
    "date_range": ["2017-11", "2019-7"],
  }
  
  print(assembler)
  result = assembler.assemble(FEATURES, config, TYPES, dataframes)
  pickle.dump(result, open('./neuron_trash_assemblertest_result.pickle', 'wb'))
  result = pickle.load(open('./neuron_trash_assemblertest_result.pickle', 'rb'))

  # print(result[result['customer']=='5b69c4240998ba2b42de9531'])


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
