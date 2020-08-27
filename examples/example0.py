
import os
import sys
import pickle
import asyncio
import json
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

import pandas as pd

df_path = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(df_path)
import feat

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 50)

TYPE_CONFIG = {
  'customer': {
    'key': 'id',
    'pointers': None,
    'date_key': '__date__'
  },
  'order': {
    'key': 'id',
    'pointers': {
    'customer_id': 'customer.id',
    'customer': 'customer.id'
    }
  },
  'line_item': {
    'key': 'id',
    'pointers': {
    'order_id': 'order.id',
    'order': 'order.id'
    }
  }
}

FEATURES = [
  # """TIME_SINCE_SEEN(Order.COUNT(id|DATE(created_at),customer))""",
  # """Order.WHERE(status,"cancelado")""",
  """Order.COUNT_WHERE(status,"cancelado"|DATE(created_at),customer)""",
]

async def main():
  dataframes = pickle.load(open('/home/ubuntu/dataframes.pickle', 'rb'))

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
