
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
  'lineitem': {
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
  """LAST_BEFORE(Order.LATEST(status|customer,DATE(created_at)))""",
  # """Order.COUNT_WHERE(status,"cancelado"|DATE(created_at),customer)""",
]

async def main():
  print("what bro")

  dataframes = pickle.load(open('/home/ubuntu/dataframes.pickle', 'rb'))

  print("loaded")

  # import ptvsd
  # ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
  # ptvsd.wait_for_attach()
  # breakpoint()

  next_week = datetime.now() + timedelta(6 - datetime.now().weekday())

  order_types = {
    'id': 'str',
    'created_at': 'int',
    'customer': 'int',
    'status': 'int',
    'cupom': 'int',
    'payment_type': 'int',
    'periodo_inicio': 'int',
    'periodo_fim': 'int',
    'is_recurrent': 'int',
    # 'DATE(created_at)': ''
    'is_kit': 'int',
    'shipping_address_postal_code': 'int',
    'shipping_address_district': 'int',
    'shipping_address_address_locality': 'int',
    'shipping_address_address_region': 'int',
    'discount_codes': 'int',
    'total_price': 'int',
    'subtotal_price': 'int',
    'freight_price': 'int',
    'total_discounts': 'int',
  }

  cols_and_types = {}
  for column, type in order_types.items():
    assert(type in ['int', 'str', 'float', 'bool'])
    cols_and_types[column] = pd.Series([], dtype=type)
  dataframes['order'] = pd.DataFrame(cols_and_types)

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
