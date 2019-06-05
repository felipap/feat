
import sys
import asyncio
import pickle

sys.path.append('/Users/felipe/dev') # Make sure to pip3 install -e ~/dev/assembler
import assembler
sys.path.append('/Users/felipe/dev/oracle')
from brain.src.lib.data import fetch_namespace_dataframes

if False:
  loop = asyncio.get_event_loop()
  dataframes = loop.run_until_complete(fetch_namespace_dataframes('verb'))
  pickle.dump(dataframes, open('dataframes.pickle', 'wb'))
dataframes = pickle.load(open('dataframes.pickle', 'rb'))

type_config = {
  'users': {
    'pivots': ['id'],
    'column_cast': {
      'status': 'str',
      'unread': 'str',
      'created': 'datetime64[ns]',
      'customer_type': 'str',
    },
  },
  'orders': {
    'pivots': ['id'],
    'pointers': {
      'customer': 'users.id',
    },
    'column_cast': {
      'date': 'datetime64[ns]',
    },
  },
  'products': {
    'pivots': ['id'],
  },
  'order_items': {
    'pivots': ['id'],
    'pointers': {
      'order': 'orders.id',
      'product': 'products.id',
    },
  },
}

shape = {
    'date_range': ['2017-12', '2019-5'],
    'features': [
      """JSON_GET(customer.flex_plans,"[0]['items'][0]['id']")""",
      # """PARSE_FLEX_PLANS(customer.flex_plans)""",
      # """GREATERTHAN(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),0)""",
      # "FWD(      ACC(        Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}          .SUM(quantity|order.customer,CMONTH(order.date))        ),1,CMONTH(date))""",
      # """FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),1,CMONTH(date))""",
      # """FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),2,CMONTH(date))""",
      # """FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),3,CMONTH(date))""",
      # """
      # FWD(
      #   Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.
      #   TSINCESEEN(CMONTH(order.date),CMONTH(order.date)|order.customer,CMONTH(order.date))
      #   ,1,CMONTH(date))""",
      # """
      # FWD(
      #   MEAN_DIFF(
      #     Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.
      #       SUM(quantity|CMONTH(order.date),order.customer),CMONTH(date)
      #     ),1,CMONTH(date))""",
      # """TIME_SINCE(CMONTH(Users{customer=id}.created))""",
    ],
    "output": {
      'date_block': 'CMONTH(date)',
      'pivots': ['CMONTH(date)', 'customer'],
      'pointers': {'customer': 'Users.id', 'product': 'Products.id'}
    },
}

result = assembler.assemble(shape, type_config, dataframes)

result

