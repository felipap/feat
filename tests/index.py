
import sys
import asyncio
import pickle
import pandas as pd
from timeit import default_timer as timer

sys.path.append('/Users/felipe/dev') # Make sure to pip3 install -e ~/dev/assembler
import assembler
sys.path.append('/Users/felipe/dev/settler2')
from brain.src.lib.data import fetch_namespace_collections
from brain.src.live import VERB_make_live_users

if False:
  loop = asyncio.get_event_loop()
  dataframes = loop.run_until_complete(fetch_namespace_collections('verb'))
  pickle.dump(dataframes, open('dataframes.pickle', 'wb'))
dataframes = pickle.load(open('dataframes.pickle', 'rb'))


date_range = range(576, 593)

# dataframes['Users'] = dataframes['Users'].iloc[:1000]

# start = timer()
# Users = VERB_make_live_users(dataframes['Users'], date_range)
# pickle.dump(Users, open('Users.pickle', 'wb'))
# print("elapsed: %ds" % (timer() - start))
Users = pickle.load(open('Users.pickle', 'rb'))

dataframes['Users'] = Users

type_config = {
  'users': {
    'pivots': ['id', 'CMONTH(date)'],
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
      "DOMAIN_EXT(EMAIL_DOMAIN(customer.email))",
      # "JSON_GET(customer.flex_plans,\"['shippingAddress']['state']\")",
      # "JSON_GET(customer.shipping_address,\"['state']\")",
      # "JSON_GET_FLEXPLAN(customer.flex_plans,\"['shippingAddress']['state']\")",
      # "TIME_SINCE(CMONTH(Users{customer=id}.created))",
      # "EMAIL_DOMAIN(customer.email)",
      # "JSON_GET(customer.shipping_address,\"['state']\")",
      # "JSON_GET_FLEXPLAN(customer.flex_plans,\"['rushed']\")",
      # "FWD(JSON_GET_FLEXPLAN(customer.flex_plans,\"['items'][0]['id']\"),1,CMONTH(date))",
      # "JSON_GET(customer.flex_plans,\"['shippingAddress']['state']\")",
      # """JSON_GET(customer.flex_plans,"[0]['items'][0]['id']")""",
      # """PARSE_FLEX_PLANS(customer.flex_plans)""",
      # """GREATERTHAN(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),0)""",
      # "FWD(      ACC(        Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}          .SUM(quantity|order.customer,CMONTH(order.date))        ),1,CMONTH(date))""",
      # """FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),1,CMONTH(date))""",
      # """FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),2,CMONTH(date))""",
      # """FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),3,CMONTH(date))""",
      """
      FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.
        TSINCESEEN(CMONTH(order.date),CMONTH(order.date)|order.customer,CMONTH(order.date))
        ,1,CMONTH(date))""",
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
pickle.dump(result, open('/Users/felipe/delete.pickle', 'wb'))
result = pickle.load(open('/Users/felipe/delete.pickle', 'rb'))

result.fillna(0, inplace=True)

tsince = 'FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.TSINCESEEN(CMONTH(order.date),CMONTH(order.date)|order.customer,CMONTH(order.date)),1,CMONTH(date))'
print(result[(result[tsince]==0)&(result['CMONTH(date)']==591)].shape)


print(result[state_col].unique())

result
