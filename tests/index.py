
import sys
import asyncio
import pickle

sys.path.append('/Users/felipe/dev') # Make sure to pip3 install -e ~/dev/assembler
import assembler
sys.path.append('/Users/felipe/dev/oracle')
from brain.src.lib.data import fetch_namespace_dataframes
from brain.src.lib.state import translate_states

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
      # "EMAIL_DOMAIN(customer.email)",
      # "EMAIL_DOMAIN_EXT(customer.email)",
      # "JSON_GET(customer.shipping_address,\"['state']\")",
      "JSON_GET_FLEXPLAN(customer.flex_plans,\"[0]['shippingAddress']['state']\")",
      "JSON_GET_FLEXPLAN(customer.flex_plans,\"[0]['rushed']\")",
      # "FWD(JSON_GET_FLEXPLAN(customer.flex_plans,\"[0]['items'][0]['id']\"),1,CMONTH(date))",
      # "JSON_GET(customer.flex_plans,\"[0]['shippingAddress']['state']\")",
      # """JSON_GET(customer.flex_plans,"[0]['items'][0]['id']")""",
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

dataframes['Users'] = dataframes['Users'].iloc[:100]
# users = users.iloc[:100]

result = assembler.assemble(shape, type_config, dataframes)
pickle.dump(result, open('delete.pickle', 'wb'))


result = pickle.load(open('delete.pickle', 'rb'))

state_col = "JSON_GET(customer.shipping_address,\"['state']\")"

result[state_col] = translate_states(result[state_col])

# print(result[state_col].unique())

result
