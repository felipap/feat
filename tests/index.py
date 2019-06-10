
import sys
import asyncio
import pickle
import pandas as pd
from timeit import default_timer as timer

# Make sure to `pip3 install -e` the folder of these projects?
sys.path.append('/Users/felipe/dev')
import assembler
sys.path.append('/Users/felipe/dev/settler2')
from brain.src.live import load_namespace

# start = timer()
# loop = asyncio.get_event_loop()
# dataframes = loop.run_until_complete(load_namespace('verb', range(576, 593)))
# print("elapsed: %ds" % (timer() - start))
# pickle.dump(dataframes, open('dataframes.pickle', 'wb'))
dataframes = pickle.load(open('dataframes.pickle', 'rb'))

type_config = {
  'users': {
    'pivots': ['id', 'CMONTH(date)'],
    'column_cast': {
      'created': 'datetime64[ns]',
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
    "GREATERTHAN(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),0)",

    #"ACC(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(total_price|order.customer,CMONTH(order.date)))",

    "MINUSPREV(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer))",
    "JSON_GET(customer.shipping_address,\"['state']\")",
    "customer.school_delivery",
    "JSON_GET(customer.flex_plans,\"['items'][0]['id']\")",
    "TIME_SINCE(CMONTH(Users{customer=id}.created))",
    
    #"EMAIL_DOMAIN(customer.email)",
    "DOMAIN_EXT(EMAIL_DOMAIN(customer.email))",
            
    #Previously acc-ed !!! For some reason without FWD is being hard to train1
    "ACC(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date)))",
    "FWD(ACC(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|order.customer,CMONTH(order.date))),1)",
    
    #Order sums (this and prev months)
    "Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer)",
    "FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),1)",
    "FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),2)",
    "FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.SUM(quantity|CMONTH(order.date),order.customer),3)",
    
    #Dcount since last order (prev months)
    "Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.TSINCESEEN(CMONTH(order.date),CMONTH(order.date)|order.customer,CMONTH(order.date))",
    ##"FWD(Order_items{CMONTH(date)=CMONTH(order.date);customer=order.customer}.TSINCESEEN(CMONTH(order.date),CMONTH(order.date)|order.customer,CMONTH(order.date)),1)",
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

result
