
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

df_path = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(df_path)
import feat


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
    "key": "id",
    # "types": {
    #   "created": "datetime64[ns]",
    # },
    "date_key": "__date__",
  },
  "order": {
    "key": "id",
    # "types": {
    #   "date": "datetime64[ns]"
    # },
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
  # "text": {
  #   "key": "id",
  #   "pointers": {
  #     "customer": "customer.id",
  #   },
  # },
  "product": {
    "key": "id",
  },
}


FEATURES = [
  # "Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|DATE(order.date),order.customer)",
  # "ACCUMULATE(CSINCE(Order.COUNT(id|customer,DATE(date))))",
  # "TIME_SINCE_SEEN(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|DATE(order.date),order.customer))",
  # "GREATERTHAN(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|DATE(order.date),order.customer),0)",
  # "TIME_SINCE(Customer{customer=id}.created)",
  # "Order.LATEST(JSON_GET(discounts,\"[0]['code']\")|customer,DATE(date))",
  # "Order.LATEST(JSON_GET(refund,\"[0]['status']\")|customer,DATE(date))",
  "STREND(Order.COUNT(id|DATE(date),customer))",
  # # "customer.school_delivery",
  # "customer.source",
  # "JSON_GET(customer.flex_plan,\"['items'][0]['id']\")",
  # "JSON_GET(customer.flex_plan,\"['items'][0]['quantity']\")",
  # # "JSON_GET(customer.flex_plan,\"['items'][0]['flavor']\")",
  # "customer.created",
  # "customer.flex_status",
  # # "SHIFT(customer.flex_status,1)",
  # # "SHIFT(customer.flex_status,2)",
  # "JSON_GET(customer.shipping_address,\"['state']\")",
  # "CSINCE(Order.COUNT(id|customer,DATE(date)))",
  # # "SHIFT(CSINCE(Order.COUNT(id|customer,DATE(date))),1)",
  # # "SHIFT(CSINCE(Order.COUNT(id|customer,DATE(date))),2)",
  # "DOMAIN_EXT(EMAIL_DOMAIN(customer.email))",
  # "ACCUMULATE(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|order.customer,DATE(order.date)))",
  # # "SHIFT(ACCUMULATE(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|order.customer,DATE(order.date))),1)",
  # # "SHIFT(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|DATE(order.date),order.customer),1)",
  # # "SHIFT(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|DATE(order.date),order.customer),2)",
  # # "SHIFT(TIME_SINCE_SEEN(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|DATE(order.date),order.customer)), 1)",
  # # "SHIFT(TIME_SINCE_SEEN(Order_item{__date__=DATE(order.date);customer=order.customer}.SUM(quantity|DATE(order.date),order.customer)), 2)",
  # "Order.SUM(JSON_GET(paid,\"['subtotal']\")|customer,DATE(date))",
  # "Order.SUM(JSON_GET(paid,\"['discountTotal']\")|customer,DATE(date))",
  # "Order.LATEST(order_type|customer,DATE(date))",
  # "Order.LATEST(source|customer,DATE(date))",
  # "Order.LATEST(JSON_GET(shipping,\"['shippingType']\")|customer,DATE(date))",
  # "Order.LATEST(DT_DAY_OF_THE_WEEK(created)|customer,DATE(date))",
  # "Order.LATEST(DT_DAY_OF_THE_MONTH(created)|customer,DATE(date))",
  # "Order.LATEST(DT_MONTH_OF_THE_YEAR(created)|customer,DATE(date))",
  # "CP_CHANGED(JSON_GET(customer.flex_plan,\"['items'][0]['id']\"))",
  # "CP_CHANGED(JSON_GET(customer.flex_plan,\"['items'][0]['quantity']\"))",
  # # "CP_CHANGED(JSON_GET(customer.flex_plan,\"['items'][0]['flavor']\"))",
]


async def main():
  # dataframes = load_namespace_from_file('/Users/felipe/data.json')
  # dataframes['user'].rename(columns={ '__ts__': 'DATE(date)' }, inplace=True)
  # pickle.dump(dataframes, open('./neuron_trash_assemblertest.pickle', 'wb'))
  # dataframes = pickle.load(open('./neuron_trash_assemblertest.pickle', 'rb'))
  dataframes = pickle.load(open('./neuron_trash_jobs_first.pickle', 'rb'))
  # dataframes = pickle.load(open('./neuron_assembler_small_df.pickle', 'rb'))

  dataframes['customer'].rename(columns={ 'CMONTH(date)': '__date__' }, inplace=True)
  # dataframes['customer'] = dataframes['customer'][dataframes['customer'].id == '5d430b531a55d200152297fb']
  # dataframes['order'] = dataframes['order'][dataframes['order'].customer == '5d430b531a55d200152297fb']

  print("dataframes", dataframes['customer'].size, dataframes['order'].size)
  
  # dataframes['customer']['__date__'] = dataframes['customer']['__date__'].astype('datetime64[ns]')

  import ptvsd
  ptvsd.enable_attach(address=('localhost', 5678), redirect_output=True)
  ptvsd.wait_for_attach()

  config = {
    'block_type': 'week',
    'pointers': {'customer': 'customer.id'},
    "date_range": ["2017-11-01", "2019-9-14"],
  }
  
  result = feat.assemble(FEATURES, config, TYPES, dataframes)
  pickle.dump(result, open('./neuron_trash_assemblertest_result.pickle', 'wb'))
  result = pickle.load(open('./neuron_trash_assemblertest_result.pickle', 'rb'))

  # print(result[result['customer']=='5b69c4240998ba2b42de9531'])


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
