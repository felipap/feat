
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
# dataframes = loop.run_until_complete(load_namespace('verb', range(575, 593)))
# print("elapsed: %ds" % (timer() - start))
# pickle.dump(dataframes, open('dataframes.pickle', 'wb'))
dataframes = pickle.load(open('./__cache__/neuron_trash_jobs_first.pickle', 'rb'))

type_config = {
  'customer': {
    'pivots': ['id', 'CMONTH(date)'],
    'column_cast': {
      'created': 'datetime64[ns]',
    },
  },
  'order': {
    'pivots': ['id'],
    'pointers': {
      'customer': 'customer.id',
    },
    'column_cast': {
      'date': 'datetime64[ns]',
    },
  },
  'product': {
    'pivots': ['id'],
  },
  'lineitem': {
    'pivots': ['id'],
    'pointers': {
      'order': 'order.id',
      'product': 'product.id',
    },
  },
}

shape = {
    'date_range': ['2017-11', '2019-5'],
    'features': [
      "customer.subscription_status",
    ],
    "output": {
      'date_block': 'CMONTH(date)',
      'pivots': ['CMONTH(date)', 'customer'],
      'pointers': {'customer': 'customer.id'}
    },
}

result = assembler.assemble(shape, type_config, dataframes)
pickle.dump(result, open('/Users/felipe/delete.pickle', 'wb'))
result = pickle.load(open('/Users/felipe/delete.pickle', 'rb'))

result.fillna(0, inplace=True)

result
