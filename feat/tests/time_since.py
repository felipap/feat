
import sys
sys.path.append('/Users/felipe/dev') # Make sure to pip3 install -e ~/dev/assembler

import assembler
import pickle
from os import path

# from assembler.assembler.lib.state import load_state
# state = load_state()
# dataframes = state['dataframes']
# pickle.dump(dataframes, open(df_path, 'wb'))

df_path = path.join(path.dirname(__file__), 'dataframes.pickle')
dataframes = pickle.load(open(df_path, 'rb'))

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
    'date_range': ['2017-11', '2019-5'],
    'features': [
      "customer.status",
      "TIME_SINCE(CMONTH(Users{customer=id}.created_at))",
    ],
    "output": {
      'date_block': 'CMONTH(date)',
      'pivots': ['CMONTH(date)', 'customer'],
      'pointers': {
        'customer': 'Users.id',
        'product': 'Products.id',
      },
    },
}

assembler.assemble(shape, type_config, dataframes)

