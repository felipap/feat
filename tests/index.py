
import sys
sys.path.append('/Users/felipe/dev') # Make sure to pip3 install -e ~/dev/assembler

import assembler
from assembler.assembler.lib.state import load_state, save_state

state = load_state()
dataframes = state['dataframes']
type_config = state['type_config']

shape = {
    'date_range': ['2017-12', '2019-5'],
    'features': [
      """PARSE_FLEX_PLANS(customer.flex_plans)""",
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

