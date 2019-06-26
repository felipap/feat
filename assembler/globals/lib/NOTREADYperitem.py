
from timeit import default_timer as timer
from functools import wraps
import multiprocessing

import pandas as pd
import numpy as np

def make_percol(innerfn, fillna=0):
  
  @wraps(innerfn)
  def magic(ctx, name, args):
    child = args[0]

    df = child.get_stripped()
    df.rename(columns={ child.name: '_value_' }, inplace=True)

    # for index, row in df.iterrows():
    #   df.at[index,'_result_'] = innerfn(row, args)
    records = df.to_dict('records')
    # chunks = list(map(list, np.array_split(records, 4)))

    results = []
    for record in records:
      results.append({ **record, '_result_': innerfn(record, args) })

    final = pd.DataFrame(results) # list(np.hstack(results)))
    
    # df[name] = (df[name]>int(arg)).astype(np.bool)

    final.rename(columns={ '_tcount_': 'CMONTH(date)', '_result_': name }, inplace=True)

    result = ctx.create_subframe(name, child.pivots)
    result.fill_data(final, fillnan=fillna)
    return result
  return magic