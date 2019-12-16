
from timeit import default_timer as timer
from functools import wraps
import multiprocessing

import pandas as pd
import numpy as np


def make_per_col(innerfn, fillna=0, dtype=None, num_args=1):
  """Creates an assembler function from an Python function that takes a
  single Pandas column (plus any other arguments) as the input."""
  
  @wraps(innerfn)
  def magic(ctx, name, args):
    child = args[0]

    df = child.get_stripped()
    # df.rename(columns={ child.name: '_value_' }, inplace=True)
    df['_value_'] = df[child.name]

    print("THIS MIGHT BREAK SOMETHING")
    df = df[pd.notnull(df['_value_'])]
    
    df[name] = innerfn(df['_value_'], *args[1:])

    if dtype:
      print("using dtype %s" % repr(dtype))
      df[name] = df[name].astype(dtype)

    result = ctx.table.create_subframe(name, child.pivots)
    print("FIXME trying to fillna = ", fillna)
    result.fill_data(df, fillnan=fillna, dtype=dtype)
    return result

  return {
    'call': magic,
    'num_args': num_args,
    'takes_pivots': False,
  }