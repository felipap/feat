
from timeit import default_timer as timer
from functools import wraps
import multiprocessing

import pandas as pd
import numpy as np

def make_percol(innerfn, fillna=0, dtype=None):
  """Creates an assembler function from an Python function that takes a
  single Pandas column (plus any other arguments) as the input."""
  
  @wraps(innerfn)
  def magic(ctx, name, args):
    child = args[0]

    df = child.get_stripped()
    df.rename(columns={ child.name: '_value_' }, inplace=True)

    print("THIS MIGHT BREAK SOMETHING")
    df = df[pd.notnull(df['_value_'])]
    
    df['_result_'] = innerfn(df['_value_'], args)
    df[name] = innerfn(df['_value_'], args)

    if dtype:
      print("using dtype %s" % repr(dtype))
      df[name] = df[name].astype(dtype)

    result = ctx.create_subframe(name, child.pivots)
    print("FIXME trying to fillna = ", fillna)
    result.fill_data(df, fillnan=fillna, dtype=dtype)
    return result
  return magic