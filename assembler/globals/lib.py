
from timeit import default_timer as timer
from datetime import datetime, timedelta
from dateutil import relativedelta
import time
import numpy as np
import pandas as pd
import functools
import pyjson5
from pandas.io.json import json_normalize

def fancy_apply(df, function, **kwargs):
  total = df.shape[0]
  
  count = 0
  @functools.wraps(function)
  def wrapped(row):
    nonlocal count
    count += 1
    if count % 2000 == 0:
      print("\r<%s> done: %d%%" % (function.__name__, 100*count/total), end="")
    return function(row)

  start = timer()
  try:
    result = df.apply(wrapped, **kwargs)
  except Exception as e:
    import traceback; traceback.print_exc()
    print("\nError using .apply(). row=%s" % count, e)
    raise Exception(e)
  
  print("\nFunction took: %s seconds" % round(timer() - start, 2))
  return result
