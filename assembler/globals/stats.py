# """
# Functions related to statistics over time.
# """

# from .lib.pergroup import make_pergroup

# import numpy as np
# import pandas as pd

# pd.set_option('display.max_columns', 30)

# def accumulate_foreach(keys, rows):
#   result = {}
#   count = 0
#   for date in sorted(rows.keys()):
#     row = rows[date]
#     if row and row['_value_']:
#       count += row['_value_'] 
#     result[date] = count
#   return result

# accumulate = make_pergroup(accumulate_foreach)