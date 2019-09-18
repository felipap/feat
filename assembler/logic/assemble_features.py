
import colorful
from timeit import default_timer as timer
import pandas as pd

from ..common.Context import Context
from .assembler import assemble_column

def assemble_features(graph, output, trees, date_block):
  """
  Assemble a list of features based on a graph of tables and pointers between
  them.
  """ 
  
  # The context holds the state of the program as it explores the tree of
  # trees and assembles the data.
  print("date block is", date_block)
  context = Context(graph, output)
  
  for index, tree in enumerate(trees):
    print(colorful.green(f'Feature {index+1}/{len(trees)}:'), tree.get_name())

    start = timer()
    result = assemble_column(context, tree._nested)
    end = timer()

    # Calling assemble_column with context.current set to 'output' should take
    # care of merging the assembled columns with the Output dataframe.
    if not context.output.has_column(result.name):
      raise Exception('Something went wrong')
    
    print("elapsed: %.2fs" % (end - start))
  
  # # Assert no duplicate features (which might break things later)
  # if sorted(list(set(to_return.columns))) != sorted(to_return.columns):
  #   raise Exception("Duplicate features found.")
  
  col_names = list(map(lambda c: c.get_name(), trees))
  col_names += ['__date__']
  col_names += output.get_pointers().keys()
  
  return context.df[col_names]
