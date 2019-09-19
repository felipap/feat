
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Set, Dict

import pandas as pd
import numpy as np

from .common.Graph import Graph
from .common.Table import create_table_from_config
from .common.Output import Output
from .lib.gen_cartesian import gen_cartesian
from .assembler import assemble_features
from .parser import parseLineToCommand, Tree
from .lib.state import save_state
from .lib.cmonth import date_to_cmonth, cmonth_to_date, date_yearmonth, yearmonth_date
from .graph_config import GraphConfig


def parse_features(features: List[str]):
  # TODO parse all before starting to assemble one-by-one.
  
  if len(set(features)) < len(features):
    print("Duplicate features found:", len(set(features)), len(features))
    for feature in features:
      if features.count(feature) > 1:
        print("–", feature)
    raise Exception()
  
  commands = []
  for feature in features: # REVIEW no need to validate tree?
    commands.append(Tree(parseLineToCommand(feature)))
  return commands


def generate_date_range(date_range):
  """Inclusive range."""
  start = datetime.strptime(date_range[0], '%Y-%m')
  end = datetime.strptime(date_range[1], '%Y-%m')

  result = []
  curr = start
  while curr <= end:
    result.append(curr)
    curr += relativedelta(months=1)
  return result


def assemble(features, config, table_configs, dataframes):
  """
  Use the input dataframes and configurations to create the tables and
  initialize the data graph.
  """

  graph = Graph()
  for table_name, table_config in table_configs.items():
    # Check that the dataframe for this table has been supplied.
    if not table_name in dataframes:
      raise Exception(f'Dataframe for table {table_name} was not supplied.')

    table = create_table_from_config(table_name, table_config, dataframes.pop(table_name))
    graph.add_table(table)

  date_range = generate_date_range(config['date_range'])
  output = Output(graph.tables, config['pointers'], date_range)
  graph.add_output(output)
  graph.wrap()

  command_trees = parse_features(features)
  assembled = assemble_features(graph, output, command_trees, config['date_block'])
  
  # Alert of NaN values being returned.
  for column in assembled.columns:
    if assembled[column].isna().any():
      # print("Column %s has NaN items " % column, assembled[column].unique())
      pass
    
    # FIXME document this. Well wtf is this
    if assembled[column].dtype == np.dtype('O'):
      assembled[column] = assembled[column].astype(str)

  # Translate cmonth values to datetimes.
  mapping = { c: date_yearmonth(cmonth_to_date(c)) for c in range(570, 650) }
  # print("map is", mapping)
  assembled['CMONTH(date)'] = assembled['__date__'].replace(mapping)

  return assembled
