
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Set, Dict

import pandas as pd
import numpy as np

from .common.Graph import Graph
from .common.Table import create_table_from_config
from .common.Output import Output
from .lib.gen_cartesian import gen_cartesian
from .assembler import assemble_many
from .parser import parse_feature
from .lib.state import save_state
from .lib.tblock import date_to_cmonth, cmonth_to_date, date_yearmonth, yearmonth_date, date_to_cweek, cweek_to_date
from .graph_config import GraphConfig


def parse_features(features: List[str]):
  # TODO parse all before starting to assemble one-by-one.
  
  if len(set(features)) < len(features):
    print("Duplicate features found:", len(set(features)), len(features))
    found = set()
    for feature in features:
      if feature in found:
        continue
      if features.count(feature) > 1:
        print("–", feature)
        found.add(feature)
    raise Exception()
  
  commands = []
  for feature in features: # REVIEW no need to validate tree?
    commands.append(parse_feature(feature))
  return commands


def validate_result(assembled):
  # Alert of NaN values being returned.
  for column in assembled.columns:
    if assembled[column].isna().any():
      print("Column %s has NaN items " % column, assembled[column].unique())

    if assembled[column].isna().any():
      print("Column %s has NaN items " % column, assembled[column].unique())

    # FIXME document this. Well wtf is this
    if assembled[column].dtype == np.dtype('O'):
      assembled[column] = assembled[column].astype(str)


def assemble(features, dataframes, table_configs, output_config, block_type='month'):
  """
  Use the input dataframes and configurations to create the tables and
  initialize the data graph.
  """

  if block_type not in ['month', 'week']:
    raise Exception()

  graph = Graph()
  for table_name, table_config in table_configs.items():
    # Check that the dataframe for this table has been supplied.
    if not table_name in dataframes:
      raise Exception(f'Dataframe for table {table_name} was not supplied.')

    table = create_table_from_config(table_name, table_config, dataframes.pop(table_name), block_type)
    graph.add_table(table)

  output = Output(graph.tables, output_config, block_type)
  graph.add_output(output)
  graph.wrap()

  command_trees = parse_features(features)
  assembled = assemble_many(graph, output, command_trees, block_type)

  validate_result(assembled)
  # Translate cmonth values to datetimes.
  # mapping = { c: date_yearmonth(cmonth_to_date(c)) for c in range(570, 650) }
  mapping = { c: datetime.strftime(cweek_to_date(c), '%Y-%m-%d') for c in range(2495, 2595) }

  # print("map is", mapping)
  assembled['__dcount__'] = assembled['__date__'].replace(mapping)
  assembled['__date__'] = assembled['__date__'].replace(mapping)

  return assembled
