# https://github.com/lark-parser/lark/blob/master/docs/lark_cheatsheet.pdf

from os import path
from functools import wraps
import importlib
from typing import List, Set, Dict
# from lark import Lark, Transformer, v_args, Token, Tree
from lark import Lark, Transformer, v_args, Token, Tree

from .Command import Command

# TODO: consider using https://docs.python.org/3.7/library/importlib.html#module-importlib.resources
LOCAL_DIR = path.dirname(path.realpath(__file__))
GRAMMAR_FILE_PATH = path.join(LOCAL_DIR, 'grammar.lark')

def annotate_match(method):
  """Adds a 'name' field to the dictionary result of the decorated method, corresponding to
  the underlying matched text. Also adds a '_last' field to the dictionary, and sets it to
  the name of the decorated method."""

  warning = "decorator expects method to be annotated with @v_args(meta=True)"

  def objectIsLarkMeta(obj):
    return hasattr(obj, "empty")

  @wraps(method)
  def foo(self, *args):
    assert len(args) == 2, warning
    assert objectIsLarkMeta(args[1]), warning

    [children, meta] = args

    result = method(self, children, meta)
    assert type(result) == dict, \
      "decorator expects method result to always be a dictionary"
    
    if meta.empty:
      return None
    
    name = self.text[meta.start_pos: meta.end_pos]\
      .replace("\n","")\
      .replace(" ","")\
      .replace("\t","")
    result["name"] = name
    result["_last"] = method.__name__
    return result

  return foo


class JSONifier(Transformer):
  def __init__(self, text):
      """text is the original command that was parsed, which we need inside
      column_call()
      """

      self.text = text

  @v_args(meta=True)
  @annotate_match
  def root_column(self, children, _):
    assert 1 <= len(children) <= 3
    # If : this_column
    if len(children) == 1:
      return children[0]
    assert type(children[0]) == Token
    # If : TABLE_NAME "." this_column
    if len(children) == 2:
      return {
        "root": children[0].value,
        "next": children[1],
      }
    # If : TABLE_NAME translation "." this_column
    return {
      "root": children[0].value,
      "translation": children[1],
      "next": children[2],
    }

  @v_args(meta=True)
  @annotate_match
  def translation(self, children, _):
    assert len(children) % 2 == 0

    return {
      "map": list(zip(*(iter(children),) * 2)),
      "map_str": list(zip(*(iter(c['name'] for c in children),) * 2)),
    }

  @v_args(meta=True)
  @annotate_match
  def this_column(self, children, meta):
    if len(children) == 1 and type(children[0]) == Token:
      assert children[0].type == 'LOWERCASE'
      return {
        "this": children[0].value,
        "is_terminal": True,
      }

    if len(children) == 2 and type(children[0]) == Token: # child[1] is from 'this_column'
      assert children[0].type == 'LOWERCASE'

      return {
        "this": children[0].value,
        "next": children[1],
        # "column": '%s.%s' % (children[0].value, children[1].column),
      }

    if len(children) == 1 and type(children[0]) == dict: # child[0] is from 'fn_call'
      return children[0]

    raise Exception()

  @v_args(meta=True)
  @annotate_match
  def fn_call(self, children, meta):
    assert children[0].type == 'FUNCTION_NAME'
    assert type(children[1]['args']) == list

    groupby = None
    if len(children) == 3:
      assert type(children[2]) == Tree and children[2].data == 'groupby'
      groupby = children[2].children

    return {
      "is_terminal": True,
      "function": children[0].value,
      "args": children[1]['args'],
      "groupby": groupby,
    }

  @v_args(meta=True)
  @annotate_match
  def fn_args(self, children, _):
    for child in children:
      assert child.data == 'fn_arg'
    return {
      "args": [child.children[0] for child in children]
    }

  array = list

  number = v_args(inline=True)(float)
  string = v_args(inline=True)(str)


def parse_feature(string):
  grammar = open(GRAMMAR_FILE_PATH).read()

  json_parser = Lark(
    grammar,
    start='root_column',
    propagate_positions=True,
    debug=True,
  )

  raw = json_parser.parse(string)
  # print("raw is", raw, "\n", raw.data, dir(raw.data))
  transformed = JSONifier(string).transform(raw)
  
  return Command(transformed)


def parse_features(features: List[str]):
  """
  Parse a list of strings into their respective Command objects.
  """
  
  # Throw if duplicate features are passed.
  # We must remove the space from the features so equivalent features look
  # identical.
  stripped_features = list(map(lambda x: x.replace(' ', ''), features))
  if len(set(stripped_features)) < len(stripped_features):
    print("Duplicate features found:", len(set(stripped_features)), len(stripped_features))
    found = set()
    for feature in stripped_features:
      if feature in found:
        continue
      if stripped_features.count(feature) > 1:
        print("–", feature)
        found.add(feature)
    raise Exception()
  
  commands = []
  for index, feature in enumerate(features): # REVIEW no need to validate tree?
    if not feature.strip():
      print('Cannot parse empty feature at index', index)

    try:
      commands.append(parse_feature(feature))
    except Exception as e:
      print("Failed to parse feature", feature)
      raise e
  return commands