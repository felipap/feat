import pickle
from os import path

def save_state(**kwargs):
  basepath = path.dirname(__file__)
  filepath = path.abspath(path.join(basepath, "state.pickle"))
  print("SAVING STATE", kwargs.keys(), "to", filepath)
  pickle.dump(kwargs, open(filepath, "wb"))


def load_state(filepath=None):
  basepath = path.dirname(__file__)
  if not filepath:
    filepath = path.abspath(path.join(basepath, "state.pickle"))
  print("LOADING STATE from", filepath)
  return pickle.load(open(filepath, "rb"))