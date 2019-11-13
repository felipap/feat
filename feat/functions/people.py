
import numpy as np
import pandas as pd
import gender_guesser.detector as gender
d = gender.Detector()

from .lib.per_value import per_value

def call_gender(value, _):
  return d.get_gender(value)

functions = {
  'PEOPLE_GENDER': per_value(call_gender, fillna=''),
}
