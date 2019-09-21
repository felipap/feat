
import numpy as np
import pandas as pd

from .lib.per_value import per_value

def call_email_domain(value, _):
  if value and pd.notna(value) and '@' in value:
    return value.split('@')[1].lower()
  return None

def call_domain_ext(value, _):
  if value and pd.notnull(value) and '.' in value:
    return '.'.join(value.split('.')[1:])
  return None

functions = {
  'EMAIL_DOMAIN': per_value(call_email_domain, fillna=''),
  'DOMAIN_EXT': per_value(call_domain_ext, fillna=''),
}
