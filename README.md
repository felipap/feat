
# Feat

Takes a set of connected tables and a list of feature descriptions and
automatically assembles a dataframe containing those features.

Feature descriptions are strings like:

```
TIME_SINCE_SEEN(Line_item.SUM(quantity|DATE(order.date),order.customer))
```



```
virtualenv venv -p python3.7
```

### Code

- `feat/globals` List of manipulation functions.


### FIX

TIME_SINCE(Customer{customer=id}.created_at) works, but TIME_SINCE(customer.created_at) fails.


### Good Python

https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/framework