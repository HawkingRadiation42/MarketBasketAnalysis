# MarketBasketAnalysis

#### Download the input data from here: 'http://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx'


#### Code to send request using python

```
import requests
url = 'function_url'
file_path = 'mba.xlsx'
with open(file_path, 'rb') as f:
    file_data = f.read()
    r = requests.post(url, data=file_data)
print(r.text)
```
