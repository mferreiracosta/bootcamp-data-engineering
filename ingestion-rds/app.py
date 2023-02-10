from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'start':'1',
  'limit':'10',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': '4cdda4de-d018-4298-a662-4668d8b658ba',
}

session = Session()
session.headers.update(headers)

name = []
symbol = []
date_added = []
last_updated = []
circulating_supply = []
total_supply = []
max_supply = []
price = []
volume_24h = []
percent_change_1h = []
percent_change_24h = []
percent_change_7d = []

try:
  response = session.get(url, params=parameters)
  data = json.loads(response.text)
  for coin in data['data']:
    print(coin['name'])
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)