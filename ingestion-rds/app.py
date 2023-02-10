import json
import pandas as pd
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from model import Coins


def check_if_valid_data(df: pd.DataFrame) -> bool:

    # Check if dataframe is empty
    if df.empty:
        print("\nDataframe empty. Finishing execution")
        return False

    # Check for nulls
    if df.symbol.empty:
        raise Exception("\nSymbol is Null or the value is empty")

    # Check for nulls
    if df.price.empty:
        raise Exception("\nPrice is Null or the value is empty")

    # Check for nulls
    if df.date_added.empty:
        raise Exception("\nDate_added is Null or the value is empty")

    return True


def load_data(table_name, coins_df, session_db, engine_db):

    # validate
    if check_if_valid_data(coins_df):
        print("\nData valid, proceed to load stage")

    # load data on database
    try:    
        coins_df.to_sql(table_name, engine_db, index=False, if_exists='append')
        print("\nData loaded on database")

    except Exception as err:
        print(f"\nFail to load data on database: {err}")


    session_db.commit()
    session_db.close()
    print("\nClose database sucessfully")

    return session_db


def get_data(session_db, engine_db, start, limit, convert, key, url):

    # set limit of data from api
    parameters = {
        'start': start,
        'limit': limit,
        'convert': convert
    }

    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': key,
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

        print("\n")
        for coin in data['data']:
            name.append(coin['name'])
            symbol.append(coin['symbol'])
            date_added.append(coin['date_added'])
            last_updated.append(coin['last_updated'])
            circulating_supply.append(coin['circulating_supply'])
            total_supply.append(coin['total_supply'])
            max_supply.append(coin['max_supply'])
            price.append(coin['quote']['USD']['price'])
            volume_24h.append(coin['quote']['USD']['volume_24h'])
            percent_change_1h.append(coin['quote']['USD']['percent_change_1h'])
            percent_change_24h.append(coin['quote']['USD']['percent_change_24h'])
            percent_change_7d.append(coin['quote']['USD']['percent_change_7d'])

        # Prepare a dictionary in order to turn it into a pandas dataframe below
        coin_dict = {
            "name": name,
            "symbol": symbol,
            "date_added": date_added,
            "last_updated": last_updated,
            "circulating_supply": circulating_supply,
            "total_supply": total_supply,
            "max_supply": max_supply,
            "price": price,
            "volume_24h": volume_24h,
            "percent_change_1h": percent_change_1h,
            "percent_change_24h": percent_change_24h,
            "percent_change_7d": percent_change_7d
        }
    except Exception as e:
        print(f'Error to get data from API:  {e}')
        exit(1)

    # Create dataframe to structure data
    coins_df = pd.DataFrame(coin_dict, columns=["name", "symbol", "date_added", "last_updated", "circulating_supply", "total_supply", "max_supply", "price", "volume_24h", "percent_change_1h", "percent_change_24h", "percent_change_7d"])
    print("Data on pandas dataframe:\n")
    print(coins_df.head(15))

    # call the function to load data on database
    load_data('tb_coins', coins_df, session_db, engine_db)


# Make the coin table
get_session_db, get_engine_db = Coins.start()

# call the get_data function and load data on database
get_data(session_db=get_session_db,
         engine_db=get_engine_db,
         start=1, 
         limit=5000, 
         convert='USD', 
         key='4cdda4de-d018-4298-a662-4668d8b658ba', 
         url='https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest')