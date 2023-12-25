import requests
import pandas as pd
import json
from sqlalchemy import create_engine
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects



url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
  'start':'1',
  'limit':'5000',
  'convert':'USD'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': '3cff8582-5f90-4058-8d26-a39b3c66a24b',
}



try:
  response = requests.get(url, params=parameters, headers=headers)
  data = json.loads(response.text)
  df=pd.json_normalize(data['data'])
   
  
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)

##
for column in df.columns:
    if '.' in column:
        column_splited=column.split('.')
        lower_column_splited=[column.lower() for column in column_splited]
        df.rename(columns={column:'_'.join(lower_column_splited)},inplace=True)


df_clean=df[['id', 'name', 'symbol', 'slug', 'num_market_pairs', 'date_added',
      'tags', 'max_supply', 'circulating_supply', 'total_supply',
      'infinite_supply', 'platform', 'cmc_rank',
      'self_reported_circulating_supply', 'self_reported_market_cap',
      'tvl_ratio', 'last_updated', 'quote_usd_price', 'quote_usd_volume_24h',
      'quote_usd_volume_change_24h', 'quote_usd_percent_change_1h',
      'quote_usd_percent_change_24h', 'quote_usd_percent_change_7d',
      'quote_usd_percent_change_30d', 'quote_usd_percent_change_60d',
      'quote_usd_percent_change_90d', 'quote_usd_market_cap',
      'quote_usd_market_cap_dominance', 'quote_usd_fully_diluted_market_cap',
      'quote_usd_tvl', 'quote_usd_last_updated']]

##Inserting coin word in the register section os response
for column in df_clean.columns[0:17]:
    df_clean.rename(columns={column:f'coin_{column}'},inplace=True)

## Transforming dates as string to datetime
df_clean['coin_date_added']=pd.to_datetime(df_clean['coin_date_added'])
df_clean['coin_last_updated']=pd.to_datetime(df_clean['coin_last_updated'])
df_clean['quote_usd_last_updated']=pd.to_datetime(df_clean['quote_usd_last_updated'])


## Split register from full df
df_cripto_register=df_clean[['coin_id', 'coin_name', 'coin_symbol', 'coin_slug',
      'coin_num_market_pairs', 'coin_date_added', 'coin_tags',
      'coin_max_supply', 'coin_circulating_supply', 'coin_total_supply',
      'coin_infinite_supply', 'coin_platform', 'coin_cmc_rank',
      'coin_self_reported_circulating_supply',
      'coin_self_reported_market_cap', 'coin_tvl_ratio', 'coin_last_updated']]

## Split cripto quotes from full df
df_cripto_quote=df_clean[['coin_id','quote_usd_price', 'quote_usd_volume_24h',
      'quote_usd_volume_change_24h', 'quote_usd_percent_change_1h',
      'quote_usd_percent_change_24h', 'quote_usd_percent_change_7d',
      'quote_usd_percent_change_30d', 'quote_usd_percent_change_60d',
      'quote_usd_percent_change_90d', 'quote_usd_market_cap',
      'quote_usd_market_cap_dominance', 'quote_usd_fully_diluted_market_cap',
      'quote_usd_tvl', 'quote_usd_last_updated']]

engine = create_engine('postgresql://stefano:postgres@localhost:5432/coin')
df_cripto_register.to_sql(name='cryptocurrency_register', con=engine, if_exists='replace')
df_cripto_quote.to_sql(name='cryptocurrency_quotes', con=engine, if_exists='append')

