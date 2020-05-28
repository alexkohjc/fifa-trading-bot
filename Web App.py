#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
# desktop
sys.path.insert(0, 'C:/Users/Alex Koh/Dropbox/Python/FIFA/fut-master')
#laptop
#sys.path.insert(0, 'C:/Users/Alex/Dropbox/Python/FIFA/fut-master')

import fut
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime, timedelta
import random
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import lxml.html as lh
from IPython.display import display
import sqlite3
import statistics
from apscheduler.schedulers.background import BackgroundScheduler

#don't truncate
pd.set_option('display.max_colwidth', -1, 'display.max_columns', 999, 'display.max_rows', 999)
pd.set_option('float_format', '{:,.2f}'.format)
pd.options.mode.chained_assignment = None  # default='warn'

## database of players
players_db = fut.core.players()

## connect to sqlite3
# desktop
conn = sqlite3.connect('C:/Users/Alex Koh/Dropbox/Python/FIFA/player_db.db')
#conn = sqlite3.connect('C:/Users/Alex/Dropbox/Python/FIFA/player_db.db')
cursor = conn.cursor()

## create scheduler
scheduler = BackgroundScheduler()

## proxies
proxy_list = ['http://35.182.54.53:3128',
                'http://36.68.32.102:3128',
                'http://181.129.50.162:36796',
                'http://51.77.229.110:8080',
                'http://51.77.229.171:8080',
                'http://122.102.28.187:49828',
                'http://201.16.212.79:80',
                'http://177.53.57.154:46416',
                'http://90.182.160.242:57637',
                'http://80.48.119.28:8080',
                'http://35.182.193.6:3128',
                'http://176.196.254.206:41986',
                'http://206.189.163.84:80',
                'http://157.230.149.189:80',
                'http://157.230.212.164:8080',
                'http://206.189.234.211:80',
                'http://157.230.216.214:80',
                'http://157.230.140.12:8080']


# In[2]:


def sell_buy_players(f_sell=1, f_buy=1):
    
    global core
    
    time_start = datetime.now()
    print("Start Time: " + str(time_start))
    
    while True:
        try:  
            core = fut.Core('theolskyhart@gmail.com', 'P@ssw0rd', 'raichu', platform='ps4')
            print('Logged in successfully!')
            print('Current coins: ' + str(core.keepalive()))
            break
        except:
            print('Failed to login, trying again...')
            pass
    
    if f_sell == 1:
        time_start_sell = datetime.now()
        sell_df = sell_players()
        time_sell = datetime.now()
        time_taken_sell = time_sell - time_start_sell
        print("Time taken to sell: " + str(time_taken_sell))
    
    if f_buy == 1:
        time_start_buy = datetime.now()
        buy_df = buy_players(max_buy=6000000)
        time_taken_buy = datetime.now() - time_start_buy
        print("Time taken to buy: " + str(time_taken_buy))
    
    time_end = datetime.now()
    time_delta = time_end - time_start
    print("End Time: " + str(time_end))
    print("Time taken to sell & buy: " + str(time_delta))
    
    print('Current coins: ' + str(core.keepalive()))
    print('Logging out...')
    core.logout()
    
    return None

def get_current_prices(df): 
        # convert to dataframe if its a single resource_id
    print('Getting current prices...')
    
    if type(df) == int:
        df = pd.DataFrame({'resourceId': [df]})
    
    current_price_df = pd.DataFrame(columns=['resourceId', 'Name', 'Current', 'Time'])
    
    df['resourceId'] = df['resourceId'].astype(int) # change to 'int' so that baseId can take in
    
    resource_ids = df['resourceId'].unique() # unique to reduce # requests
    
        # iterate through an Array?
    for resource_id in resource_ids:           
        name = fut.core.get_player_name(resource_id)
        
            # current price data
        url = 'https://www.futbin.com/19/playerPrices?player={0}'
        response = requests.get(url.format(resource_id), proxies={'http': random.choice(proxy_list)}, timeout=5)
        current_price_data = response.json()
            # sleep
        time.sleep(random.randint(1,3))
        
        current_price1 = int(current_price_data[str(resource_id)]['prices']['ps']['LCPrice'].replace(',', ''))
        current_price2 = int(current_price_data[str(resource_id)]['prices']['ps']['LCPrice2'].replace(',', ''))
        current_price3 = int(current_price_data[str(resource_id)]['prices']['ps']['LCPrice3'].replace(',', ''))
        current_price4 = int(current_price_data[str(resource_id)]['prices']['ps']['LCPrice4'].replace(',', ''))
        current_price5 = int(current_price_data[str(resource_id)]['prices']['ps']['LCPrice5'].replace(',', ''))
        current_price = statistics.median([current_price1, 
                                           current_price2, 
                                           current_price3, 
                                           current_price4, 
                                           current_price5])
        #prp_min = int(current_price_data[str(resource_id)]['prices']['ps']['MinPrice'].replace(',', ''))
            # up to LCPrice5; there is also prp_min
        current_price_time = current_price_data[str(resource_id)]['prices']['ps']['updated']
            
            # append to df
        current_price_df = current_price_df.append({'resourceId': resource_id, 
                                                  'Name': name, 
                                                  'Current': current_price, 
                                                 'Time': current_price_time}, ignore_index=True)

    return current_price_df

def get_tradepile_players():
    print('Getting tradepile players...')
    
    tradepile = core.tradepile()

        #sleep
    time.sleep(random.randint(1,3))

    tradepile_df = pd.DataFrame(columns=['itemId', 'resourceId', 'Name', 'Bought'])

    for card in tradepile:
        if card.get('tradeState') != 'active'        and card.get('itemType') == 'player'        and card.get('rareflag')!= 0        and card.get('tradeState') != 'closed':

            #prp_min = card.get('marketDataMinPrice') # this is used to set starting bid
            
            tradepile_df = tradepile_df.append({'itemId': card.get('id'), 
                                                  'resourceId': card.get('resourceId'), 
                                                    'Name': fut.core.get_player_name(card.get('resourceId')),
                                                  'Bought': card.get('lastSalePrice')
                                                },ignore_index=True)
    
    tradepile_df['Bought'] = tradepile_df['Bought'].astype(int)

    return tradepile_df

def get_price_profit_steps(price):
    if 150 <= price <= 1000:
        price_step = 50
        profit_threshold = 80 # arbitiary
        
    elif 1100 <= price <= 10000:
        price_step = 100
        profit_threshold = 50 # arbitiary
        
    elif 10250 <= price <= 50000:
        price_step = 250
        profit_threshold = 20 # arbitiary
        
    elif 50500 <= price <= 100000:
        price_step = 500
        profit_threshold = 50 # arbitiary
        
    else: 
        price_step = 1000
        profit_threshold = 50 # arbitiary

    return price_step, profit_threshold


def sell_players():
       
    tradepile_df = get_tradepile_players()

    current_price_df = get_current_prices(tradepile_df)
    refreshed_prices_df = tradepile_df.merge(current_price_df.drop(['Name'], axis=1), on='resourceId', how='left') 
        
        # to be added for different rule to determine sell; sell when X std. dev. above mean
    #get_hist_prices_db() 
    #get_hist_price_stats()

    tradepile_df = sell_tradepile_players(refreshed_prices_df)
    display(tradepile_df)
    
    return tradepile_df

def sell_tradepile_players(df):
    print('Selling tradepile players...')
    
    tradepile_df = pd.DataFrame(columns=['itemId', 'resourceId', 'Name', 'Bought', 'Current', 'Profit', 'Profit %', 'Sell?'])
    
    print('--------------------------------------------')
    for index, row in df.iterrows():
        buy_price = row['Bought']
        current_price = row['Current']
        profit = current_price - buy_price
        profit_percentage = profit / buy_price * 100
        
        price_step, profit_threshold = get_price_profit_steps(current_price)
        
        if profit_percentage >= profit_threshold:
            to_sell = 1
            # ACTUAL SELLING
            sell_start_bid = current_price - price_step # max(current_price - price_step, prp_min)
            core.sell(item_id = row['itemId'], bid = sell_start_bid, buy_now = current_price)
            print('Listed: ' + row['Name'] + " - Bid: " + str(sell_start_bid) + " BIN: " + str(current_price))
            time.sleep(random.randint(3,5))
        else:
            to_sell = 0
            pass
        
        tradepile_df = tradepile_df.append({'itemId': row['itemId'], 
                                            'resourceId': row['resourceId'], 
                                            'Name': row['Name'],
                                            'Bought': buy_price,
                                            'Current': current_price,
                                            'Profit': profit,
                                            'Profit %': int(profit_percentage), # change to int for aesthetic reasons
                                            'Sell?': to_sell}, ignore_index=True)
        
        tradepile_df.sort_values(['Sell?', 'Profit %'], ascending=False, inplace=True)
        
    print('--------------------------------------------')
    
    return tradepile_df

def buy_players(max_buy=3000):
    print('Performing buy operation...')
   
    hist_price_stats = get_price_stats()
    shortlisted_players_hist = get_shortlisted_players(hist_price_stats,
                                                     #median = -0.20, 
                                                      z_score = -1.00)
    display(shortlisted_players_hist)
    
    current_price_df = get_current_prices(shortlisted_players_hist) 
    
    shortlisted_current = shortlisted_players_hist.merge(current_price_df.drop(['Name'], axis=1), on='resourceId', how='left') 
    
    current_price_stats = get_current_price_stats(shortlisted_current)
    
    shortlisted_players_current = get_shortlisted_players(current_price_stats,
                                                         #median = -0.20, 
                                                          z_score = -1.00)
    display(shortlisted_players_current)
    
    bought_list = buy_shortlisted_players(shortlisted_players_current)
    
    bought_list_max_buy = bought_list[bought_list['Current'] <= max_buy]
    
    return bought_list


def get_price_stats(f_ytd_avg=0, f_ytd_range=1, f_avg_30d=0, f_range_30d=0,
                    f_avg_14d=1, f_range_14d=1, f_avg_7d=0,f_range_7d=0):
    
    print('Reading hist prices from DB...')
    
    hist_price_db = pd.read_sql(sql='select * from player_prices', con=conn)
    
    print('Calculating hist price stats...')
    
    last_update_date = hist_price_db.groupby('resourceId').last().reset_index()[['resourceId', 'Date']]
    hist_price_db = hist_price_db.pivot(index='resourceId', columns='Date', values='Price')
    hist_price_db.replace(0, np.NaN, inplace=True)
    
    hist_price_db_final = pd.DataFrame()
    
        # create a mix and match list?
    hist_price_db_final['YTD Max'] = hist_price_db.max(axis=1, skipna=True)
    hist_price_db_final['YTD Min'] = hist_price_db.min(axis=1, skipna=True)
    hist_price_db_final['YTD Mean'] = hist_price_db.mean(axis=1, skipna=True)
    hist_price_db_final['YTD Median'] = hist_price_db.median(axis=1, skipna=True)

    hist_price_db_final['14d Max'] = hist_price_db.iloc[:, -14:].max(axis=1, skipna=True)
    hist_price_db_final['14d Min'] = hist_price_db.iloc[:, -14:].min(axis=1, skipna=True)
    hist_price_db_final['14d Mean'] = hist_price_db.iloc[:, -14:].mean(axis=1, skipna=True)
    hist_price_db_final['14d Median'] = hist_price_db.iloc[:, -14:].median(axis=1, skipna=True)

    hist_price_db_final['YTD S.D.'] = hist_price_db.std(axis=1, skipna=True)
    hist_price_db_final['14d S.D.'] = hist_price_db.iloc[:, -14:].std(axis=1, skipna=True)

    hist_price_db_final['Yest'] = hist_price_db[hist_price_db.columns[-1]]
    hist_price_db_final['Last Updated'] = hist_price_db[hist_price_db.columns[-1]]
    
    hist_price_db_final.reset_index(inplace=True)
    
    hist_price_db_final['Name'] = hist_price_db_final['resourceId'].apply(lambda x: fut.core.get_player_name(x))

    hist_price_db_final.set_index(['resourceId', 'Name'], inplace=True)
        # change NAs to 0s
    hist_price_db_final.fillna(0, inplace=True)
    hist_price_db_final = hist_price_db_final.round().astype(int)
    
        # calc C.V. here to avoid conversion to int
    hist_price_db_final['YTD C.V.'] = hist_price_db_final['YTD S.D.'] / hist_price_db_final['YTD Mean']
    hist_price_db_final['14d C.V.'] = hist_price_db_final['14d S.D.'] / hist_price_db_final['14d Mean']
    
    hist_price_db_final['% of 14d Median'] = (hist_price_db_final['Yest'] - hist_price_db_final['14d Mean']) /                                                hist_price_db_final['14d Mean']
    hist_price_db_final['14d Z-Score'] = (hist_price_db_final['Yest'] - hist_price_db_final['14d Mean']) /                                            hist_price_db_final['14d S.D.']
    
    #hist_price_db_final['% from 14d Median'] = (hist_price_db_final['% from 14d Median'] * 100).astype(int)
    
    hist_price_db_final.reset_index(inplace=True)
    
        # rearrange columns
    hist_price_db_final = hist_price_db_final[['resourceId', 'Name',
                                               'YTD Max', 'YTD Min', 'YTD Mean', 'YTD Median',  
                                               'YTD S.D.', 'YTD C.V.',
                                               '14d Max', '14d Min', '14d Mean', '14d Median',  
                                               '14d S.D.', '14d C.V.', 
                                              'Yest', '% of 14d Median', '14d Z-Score']]
    hist_price_db_final = hist_price_db_final.merge(last_update_date, on='resourceId', how='left') 
    hist_price_db_final.rename(columns={'Date': 'Last Updated'}, inplace=True)
    
    if f_ytd_avg == 0:
        hist_price_db_final.drop(['YTD Mean', 'YTD Median'], axis=1, inplace=True)
    if f_ytd_range == 0:
        hist_price_db_final.drop(['YTD Max','YTD Min'], axis=1, inplace=True)
    
    hist_price_db_final.sort_values('14d Z-Score', inplace=True)
    
    print('Writing price stats to DB...')
    hist_price_db_final.to_sql(name='player_price_stats', index=False, if_exists='replace', con=conn)
    
    # TODO: fix YTD Min = 0 for players; maybe because added late? or not available on market?
    # TODO: write output to DB?
    
    return hist_price_db_final

def get_shortlisted_players(price_stats, median = -0.20, z_score = -1.00):
    print('Shortlisting players based on Z-Score less than ' + str(z_score))
    
    shortlisted_players = price_stats[price_stats['14d Z-Score'] <= z_score]
        # currently only considers 14d Z-Score
        
    return shortlisted_players

def get_current_price_stats(current_price_df, median = -0.20, z_score = -1.00):
    
    resource_id_list = list(current_price_df['resourceId'])
    
    query = 'select * from player_prices where resourceId in ({})'

    hist_price_db = pd.read_sql(query.format(','.join(list('?' * len(resource_id_list)))),                                            con=conn, params=resource_id_list)
    
    hist_price_db = hist_price_db.append({'resourceId': current_price_df['resourceId'],
                                         'Date': datetime.utcnow().strftime('%Y-%m-%d'),
                                         'Price': current_price_df['Current']}, ignore_index=True)
    
    
        ## might wanna change here to append before pivot; can be simpler
    hist_price_db = hist_price_db.pivot(index='resourceId', columns='Date', values='Price')
    
    hist_price_db.reset_index(inplace=True) 
    yest_price_df = hist_price_db.iloc[:, [0] + [-1]] # get first and last column ['resourceId', 'Yest']
    yest_price_df.rename(columns={yest_price_df.columns[-1]: "Yest"}, inplace=True)
    
    current_price_df = current_price_df[['resourceId', 'Current']] # drop all other columns
    
    hist_price_db = hist_price_db.merge(current_price_df, on='resourceId', how='left') # will get daily price data + current
    
    hist_price_db = hist_price_db.melt(id_vars = 'resourceId', var_name = 'Date' , value_name='Price')                        .sort_values(['resourceId', 'Date'])
        
    current_price_stats = get_price_stats(hist_price_db) # returns a df w/ price stats but 'Yest' column is actually
                                                         # 'Current' 
    current_price_stats.rename(columns={"Yest": "Current"}, inplace=True)  
    current_price_stats = current_price_stats.merge(yest_price_df, on='resourceId', how='left') # left join to get yest price
    
    current_price_stats.set_index(['resourceId', 'Name'], inplace=True)
    current_price_stats.fillna(0, inplace=True)
    current_price_stats['Yest'] = current_price_stats['Yest'].astype(int)
    current_price_stats.reset_index(inplace=True)
    
        # rearrange columns
    current_price_stats = current_price_stats[['resourceId', 'Name',
                                               'YTD Max', 'YTD Min', 
                                               #'YTD Mean', 'YTD Median',  
                                               'YTD S.D.', 'YTD C.V.',
                                               '14d Max', '14d Min', 
                                               '14d Mean', '14d Median',  
                                               '14d S.D.', '14d C.V.', 
                                               'Yest', 'Current', 
                                               '% of 14d Median', '14d Z-Score']]
    
    current_price_stats = current_price_stats[current_price_stats['Current'] != 0]
    
    current_price_stats.sort_values('14d Z-Score', inplace=True)
    
    return current_price_stats

def buy_shortlisted_players(df, num_bins=3):
    print('Buying shortlisted players...')
    
    players_counts = get_tradepile_players_counts()

    df = df.merge(players_counts.drop(['Name', 'Avg. Buy'], axis=1), on='resourceId', how='left')
    df.fillna(0, inplace=True)
    df['Count'] = df['Count'].astype(int)

    for index, row in df.iterrows():

        resource_id = row['resourceId']
        current_price = row['Current']
        name = row['Name']
        count = row['Count']
        num_to_buy = num_bins - count

        # while count < num_bins: # change to this so that can buy until num_bins; add timer to break, etc.
        if count < num_bins:
            print("To buy " + str(num_to_buy) + " " + str(name) + " at " + str(current_price))
                 
                # search transfer market, with max_buy set at current price
            search_results = search_transfer_market(resource_id, max_buy = current_price).head(num_bins - count) 
        
            if search_results.shape[0] == 0:
                print("No trades for " + name + " at " + str(current_price))
            else:
                display(search_results)

                #sleep
            time.sleep(random.randint(1,3))

            for index, row in search_results.iterrows():
                try:
                        ### !!! ACTUAL BUYING !!! ###
                    core.bid(row['tradeId'], row['buyNowPrice']) 
                    print("Bought for " + str(row['tradeId']) + " at " + str(row['buyNowPrice']))

                        #sleep
                    time.sleep(random.randint(1,3)) 

                        #send to tradepile
                    core.sendToTradepile(row['itemId'])

                        #sleep
                    time.sleep(random.randint(1,3)) 

                except NoTradeExistingError: #except Exception: will not catch keyboard interupt, etc.
                    print(str(row['tradeId']) + " was not executed; likely was already bought by others")
                    pass         
        else: 
            print("Already have " + str(count) + " " + name)
            
        print("---------------------------")

    print("End")
    print(str(core.keepalive()) + " coins left" )
    print("TODO: add a loop to continue searching and buying until we have X no. of players on hand")
    
    return None

def get_tradepile_players_counts():
    print('Getting tradepile player counts...')
    
    tradepile_df = get_tradepile_players()
    
    tradepile_df = tradepile_df.groupby(['resourceId', 'Name'], as_index=False).agg({'Bought': 'mean', 'itemId':'count'})
    
    tradepile_df.rename(index=str, columns={'Bought': 'Avg. Buy', 'itemId': 'Count'}, inplace=True)
    
    return tradepile_df

def search_transfer_market(resource_id, max_buy=None): # can use resource_id to search, but it's called assetId 
    print('Searching transfer market for players...')

    search_results = pd.DataFrame(columns=['resourceId', 'tradeId', 'Name', 'itemId', 
                                           'rating', 'position', 'club', 'nationality',
                                            'currentBid','buyNowPrice','expires'
                                           #'ID', 'tradeState', 'bidState', 'startingBid'
                                          ])

    searchs = core.searchAuctions(ctype='player', 
                                     assetId=resource_id, 
                                     max_buy=max_buy, #change this to 80% of median price instead of just current price
                                                            # but might to use price_steps or sth 
                                     #start=12, #in orders of 12, 24, etc. 12 or 20?
                                    )

    for result in searchs:

        #trade_state = result['tradeState']
        #bid_state = result['bidState']
        #starting_bid = result['startingBid']

        search_results = search_results.append({'resourceId': resource_id,
                                                'tradeId': result['tradeId'],
                                              'Name': fut.core.get_player_name(resource_id),
                                              'itemId': result['id'],
                                                'rating': result['rating'],
                                                'position': result['position'],
                                                'club': result['teamid'],
                                                'nationality': result['nation'],
                                                'buyNowPrice': result['buyNowPrice'],
                                              'currentBid': result['currentBid'],
                                              'expires': result['expires']
        }, ignore_index=True)

    search_results.sort_values('buyNowPrice', inplace=True)
    
    #print("TODO: might wanna change the buy_now price to 80% of median instead of current")
    
    return search_results

def get_sold_players():
    print('Getting players sold...')
    tradepile = core.tradepile()

    sold_players_df = pd.DataFrame(columns=['itemId', 'resourceId', 'Name', 'Sold'])

    for card in tradepile:
        if card.get('tradeState') in ['closed'] and card.get('itemType')=='player': #and card.get('expires')=sth?

            #last_sale_price = card.get('lastSalePrice')
            #trade_id = card.get('tradeId')
            #timestamp = card.get('timestamp')
            #bin_price = card.get('buyNowPrice') 

            sold_players_df = sold_players_df.append({'itemId': card.get('id'), 
                                              'resourceId': card.get('resourceId'),
                                                'Name': fut.core.get_player_name(resource_id),
                                              #'Bought': last_sale_price,
                                                'Sold' : card.get('currentBid'), # should be actual selling price 
                                                                                  # once tradestate is 'closed'
                                                #'BIN' : bin_price, 
                                                #'Expires': expires
                                               }, ignore_index=True)
    
    print("TODO: bought price is lost once item is sold. gotta get it from somewhere else, maybe link thru item_id") 
    print(core.keepalive())
    
    return sold_players_df


# In[3]:


sell_buy_players(f_sell = 1, f_buy = 0)


# In[32]:


# https://apscheduler.readthedocs.io/en/latest/userguide.html

#sell_players_job = scheduler.add_job(sell_players, 'interval', minutes=65, id='my_job_id')#, next_run_time=datetime.now())
sell_buy_job = scheduler.add_job(sell_buy_players, 'interval', minutes=65, id='my_job_id', next_run_time=datetime.now())

#scheduler.configure(jobstores=MemoryJobStores, executors=ThreadPoolExecutor, job_defaults=job_defaults)

scheduler.start()


# In[25]:


scheduler.shutdown(wait=False)


# In[31]:


#scheduler.get_jobs()
scheduler.print_jobs()

