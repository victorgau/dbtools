import pandas as pd
from pymongo import MongoClient
import json


# 參考：http://stackoverflow.com/questions/16249736/how-to-import-data-from-mongodb-to-pandas
def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]


def mongo2df(db, collection, symbol='TSLA', host='localhost', port=27017, username=None, password=None):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find({'symbol':symbol})

    data = list(cursor)

    if not data:
    	return pd.DataFrame()

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(data)
                                  
    df['date'] = pd.to_datetime(df['date'], unit='ms')
    
    df = df.set_index('date')

    # Delete the _id
    del df['_id']

    return df.sort_index()


def df2mongo(db, collection, df, symbol='TSLA', host='localhost', port=27017, username=None, password=None, replace=False):
    """ Read from DataFrame and Write to Mongo """

    if df.empty:
    	return 0
    
    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    if replace:
    	cursor = db[collection].delete_many({'symbol':symbol})
    	df['symbol'] = symbol
    	df['date'] = df.index
    	records = df.to_dict('records')
    	db[collection].insert_many(records)
    	return len(df.index)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find({'symbol':symbol})
                                  
    data = list(cursor)
                                  
    if data:
    	df1 =  pd.DataFrame(data)
    	df1['date'] = pd.to_datetime(df1['date'], unit='ms')

    	df['symbol'] = symbol
    	df['date'] = df.index

    	df = df[~df.index.isin(df1['date'])]

    	records = df.to_dict('records')
    	db[collection].insert_many(records)

    return len(df.index)

