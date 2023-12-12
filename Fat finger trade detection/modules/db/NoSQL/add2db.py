"""

Author: Meng Hong Liv
Student ID: 22232385
Project : MongoDB Trades aggregator for insertion to SQLite
Description   : Class to retrieve and aggregate data from MongoDB


"""



from pymongo import MongoClient
import datetime

class GetMongo_Add2DB:

    """
    Arguments:
        mongo_config (list): configuration for MongoDB. in ./config/conf.yaml
        business_date (date): desired date for trade aggregation

    Public methods:
        aggregate_mongo_data: retrieve all trades for the given business date and perform
                              aggregation by date,trader, symbol and ccy.

    mongo_client = GetMongo(conf['dev']['config']['Database']['Mongo'],
                            datetime.datetime.strptime('2017-07-21', '%Y-%m-%d'))

    """

    def __init__(self, mongo_config, business_date):
        self.mongo_config = mongo_config
        self.business_date = business_date

    
    def _init_mongo_client(self):
        """
        
        creates MongoDB client and points to a specific collection.
        
        """
        mng_client = MongoClient(self.mongo_config['url'])
        mng_db = mng_client[self.mongo_config['Db']]        
        mng_collection = mng_db[self.mongo_config['Collection']]
        return mng_collection

    def _create_mongo_pipeline(self):
        """
        aggregation pipeline:
            - filter only today's trades;
            - aggregate by date,trader, symbol and ccy;
            - summarise net_quantity and net_amount as sum of quantity and notional
        """
        
        start_date = self.business_date
        end_date = self.business_date + datetime.timedelta(hours=24)
        
        pipeline = [
            {'$match': 
                {
                '$and': [{'DateTime': {'$gte': "ISODate("+start_date.strftime('%Y-%m-%d')+"T"+'%H:%M:%S'+".000Z"")"}},
				        {'DateTime': {'$lte': "ISODate("+end_date.strftime('%Y-%m-%d')+"T"+'%H:%M:%S'+".000Z"")"}}]
                }
            },
            {'$group': {
                '_id': 
                    {
                    'cob_date': "$DateTime",   
                    'trader': "$Trader",
                    'symbol': "$Symbol",
                    'ccy': "$Ccy"
                    }, 
				'net_amount': {'$sum': "$Notional"},
				'quantity':  {'$sum': "$Quantity"}
                } 
            }
        ]        
        return pipeline

    def _aggregate_mongo_data(self): 
        
        pipe_line = self._create_mongo_pipeline()

        client_collection = self._init_mongo_client()
                                                                    
        cursor = client_collection.aggregate(pipeline=pipe_line)     # this will return a MongoDB cursor in result
                                                                    
        results = list(cursor)                                          # convert cursor to list
        return results

    def aggregate_to_load(self):
        
        bus_date = datetime.datetime.strftime(self.business_date, '%d-%b-%Y')
        data_load = self._aggregate_mongo_data()
        
        list_output = []
        
        for dict in data_load:
            date_id = dict['_id']['trader'] + datetime.datetime.strftime(self.business_date, '%Y-%m-%d').replace('-', '') + dict['_id']['symbol']
            list_output.append({                
                'pos_id': date_id,
                'cob_date': bus_date,
                'trader': dict['_id']['trader'], 
                'symbol': dict['_id']['symbol'], 
                'ccy': dict['_id']['ccy'],
                'quantity': dict['quantity'],
                'net_amount': dict['net_amount']
                })     
        return list_output
   
