"""

Author: Meng Hong Liv
Student ID: 22232385
Project : MongoDB trades retriever for suspicious trade detection model 
Description   : Retrieve all trades on a given business date and store in a data dictionary


"""


from pymongo import MongoClient
import datetime

class GetMongo_forSusTrade:

    """
    Arguments:
        mongo_config (list): configuration for MongoDB. in ./properties/conf.yaml
        business_date (date): business date date for trade aggregation

    Public methods:
        _retrieve_mongo_data: gets all trades for given business date.

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

    def _create_mongo_pipeline_retrieve(self):

        """
        Pipeline to retrieve all trade within the given business day.

        """
        
        start_date = self.business_date
        end_date = self.business_date + datetime.timedelta(hours=24)

        pipeline_retrieve ={'$and': [{'DateTime': {'$gte': "ISODate("+start_date.strftime('%Y-%m-%d')+"T"+'%H:%M:%S'+".000Z"")"}},
                                    {'DateTime': {'$lte': "ISODate("+end_date.strftime('%Y-%m-%d')+"T"+'%H:%M:%S'+".000Z"")"}}]}
        return pipeline_retrieve



    def _retrieve_mongo_data(self): 
        
        pipe_line = self._create_mongo_pipeline_retrieve()

        client_collection = self._init_mongo_client()
                                                        
        cursor = client_collection.find(pipe_line)          # this will return a MongoDB cursor in result
                                                             
        results = list(cursor)                              # convert cursor to list
        return results
    

    def retrieve_to_load(self):
        
        bus_date = datetime.datetime.strftime(self.business_date, '%d-%b-%Y')
        data_load = self._retrieve_mongo_data()
        
        list_output = []
        
        for dict in data_load:
            
            list_output.append({                
                'trade_id': dict['TradeId'],
                'trade_date': bus_date,
                'trader': dict['Trader'], 
                'symbol': dict['Symbol'], 
                'ccy': dict['Ccy'],
                'trade_type': dict['TradeType'],
                'ctr_pty': dict['Counterparty'],
                'quantity': dict['Quantity'],
                'notional_amount': dict['Notional'],
                'price': dict['Notional']/dict['Quantity']})     
        return list_output

  
   
   
