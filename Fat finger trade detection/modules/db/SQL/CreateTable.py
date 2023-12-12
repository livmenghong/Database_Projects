"""

Author: Meng Hong Liv
Student ID: 22232385
Project : SQLite Loader to add suspicious trades to SQLite
Description   : Class to create new table and insert data in SQLite


"""

import sqlite3
from modules.db.NoSQL.CreateTable import GetMongo_forSusTrade

class SQLiteLoaderforSusTrade(GetMongo_forSusTrade):
    def __init__(self, mongo_config, business_date, sql_config):
        super().__init__(mongo_config, business_date)
        self.sql_config = sql_config
        self._conn = sqlite3.connect(sql_config['SQLDBPath'])
        self._cursor = self._conn.cursor()
        self.data_load = self.retrieve_to_load()

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @property
    def connection(self):
        return self._conn
 
    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

   
    def _check_sql_table(self):
        
        query = """CREATE TABLE IF NOT EXISTS {table_name} (
                 trade_id TEXT PRIMARY KEY, 
                 trade_date TEXT NOT NULL,
                 trader TEXT NOT NULL,
                 symbol TEXT NOT NULL,
                 ccy TEXT NOT NULL,
                 trade_type TEXT NOT NULL,
                 ctr_pty TEXT NOT NULL,
                 quantity INTEGER NOT NULL,
	         notional_amount INTEGER NOT NULL)""".format(table_name=self.sql_config['Table'])
        
        query_output = self.execute(query)
        
        return query_output



    def get_table(self, date, sym):         # function created to retreive a table of high and low prices from equity_prices table 

        query= """Select * From '{table_name}'
                    Where cob_date = '{trade_date}'
                    And symbol_id = '{symbol}' """ .format(
                    table_name= self.sql_config['TabletoGet'],      # '{TabletoGet}' is the name of the table we want to retrieve high and low prices from
                    trade_date= date,
                    symbol= sym
                    )

        query_output = self.execute(query)

        row = self.cursor.fetchall()
        return row 

    

    
    def upsert_position_set(self):

        self._check_sql_table()             # check if there's a table named after '{Table}'(can be found in conf.yaml)


        for docs in self.data_load:         # loop to check if price of each trade stays in between the low and high prices

            equity_prices = self.get_table(docs['trade_date'], docs['symbol'])      # use of the get_table function above to retrieve the high and low prices

            check_price= docs['notional_amount']/docs['quantity']       # definition for the price of a share of equity in the trade

            high = equity_prices[0][2]
            low = equity_prices[0][3]

            if not (low <= check_price <= high):        # 'if not' condition used so that only the trades that has the irregular price will be captured and inserted

                # 'Insert or replace into' is used to avoid duplication during inserting data to the table

                query = """INSERT OR REPLACE INTO {sql_table}(trade_id, trade_date, trader, symbol, ccy, trade_type,
                        ctr_pty, quantity, notional_amount) 
                       VALUES("{trade_id}","{trade_date}","{trader}","{symbol}","{ccy}","{trade_type}","{ctr_pty}",
                       "{quantity}","{notional_amount}")
                       """.format(
                        sql_table= self.sql_config['Table'],
                        trade_id= docs['trade_id'],
                        trade_date= docs['trade_date'],
                        trader= docs['trader'],
                        symbol= docs['symbol'],
                        ccy= docs['ccy'],
                        trade_type= docs['trade_type'],
                        ctr_pty= docs['ctr_pty'],
                        quantity= docs['quantity'],
                        notional_amount= docs['notional_amount']
                       )
                print(query)
                self.execute(query)










        
