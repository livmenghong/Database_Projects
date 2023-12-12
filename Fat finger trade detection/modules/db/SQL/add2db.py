"""

Author: Meng Hong Liv
Student ID: 22232385
Project : SQLite Loader to add trade aggregation to SQLite
Description   : Class to create new table and insert data in SQLite


"""

import sqlite3
from modules.db.NoSQL.add2db import GetMongo_Add2DB

class SQLiteLoaderforAdd2DB(GetMongo_Add2DB):
    def __init__(self, mongo_config, business_date, sql_config):
        super().__init__(mongo_config, business_date)
        self.sql_config = sql_config
        self._conn = sqlite3.connect(sql_config['SQLDBPath'])
        self._cursor = self._conn.cursor()
        self.data_load = self.aggregate_to_load()

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
                 pos_id TEXT PRIMARY KEY, 
                 cob_date TEXT NOT NULL,
                 trader TEXT NOT NULL,
                 symbol TEXT NOT NULL,
                 ccy TEXT NOT NULL,
                 net_quantity INTEGER NOT NULL,
                 net_amount INTEGER NOT NULL)""".format(table_name=self.sql_config['TabletoAdd'])
        
        query_output = self.execute(query)
        
        return query_output
    
    def upsert_position_set(self):

        self._check_sql_table()         # check if there's a table named after '{TabletoAdd}'(can be found in conf.yaml)

        for docs in self.data_load:

            # Insert or Replace into used in order to avoid duplication when inserting data in the table
            
            query = """INSERT OR REPLACE INTO {sql_table}(pos_id, cob_date, trader, symbol, ccy, net_quantity, net_amount) 
                   VALUES("{pos_id}","{cob_date}","{trader}","{symbol}","{ccy}",{quantity},{net_amount})
                   ON CONFLICT(pos_id) DO UPDATE SET 
                   net_amount={net_amount}, 
                   net_quantity={quantity};""".format(
                    sql_table=self.sql_config['TabletoAdd'],
                    pos_id=docs['pos_id'],
                    cob_date=docs['cob_date'],
                    trader=docs['trader'],
                    symbol=docs['symbol'],
                    ccy=docs['ccy'],
                    quantity=docs['quantity'],
                    net_amount=docs['net_amount']
                   )
            print(query)
            self.execute(query)
        
