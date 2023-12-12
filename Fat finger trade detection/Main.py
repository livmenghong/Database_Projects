
"""

Author: Meng Hong Liv
Student ID: 22232385
Project : Incorrect trade detection and SQL trades uploader
Description: SQLite uploader for incorrect trades and aggregated trades from a given business date

"""


import datetime


from modules.utils.info_logger import print_info_log
from modules.utils.args_parser import arg_parse_cmd
from modules.utils.config_parser import Config
from modules.db.SQL.CreateTable import SQLiteLoaderforSusTrade
from modules.db.SQL.add2db import SQLiteLoaderforAdd2DB

if __name__ == '__main__':
    print_info_log('Script started', 'progress')
    
    args = arg_parse_cmd()
    parsed_args = args.parse_args()
        
    conf = Config()

    print_info_log('Command line argument parsed & main config loaded', 'progress')



    # First objective: Detect suspicious trade

    mongo_loader_forSusTrade = SQLiteLoaderforSusTrade(mongo_config=conf['dev']['config']['Database']['Mongo'],
                                business_date=parsed_args.date_run,
                                sql_config=conf['dev']['config']['Database']['SQLite'])
    mongo_loader_forSusTrade.upsert_position_set()

    print_info_log('Trades aggregation and upsert completed', 'progress')

    mongo_loader_forSusTrade.close()
    
    print_info_log('Changes committed to db and connection closed', 'progress')



    # Second objective: Aggregate and insert into table
    
    mongo_loader_Add2DB = SQLiteLoaderforAdd2DB(mongo_config=conf['dev']['config']['Database']['Mongo'],
                                business_date=parsed_args.date_run,
                                sql_config=conf['dev']['config']['Database']['SQLite'])
    mongo_loader_Add2DB.upsert_position_set()

    print_info_log('Trades aggregation and upsert completed', 'progress')

    mongo_loader_Add2DB.close()

    print_info_log('Changes committed to db and connection closed', 'progress')
