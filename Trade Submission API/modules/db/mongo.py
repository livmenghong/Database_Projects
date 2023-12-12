from fastapi.encoders import jsonable_encoder
import pymongo
import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from modules.utils.config_parser import Config

conf= Config()

client = AsyncIOMotorClient(conf['dev']['config']['Database']['Mongo']['url'])
db = client[conf['dev']['config']['Database']['Mongo']['Db']]

# function insert trade is used for inserting trades and check on inserted info for potential errors
async def insert_trade(trade_data):
    
    # check on quantity inserted if it falls within range of 50% under the min and 50% over the max, if not quantity is unusal
    checkQuantity = 1 if -30000 < trade_data.Quantity <= 60000 else 0

    # check on price inserted if it is positive and equal to notional divides by quantity inserted, if not, the price entered is wrong
    checkPrice = 1 if trade_data.Price >0 and trade_data.Notional / trade_data.Quantity == trade_data.Price else 0

    # if both tests above has been satified making sum of checkQuantity and checkPrice equals 2, trade will be submitted
    if checkQuantity + checkPrice == 2 :
        
        trade_time =  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%SZ')
        trade_data.DateTime = trade_time
        trade_data.TradeId = trade_data.TradeType[0] + trade_data.Trader + trade_data.Symbol + datetime.datetime.strptime(trade_time, '%Y-%m-%d %H:%M:%SZ').strftime('%Y%m%d%H%M%S')
        trade = jsonable_encoder(trade_data)

        new_trade = await db[conf['dev']['config']['Database']['Mongo']['Collection']].insert_one(trade)
        created_trade = await db[conf['dev']['config']['Database']['Mongo']['Collection']].find_one({"_id": new_trade.inserted_id}, {'_id': 0})
        return created_trade, {"msg": "Trade has been submitted successfully"}


    # if checkQuantity == 0 however, checkPrice == 1, trade will be submitted, however, a message about unsual quantity will be returned
    elif checkQuantity == 0 and checkQuantity + checkPrice == 1 :
        trade_time =  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%SZ')
        trade_data.DateTime = trade_time
        trade_data.TradeId = trade_data.TradeType[0] + trade_data.Trader + trade_data.Symbol + datetime.datetime.strptime(trade_time, '%Y-%m-%d %H:%M:%SZ').strftime('%Y%m%d%H%M%S')
        trade = jsonable_encoder(trade_data)

        new_trade = await db[conf['dev']['config']['Database']['Mongo']['Collection']].insert_one(trade)
        inserted_trade = await db[conf['dev']['config']['Database']['Mongo']['Collection']].find_one({"_id": new_trade.inserted_id}, {'_id': 0})
        return inserted_trade, {"msg": "Trade has been submitted however quantity is unsually high"}

    # if checkPrice == 0, trade info submitted is wrong, therefore, the trade will not be submitted    
    else:
        if checkPrice == 0:
            return {"msg": "Error: Trade has not been submitted, Price does not equates to calculation from Quantity and Notional amount inserted, Please check the values again!"}

# function delete_trade is used for deleting trades by entering the trade id of the trade
async def delete_trade(trade_id):
    delete_result = await db[conf['dev']['config']['Database']['Mongo']['Collection']].delete_one({"TradeId": trade_id})
    return delete_result


