from pydantic import BaseModel
from typing import Optional
import datetime


# Class created to serve as base model for trade insertion 
class Model(BaseModel):

     DateTime: Optional[datetime.datetime] = None
     TradeId: Optional[str] = None
     Trader: str
     Symbol: str
     Price: float
     Quantity: float
     Notional: float
     TradeType: str
     Ccy: str
     Counterparty: str




