from fastapi import APIRouter
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from typing import Optional

import modules.db.mongo as mongodb
from modules.models.trades import Model

router = APIRouter()


@router.post("/trade/", response_description="Create a trade")
async def create_trade(trade: Model):
    trade_submit = await mongodb.insert_trade(trade)
    if len(trade_submit) == 1:
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content=trade_submit)
    else:
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=trade_submit)


@router.delete("/trade/{id}", response_description="Delete a trade")
async def delete_trade(id: str):
    delete_result = await mongodb.delete_trade(id)
    if delete_result.deleted_count == 1:
        return JSONResponse(status_code=status.HTTP_202_ACCEPTED)
    raise HTTPException(status_code=404, detail=f"TradeId: {id} not found")
