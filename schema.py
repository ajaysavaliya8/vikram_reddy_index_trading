from pydantic import BaseModel
from datetime import date
from enum import Enum
from typing import Literal
from typing import List, Dict , Optional, Union

class FormData(BaseModel):
    index:str
    lots: int
    expiry: date
    takeProfit: float
    maxDrawdown: float
    primum_trail_sl_activation_point: float
    CEstrikeDistance: float
    PEstrikeDistance: float
    premiumSL: float
    ceBuy: bool
    peBuy: bool
    timeframe: str

class TargetQtyUpdateRequest(BaseModel):
    option: str
    index: str


class BaseBrokerConfig(BaseModel):
    broker: str

class FyersConfig(BaseBrokerConfig):
    apiKey: str
    secretKey: str
    redirectUrl: str
    grantType: str
    responseType: str

class AngleOneConfig(BaseBrokerConfig):
    user_id: str
    api_key: str
    secretKey: str
    totp: str
    pin: str

class ShoonyaConfig(BaseBrokerConfig):
    user_id: str
    password: str
    api_key: str
    twofa: str
    vendor_code: str
    imei: str

BrokerConfig = Union[FyersConfig, AngleOneConfig, ShoonyaConfig]

class TradingModeLimitConfig(BaseModel):
    tradingmode : str
    limit : str
    
class Broker(BaseModel):
    broker : str    


class TableRow(BaseModel):
    name: str
    phone: str
    userID: str
    password: str
    date: str

class TableData(BaseModel):
    data: List[TableRow]
