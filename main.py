import uvicorn
from fastapi import FastAPI,Request,WebSocket,WebSocketDisconnect,status,HTTPException,Depends,Query,Form
from fastapi.websockets import WebSocketState
from collections import deque
from time import sleep
import time
from threading import Thread
from fastapi.security import HTTPBearer
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from datetime import datetime, timedelta
from schema import FormData,  TradingModeLimitConfig , Broker , TableData , TargetQtyUpdateRequest ,  BrokerConfig
from nse import load_new_contract
from admin import  create_jwt_token, get_current_user, get_websocket_user,  templates 
from fyers_runner import ThreadManager
import asyncio
import logging
from models import SQLiteConnectionPool , create_database , create_credentials_table , fetch_all_users , fetch_user_by_userId , insert_user , delete_user ,\
    insert_or_update_credentials , fetch_credentials_by_userId , fetch_all_broker_credentials
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/admin", response_class=HTMLResponse)
async def admin_home(request: Request):
    return templates.TemplateResponse("admin_login.html", {"request": request})

@app.post("/admin-login", response_class=HTMLResponse )
async def admin_login(request: Request, username: str = Form(...), password: str = Form(...)):

    correct_username = "admin"
    correct_password = "password"

    if username == correct_username and password == correct_password:
        token_data = {"sub": username}
        jwt_token = create_jwt_token(data=token_data)

        response = RedirectResponse(url="/admin-panel", status_code=303)
        response.set_cookie(key="Authorization", value=f"Bearer {jwt_token}", httponly=True)
        return response
    else:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error_message": "Invalid username or password. Please try again."
        })

@app.get("/admin-panel", response_class=HTMLResponse)
async def admin_panel(request: Request, username: str = Depends(get_current_user)):
    if username == "admin":
        employees =  fetch_all_users(pool)

        return templates.TemplateResponse( "admin_panel.html", { "request": request, "employees": employees }, )
    else:
        return templates.TemplateResponse("login.html", { "request": request, "error_message": "Invalid username or password. Please try again." })

@app.get("/admin-logout", response_class=HTMLResponse)
async def logout(request: Request):
    response = RedirectResponse(url="/admin", status_code=303)
    response.delete_cookie("Authorization")
    return response

@app.post("/save-userid-password")
async def save_table_data(request:Request , table_data: TableData , username: str = Depends(get_current_user) ):
    if username=="admin":
        table_rows = table_data.data

        for idx, row in enumerate(table_rows, start=1):
            # print(row.userID)
            if row.name=="" or row.phone=="" or  row.userID=="" or row.password=="" or row.date=="":
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupport Perameter")

        data = fetch_all_users(pool)
        previous_user_list = [ user['userId'] for  user in data]
        # print(previous_user_list)

        _all_status = []
        for idx, row in enumerate(table_rows, start=1):
            _status, _msg = insert_user(pool , row.name , row.phone , row.userID ,row.password , row.date)
            if _status:
                if row.userID not  in users_queue:
                    users_queue[row.userID] = asyncio.Queue()
                if row.userID not in _thread_manager:
                    _thread_manager[row.userID] = ThreadManager( row.userID , users_queue[row.userID] , load_new_contract_obj , pool )
            _all_status.append(_status)
        current_user = [ row.userID for index , row in enumerate(table_data.data)]
        # print("current User" , current_user )
        for user_details in previous_user_list:
            if user_details  not in current_user:
                # print(f"deleting {user_details}")
                delete_user(pool , user_details)           
                if user_details   in users_queue:
                    del users_queue[user_details]
                if user_details in _thread_manager:
                    _thread_manager[user_details].squreoff("NIFTY")
                    _thread_manager[user_details].squreoff("BANKNIFTY")
                    del _thread_manager[ user_details ]
        
        if all(_all_status):
            return {"status": "success", "message": "CRUD Completed" }
    else:
        return templates.TemplateResponse("login.html", { "request": request, "error_message": "Invalid username or password. Please try again." })
        
###########################################################################################################

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, userId: str = Form(...), password: str = Form(...)):

    data = fetch_user_by_userId(pool , userId)

    if (data is not None) and (userId == data['userId']) and password == data['password']:

        token_data = {"sub": userId}
        jwt_token = create_jwt_token(data=token_data)
        response = RedirectResponse(url="/home", status_code=303)
        response.set_cookie(
            key="Authorization", value=f"Bearer {jwt_token}", httponly=False
        )
        return response
    else:
        return templates.TemplateResponse( "login.html", { "request": request, "error_message": "Invalid userId or password. Please try again." } )

@app.get("/logout", response_class=HTMLResponse)
async def logout(request: Request):
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie("Authorization")
    return response


@app.post("/api/save-broker-config")
async def save_broker_config(config: BrokerConfig ,  userId: str = Depends(get_current_user) ):
    print("config")
    print(config)
    _status , msg = insert_or_update_credentials(pool, config.broker, userId, **{k: v for k, v in config.dict().items() if k != "broker"})
    if _status:
        if config.broker == "fyers":
            return {"message": "Fyers Config Saved", "msg":msg }
        elif config.broker == "angleone":
            return {"message": "AngleOne Config Saved","msg":msg  }
        elif config.broker == "shoonya":
            return {"message": "Shoonya Config Saved", "msg":msg }
    else:
        raise HTTPException(status_code=400, detail="Invalid broker type")


@app.post("/api/save-tradingmode-limit-config")
async def save_tradingmode_limit( config: TradingModeLimitConfig, user: str = Depends(get_current_user)):
    _status, msg = _thread_manager[user].update_tradingmode_lots( config.tradingmode, int(config.limit) )
    if _status:
        return {"diplay": "log", "heading": "Tradingmode & Lots", "msg": msg}
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=msg,
        )

@app.post("/api/update-broker")
async def update_broker( broker: Broker, user: str = Depends(get_current_user)):
    _status, msg = _thread_manager[user].update_broker( broker.broker )
    if _status:
        return {"diplay": "log", "heading": "Broker", "msg": msg}
    else:
        raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail = msg )

@app.get("/get-server-status")
async def get_server_status(userId: str = Depends(get_current_user)):
    if _thread_manager[userId].server_status():
        return {"message": f"Connection fine beetwen algo-server and {_thread_manager[userId].broker}-server "}
    else:
        raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"Not connected to {_thread_manager[userId].broker}-server" )

@app.get("/stop-broker")
async def stop_broker(userId: str = Depends(get_current_user)):
    if _thread_manager[userId].stop_broker():
        return {"message": f"Broker Stoped Successfully"}
    else:
        raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"Somthing Wrong . Please Try again" )

@app.get("/connect-to-broker")
async def connect_to_broker(user: str = Depends(get_current_user)):
    if _thread_manager[user].check_order_running():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Possible Current Order under Monitoring , Please Close First")
    else:
        print("no order running")
    
    print("closing websocket")
    try:
        _thread_manager[user].shoonya_close_websocket()
    except Exception as e:
        print("closing shoonya:" , e)
    try:
        _thread_manager[user].fyers_stop_web_socket()
    except Exception as e:
        print("closing fyers:" , e)
    try:
        _thread_manager[user].sws.close_connection()
    except Exception as e:
        print("closing fyers:" , e)

    broker = _thread_manager[user].broker
    print("broker" , broker)
    if broker == "shoonya":
        try:
            print("closing websocket")
            _thread_manager[user].shoonya_close_websocket()
            print("connect to shoonya")
            _status = _thread_manager[user].connect_shoonya()
            if _status:
                print("squreoff shoonya")
                _thread_manager[user].squreoff("NIFTY")
                _thread_manager[user].squreoff("BANKNIFTY")
                print("connect websocket")
                _thread_manager[user].shoonya_connect_to_websocket()
                print(_thread_manager[user].master_df)
                return RedirectResponse(url="/home", status_code=302)
            else:
                raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"Facing Problem in connecting to Broker")        
        except Exception as e:
            print(e)
            raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"Facing Problem in connecting to Broker")
        
    elif broker == "fyers":
        try:
            _status, redirect_main_url = _thread_manager[user].get_fyers_login_url(user)
            if _status:
                print("Redirecting to:", redirect_main_url)
                return RedirectResponse(url=redirect_main_url, status_code=303)  # Use 303 to ensure GET request
            else:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Facing Problem in connecting to Broker")
        except Exception as e:
            print(e)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Facing Problem in connecting to Broker")

    elif broker == "angleone":
        try:
            print("closing websocket")
            # _thread_manager[user].shoonya_close_websocket()
            print("connect to angleone")
            _status = _thread_manager[user].connect_angleone()
            if _status:
                print("squreoff angleone")
                _thread_manager[user].squreoff("NIFTY")
                _thread_manager[user].squreoff("BANKNIFTY")
                print("connect websocket")
                _thread_manager[user].angle_run_web_socket()
                # sleep(5)
                # print(_thread_manager[user].master_df)
                return RedirectResponse(url="/home", status_code=302)
            else:
                raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"Facing Problem in connecting to Broker")        
        except Exception as e:
            print(e)
            raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"Facing Problem in connecting to Broker")
        


    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Facing Problem in connecting to Broker")

@app.get("/request_token")
async def get_request_token(request: Request, user: str = Depends(get_current_user)):
    print("")
    try:
        full_url = str(request.url)
        _status = _thread_manager[user].fyers_load_access_token(full_url, user)
        if _status:
            if hasattr(ThreadManager, 'ws_fyers'):
                _thread_manager[user].fyers_stop_web_socket()
            else:
                print("'ws_fyers' is not define for close")
            sleep(0.5)
            _thread_manager[user].squreoff("NIFTY")
            _thread_manager[user].squreoff("BANKNIFTY")
            _status = _thread_manager[user].fyers_run_web_socket()

        if datetime.now().date() > _thread_manager[user].today:
            _thread_manager[user].booked_nifty_pl = deque([("DEMO", 0), ("LIVE", 0)])
            _thread_manager[user].booked_banknifty_pl = deque([("DEMO", 0), ("LIVE", 0)])
        return RedirectResponse(url="/home", status_code=302)
    except Exception as e:
        print("Error:", e)
        return RedirectResponse(url="/home", status_code=302)


@app.get("/home", response_class=HTMLResponse)
async def read_item(request: Request, user: str = Depends(get_current_user)):
    return templates.TemplateResponse(
        "working_page.html",
        {
            "request": request,
            "userId": user,
            "data": _thread_manager[user].get_details() ,
            "broker_config": fetch_all_broker_credentials(pool , user ),
            "tradingmode_limit": _thread_manager[user].trading_mode, "trade_limit": _thread_manager[user].real_max_trade , "booked_pl": _thread_manager[user].get_booked_pl()
        },
    )


@app.post("/submit-values")
async def submit_form(data: FormData, userId: str = Depends(get_current_user)):
    # print("Received data:", data)
    return_status, msg = _thread_manager[userId].update_details(
        data.index,
        data.lots,
        data.expiry,
        data.takeProfit,
        data.maxDrawdown,
        data.primum_trail_sl_activation_point,
        data.CEstrikeDistance,
        data.PEstrikeDistance,
        data.premiumSL,
        data.ceBuy,
        data.peBuy,
        data.timeframe )
    if return_status:
        return {
            "message": msg,
            "data": _thread_manager[userId].get_details(),
            # "fyers_config": fetch_credentials_by_userId(pool , userId ,  ) , 
            "tradingmode_limit": _thread_manager[userId].trading_mode, "trade_limit": _thread_manager[userId].real_max_trade,
            }
    else:
        raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=msg )


@app.post("/start")
async def start(data: dict, user: str = Depends(get_current_user)):
    status_of_thread, msg = _thread_manager[user].run_my_thread(data["index"], data["option"])
    if status_of_thread:
        return {"message": msg}
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)


@app.post("/increment-sl")
async def increment_sl( data: dict , user: str = Depends(get_current_user)):
    status_of_thread, msg = _thread_manager[user].increment_sl(data["option"] , data['index'])
    if status_of_thread:
        return {"message": msg}
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)


@app.post("/TargetQtyUpdate")
async def target_qty_update(request: TargetQtyUpdateRequest, user: str = Depends(get_current_user)):
    try:
        data = request.dict()
        status_code, msg = _thread_manager[user].target_qty_update(data['index'], data['option'])
        if status_code:
            return {"message": msg}
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@app.get("/SqureOff")
async def squre_off(index: str, user: str = Depends(get_current_user)):
    # print(index)
    status_of_thread, msg , form_msg = _thread_manager[user].squreoff(index)
    if status_of_thread:
        return {"message": msg , "form_msg":form_msg}
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)

@app.post("/SquareOff")
async def square_off(request: Request):
    data = await request.json()
    index = data.get("index")
    value = "SQUARE_OFF"
    result, message = target_qty_update(index, value)
    return {"success": result, "message": message}


@app.get("/getlogs")
async def get_logs(user: str = Depends(get_current_user)):
    logs = _thread_manager[user].read_logs_as_dict()
    if "error" in logs:
        raise HTTPException(status_code=404, detail=logs["error"])
    return {"user": user, "logs": logs}

@app.get("/getorderbook")
async def get_orderbook(user: str = Depends(get_current_user)):
    
    if _thread_manager[user].trading_mode == "DEMO":
        return { "orderbook": _thread_manager[user].demo_orders }
    elif _thread_manager[user].trading_mode == "LIVE":
        return { "orderbook": _thread_manager[user].real_orders }




class ConnectionManager:
    def __init__(self):
        self.active_connections = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    async def disconnect(self, websocket: WebSocket):
        user_id_to_remove = None
        for user_id, ws in self.active_connections.items():
            if ws == websocket:
                user_id_to_remove = user_id
                break

        if user_id_to_remove:
            if websocket.application_state == WebSocketState.CONNECTED:
                await websocket.close()
            del self.active_connections[user_id_to_remove]

    async def send_message(self, websocket: WebSocket, message):
        try:
            if websocket.application_state == WebSocketState.CONNECTED:
                await websocket.send_json(message)
            else:
                print(f"WebSocket is closed. Cannot send message: {message}")
                await self.disconnect(websocket)
        except (WebSocketDisconnect, RuntimeError) as e:
            print(f"Error sending message: {e}")
            await self.disconnect(websocket)

    def is_user_connected(self, user_id: str) -> bool:
        websocket = self.active_connections.get(user_id)
        return websocket and websocket.application_state == WebSocketState.CONNECTED


websocket_manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user: str = Depends(get_websocket_user)):
    if user in users_queue:
        await websocket_manager.connect(websocket, user)
        print("------------------------------------------")
        print(websocket_manager.is_user_connected(user))
    else:
        await websocket.close()
        return
    try:
        while True:
            z = await users_queue[user].get()
            # print("data")
            data = {"message": z }
            # print(data)
            # await websocket_manager.send_message(websocket, json.dumps(data))
            await websocket.send_json(data)
    except WebSocketDisconnect:
        logging.info(f"Client {user} disconnected")
        await websocket_manager.disconnect(websocket)
    except Exception as e:
        logging.error(f"Error in WebSocket connection: {e}")
        await websocket_manager.disconnect(websocket)



if __name__ == "__main__":
    pool = SQLiteConnectionPool('users.db', pool_size=5)
    create_database()
    create_credentials_table()
    users_queue = {}
    _thread_manager = {}
    data = fetch_all_users(pool)
    print(data)
    load_new_contract_obj = load_new_contract()

    for user_details in data:
        users_queue[user_details['userId']] = asyncio.Queue()
        _thread_manager[user_details['userId']] = ThreadManager( websocket_manager , user_details['userId'] , users_queue[user_details['userId']] , load_new_contract_obj , pool)
        # _thread_manager[user_details['userId']].load_access_token( "" , user_details['userId']  , True)
        # _thread_manager[user_details['userId']].run_web_socket()
    # print(_thread_manager)

    uvicorn.run(app, host="0.0.0.0", port=5000)