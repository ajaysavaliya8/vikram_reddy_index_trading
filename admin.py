from fastapi import Depends, HTTPException, Request, status, APIRouter, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse

from datetime import datetime, timedelta
import jwt
from jwt import PyJWTError
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
 

templates = Jinja2Templates(directory="templates")

SECRET_KEY = "your_secret_key"


def create_jwt_token(data: dict, expires_delta: timedelta = timedelta(hours=23)):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, SECRET_KEY, algorithm="HS256")
    return token


async def get_current_user(request: Request):
    token = request.cookies.get("Authorization")
    if not token:
        raise HTTPException(status_code=403, detail="Not authenticated")

    try:
        payload = jwt.decode(token.split(" ")[1], SECRET_KEY, algorithms=["HS256"])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=403, detail="Not authenticated")
        return username
    except PyJWTError:
        raise HTTPException(status_code=403, detail="Invalid token")


def get_websocket_user(token: str = Query(...)):
    try:
        # Strip 'Bearer ' prefix if it exists
        if token.startswith("Bearer "):
            token = token[7:]  # Remove the 'Bearer ' part

        # Decode the JWT token
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        username = payload.get("sub")

        if username is None:
            raise HTTPException(status_code=403, detail="Invalid token")
        return username
    except jwt.PyJWTError:
        print("Invalid token.")
        raise HTTPException(status_code=403, detail="Invalid token")
