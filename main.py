from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic
import base64
from minio.sse import SseCustomerKey
from pydantic import BaseModel
from index import IndexManager


app = FastAPI()
security = HTTPBasic()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_credentials=True,
    allow_headers=["*"]
)

gdal = IndexManager()


class indexingRequest(BaseModel):
    collection_id: int
    collection_name: str
    jwt_token: str
    encryption_key: str


@app.post("/indexing_collection")
async def login(request: indexingRequest):
    encryption_key = base64.urlsafe_b64decode(request.encryption_key.encode())
    encryption_key = SseCustomerKey(encryption_key)
    await gdal.get_info(request.collection_id, request.collection_name,
                        jwt_token=request.jwt_token, encryption_key=encryption_key)
