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

index = IndexManager()


class indexingRequest(BaseModel):
    collection_id: int
    collection_name: str
    jwt_token: str
    encryption_key: str


class DeleteRequest(BaseModel):
    collection_id: int
    collection_name: str
    files: list[str]


@app.post("/indexing_collection")
async def indexing_collection(request: indexingRequest):
    encryption_key = base64.urlsafe_b64decode(request.encryption_key.encode())
    encryption_key = SseCustomerKey(encryption_key)
    await index.get_info(request.collection_id, request.collection_name,
                        jwt_token=request.jwt_token, encryption_key=encryption_key)


@app.post("/delete_files")
async def delete_files(request: DeleteRequest):
    await index.delete_files(request.collection_id, request.collection_name, request.files)

