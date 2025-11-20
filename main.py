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


class indexingCollectionRequest(BaseModel):
    collection_id: int
    collection_name: str
    jwt_token: str
    encryption_key: str


class indexingFilesRequest(BaseModel):
    collection_id: int
    collection_name: str
    jwt_token: str
    encryption_key: str
    files: list[str]


class DeleteRequest(BaseModel):
    collection_id: int
    collection_name: str
    files: list[str]


@app.post("/indexing_collection")
async def indexing_collection(request: indexingCollectionRequest):
    encryption_key = base64.urlsafe_b64decode(request.encryption_key.encode())
    encryption_key = SseCustomerKey(encryption_key)
    await index.indexing_collection(request.collection_id, request.collection_name,
                        jwt_token=request.jwt_token, encryption_key=encryption_key)


@app.post("/indexing_files")
async def indexing_files(request: indexingFilesRequest):
    encryption_key = base64.urlsafe_b64decode(request.encryption_key.encode())
    encryption_key = SseCustomerKey(encryption_key)
    await index.indexing_files(request.collection_id, request.collection_name,
                        jwt_token=request.jwt_token, encryption_key=encryption_key, files=request.files)


@app.post("/delete_files")
async def delete_files(request: DeleteRequest):
    await index.delete_files(request.collection_id, request.collection_name, request.files)

