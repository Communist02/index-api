import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
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
    path: str = ''


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


@app.get("/", include_in_schema=False)
async def redirect_to_docs() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/status")
async def get_status() -> dict[str, str | int | bool | list[dict]]:
    status = {
        'status': 'active',  # active, inactive, failed
        'type': 'api',
        'urls': {
            'Документация': '/docs',
            'Репозиторий': 'http://git.eco.dvo.ru:3000/mazur/index-api'
        },
        'agents': list(await asyncio.gather(
            index.opensearch.get_s3_status(),
            index.get_status()
        ))
    }
    return status


@app.post("/indexing_collection")
async def indexing_collection(request: indexingCollectionRequest):
    encryption_key = base64.urlsafe_b64decode(request.encryption_key.encode())
    encryption_key = SseCustomerKey(encryption_key)
    await index.indexing_collection(request.collection_id, request.collection_name,
                                    jwt_token=request.jwt_token, encryption_key=encryption_key, path=request.path)


@app.post("/indexing_files")
async def indexing_files(request: indexingFilesRequest):
    encryption_key = base64.urlsafe_b64decode(request.encryption_key.encode())
    encryption_key = SseCustomerKey(encryption_key)
    await index.indexing_files(request.collection_id, request.collection_name,
                               jwt_token=request.jwt_token, encryption_key=encryption_key, files=request.files)


@app.post("/delete_files")
async def delete_files(request: DeleteRequest):
    await index.delete_files(request.collection_id, request.collection_name, request.files)
