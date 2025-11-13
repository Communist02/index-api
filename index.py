import asyncio
from datetime import UTC, datetime
from fastapi import HTTPException
from minio import Minio, S3Error
from minio.sse import SseCustomerKey
from osgeo import gdal
import config
from get_token import get_sts_token
from opensearch import OpenSearchManager


opensearch = OpenSearchManager()


class IndexManager():
    def __init__(self, endpoint_minio: str = config.minio_url):
        self.endpoint_minio = endpoint_minio

    async def delete_files(self, collection_id: int, collection_name: str, files: list[str]):
        for path in files:
            await opensearch.search_and_delete_files(path, collection_id, collection_name)

    async def get_info(self, collection_id: int, collection_name: str, jwt_token: str, encryption_key: SseCustomerKey, path: str = '', recursive: bool = True) -> list[dict]:
        auth = get_sts_token(jwt_token, 'https://' + config.minio_url, 0)
        client = Minio(self.endpoint_minio, auth['access_key'], auth['secret_key'],
                       auth['session_token'], secure=True, cert_check=not config.debug_mode)

        try:
            if path:
                prefix = path.strip('/') + '/'
                objects = await asyncio.to_thread(
                    client.list_objects,
                    collection_name,
                    recursive=recursive,
                    prefix=prefix
                )
            else:
                objects = await asyncio.to_thread(
                    client.list_objects,
                    collection_name,
                    recursive=recursive
                )
            result = []

            for obj in objects:
                object_name = obj.object_name
                if not object_name.endswith('NODATA') and not obj.is_dir:
                    file = {
                        'collection_id': collection_id,
                        'name': obj.object_name[obj.object_name.rfind('/', 0, -1 if obj.is_dir else -2) + 1:],
                        'isDirectory': obj.is_dir,
                        'path': f'/{object_name}',
                        'size': obj.size,
                    }
                    if obj.last_modified:
                        file['updatedAt'] = obj.last_modified.isoformat()
                    result.append(file)

            for file in result:
                file_metadata = {
                    'collection_id': collection_id,
                    'path': file['path'],
                    'name': file['name'],
                    'size': file.get('size', 0),
                    'format': file['name'].split('.')[-1],
                    'last_modified': file.get('last_modified', file.get('updateAt', datetime.now(UTC).timestamp()))
                }

                document = await opensearch.get_document(
                    f'{collection_id}{file['path']}')
                if document is None or document['size'] != file['size']:
                    obj = await asyncio.to_thread(
                        client.get_object,
                        collection_name,
                        object_name=file['path'],
                        ssec=encryption_key
                    )
                    content = await asyncio.to_thread(obj.read)

                    # Создаем виртуальный файл в памяти GDAL
                    vsi_path = f"/vsimem/temp_{hash(file['path'])}.{file_metadata['format']}"

                    # Записываем данные в виртуальную файловую систему GDAL
                    gdal.FileFromMemBuffer(vsi_path, content)

                    # Открываем через GDAL
                    dataset = gdal.Open(vsi_path)
                    if dataset is not None:
                        data = await self.extract_metadata(dataset, file_metadata)
                        await opensearch.update_document(
                            f'{collection_id}{file['path']}',
                            data
                        )
                    else:
                        await opensearch.update_document(
                            f'{collection_id}{file['path']}',
                            file_metadata
                        )
                    gdal.Unlink(vsi_path)

        except S3Error as error:
            print(f'Error fetching files: {error.message}, {error.code}')
            if error.code == 'NoSuchBucket':
                raise HTTPException(
                    status_code=410,
                    detail=f"No such bucket '{collection_name}': {error.message}"
                )
            elif error.code == 'AccessDenied':
                raise HTTPException(
                    status_code=423,
                    detail=f"Access Denied '{collection_name}': {error.message}"
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail={
                        'error': 'Failed to retrieve files',
                        'message': error.message
                    }
                )

    async def extract_metadata(self, dataset: gdal.Dataset, file_metadata: str):
        geotransform = dataset.GetGeoTransform()
        metadata = dataset.GetMetadata()

        doc = {
            **file_metadata,
            "raster_properties": {
                "width": dataset.RasterXSize,
                "height": dataset.RasterYSize,
                "band_count": dataset.RasterCount,
                "pixel_size": {
                    "x": abs(geotransform[1]) if geotransform else None,
                    "y": abs(geotransform[5]) if geotransform else None
                }
            },
            "gdal_metadata": metadata
        }

        return doc
