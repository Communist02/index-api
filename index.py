import asyncio
from datetime import UTC, datetime
import json
from fastapi import HTTPException
from minio import Minio, S3Error
from minio.sse import SseCustomerKey
from osgeo import gdal, osr
import config
from get_token import get_sts_token
from opensearch import OpenSearchManager
from convert import flatten_dict_with_template


opensearch = OpenSearchManager()
TypeList = {int: "int", float: "float", list: "list",
            dict: "dict", str: "str", bool: "bool"}


def OneItemIn(host, k, v, setup):
    if v is None:
        return "["
    if type(v) == str and v == "":
        return "{"
    return str(k)+ ": " + str(v)


def OneItemOut(host, k, v, setup):
    if v is None:
        return "]" + "\n"
    if type(v) == str and v == "":
        return "}" + "\n"
    return ""


def transformList(name, l, setup=None, host=None):
    lines = [OneItemIn(host, name, "", setup)]
    for k in l:
        lines.append(OneItemIn(host, k, None, setup))
        lines.append(OneItemOut(host, k, None, setup))
    lines.append(OneItemOut(host, name, "", setup))
    return lines


def transformDict(name, d, setup=None, host=None):
    lines = [OneItemIn(host, name, "", setup)]
    for k, v in d.items():
        if type(v) is dict:
            lines.extend(transformDict(k, v, setup, host))
        if type(v) is list:
            lines.extend(transformList(k, v, setup, host))
        lines.append(OneItemIn(host, k, v, setup))
        lines.append(OneItemOut(host, k, v, setup))
    lines.append(OneItemOut(host, name, "", setup))
    return lines


class IndexManager():
    def __init__(self, endpoint_minio: str = config.minio_url):
        self.endpoint_minio = endpoint_minio

    async def delete_files(self, collection_id: int, collection_name: str, files: list[str]):
        for path in files:
            await opensearch.search_and_delete_files(path, collection_id, collection_name)

    async def get_info(self, collection_id: int, collection_name: str, jwt_token: str, encryption_key: SseCustomerKey, path: str = '', recursive: bool = True) -> list[dict]:
        auth = await get_sts_token(jwt_token, 'https://' + config.minio_url, 0)
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

    async def extract_metadata(self, dataset: gdal.Dataset, file_metadata: dict):
        geotransform = dataset.GetGeoTransform()
        projection = dataset.GetProjection()

        # --- Projection / Spatial Reference ---
        srs = None
        epsg = None
        pretty_wkt = None
        proj4 = None
        is_projected = None
        is_geographic = None
        authority = None

        if projection:
            try:
                srs = osr.SpatialReference(wkt=projection)
                pretty_wkt = srs.ExportToPrettyWkt()
                proj4 = srs.ExportToProj4()
                is_projected = srs.IsProjected()
                is_geographic = srs.IsGeographic()
                authority = srs.GetAuthorityCode(None)

                # EPSG retrieval: try AUTHORITY:1, fallback to autoIdentifyEPSG()
                epsg = srs.GetAttrValue("AUTHORITY", 1)

                if not epsg:
                    try:
                        srs.AutoIdentifyEPSG()
                        epsg = srs.GetAuthorityCode(None)
                    except:
                        pass

            except Exception:
                pass

        # --- Extent / Bounding Box ---
        if geotransform:
            minx = geotransform[0]
            maxy = geotransform[3]
            maxx = minx + geotransform[1] * dataset.RasterXSize
            miny = maxy + geotransform[5] * dataset.RasterYSize

            extent = {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy}
            bbox = [minx, miny, maxx, maxy]
        else:
            extent = None
            bbox = None

        # --- Specialized metadata domains ---
        image_structure = dataset.GetMetadata("IMAGE_STRUCTURE")
        rpc_metadata = dataset.GetMetadata("RPC")
        geolocation = dataset.GetMetadata("GEOLOCATION")
        exif = dataset.GetMetadata("EXIF")
        subdatasets = dataset.GetSubDatasets()

        # --- COG heuristic ---
        is_cog = (
            image_structure is not None
            and image_structure.get("LAYOUT") == "IFDS"
            and image_structure.get("TILED") == "YES"
            and "COMPRESSION" in image_structure
        )

        # --- Bands ---
        bands = []
        for idx in range(1, dataset.RasterCount + 1):
            band = dataset.GetRasterBand(idx)

            try:
                block_size = band.GetBlockSize()
            except:
                block_size = None

            try:
                color_interp = gdal.GetColorInterpretationName(
                    band.GetColorInterpretation()
                )
            except:
                color_interp = None

            try:
                nodata = band.GetNoDataValue()
            except:
                nodata = None

            try:
                datatype = gdal.GetDataTypeName(band.DataType)
            except:
                datatype = None

            # Statistics
            try:
                min_val, max_val, mean_val, std_val = band.GetStatistics(0, 1)
                stats = {
                    "min": min_val,
                    "max": max_val,
                    "mean": mean_val,
                    "std": std_val
                }
            except:
                stats = None

            # Overviews
            overviews = []
            try:
                for oi in range(band.GetOverviewCount()):
                    ov = band.GetOverview(oi)
                    overviews.append({
                        "index": oi,
                        "width": ov.XSize,
                        "height": ov.YSize,
                        "datatype": gdal.GetDataTypeName(ov.DataType)
                    })
            except:
                pass

            bands.append({
                "index": idx,
                "datatype": datatype,
                "nodata": nodata,
                "stats": stats,
                "color_interpretation": color_interp,
                "block_size": block_size,
                "overviews": overviews,
                "metadata": band.GetMetadata()
            })

        # --- Driver Info ---
        driver = dataset.GetDriver()
        driver_info = {
            "short_name": driver.ShortName,
            "long_name": driver.LongName
        }

        # --- GCPs ---
        gcps = dataset.GetGCPs()
        gcp_projection = dataset.GetGCPProjection()

        # --- Final document assembly ---
        doc = {
            **file_metadata,
            "other": {
                "raster_properties": {
                    "width": dataset.RasterXSize,
                    "height": dataset.RasterYSize,
                    "band_count": dataset.RasterCount,
                    "pixel_size": {
                        "x": abs(geotransform[1]) if geotransform else None,
                        "y": abs(geotransform[5]) if geotransform else None,
                    },
                },
                "metadata": dataset.GetMetadata(),
                "image_structure": image_structure,
                "rpc": rpc_metadata,
                "geolocation": geolocation,
                "exif": exif,
                "subdatasets": subdatasets,
                "is_cog": is_cog,
                "bands": bands,
                "gcps": gcps,
                "gcp_projection": gcp_projection,
                "driver": driver_info,
                "projection": {
                    "raw_wkt": projection,
                    "pretty_wkt": pretty_wkt,
                    "epsg": epsg,
                    "proj4": proj4,
                    "authority": authority,
                    "is_projected": is_projected,
                    "is_geographic": is_geographic,
                },
                "geotransform": geotransform,
                "extent": extent,
                "bbox": bbox,
            },
        }
        # doc['other_text'] = json.dumps(doc['other'])
        # doc['other_text'] = "\n".join(self.dict_to_markdown(doc['other']))
        doc['other_text'] = "\n".join(flatten_dict_with_template(doc['other']))
        # print(self.flatten_for_search(doc['other_text']))
        # print("\n".join(transformDict('', doc['other'])))

        return doc
