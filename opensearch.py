from opensearchpy import NotFoundError, AsyncOpenSearch

import config

# auth = ('admin', os.getenv('OPENSEARCH_PASS'))
# For testing only. Don't store credentials in code.
auth = ('admin', 'OTFiZDkwMGRiOWQw1!')


class OpenSearchManager:
    def __init__(self, host: str = config.open_search_host, port: int = config.open_search_port, auth: tuple = auth):
        self.host = host
        self.port = port
        self.auth = auth

    # Не работает
    async def create_index(self, index_name: str = config.open_search_files_index):
        async with AsyncOpenSearch(
            hosts=[{'host': self.host, 'port': self.port}],
            http_compress=True,
            http_auth=auth,
            use_ssl=True,
            verify_certs=not config.debug_mode,
            ssl_assert_hostname=not config.debug_mode,
            ssl_show_warn=not config.debug_mode,
        ) as client:
            response = await client.indices.create(
                index=index_name)

    async def update_document(self, doc_id: int | str, document: dict, index_name: str = config.open_search_files_index):
        async with AsyncOpenSearch(
            hosts=[{'host': self.host, 'port': self.port}],
            http_compress=True,
            http_auth=auth,
            use_ssl=True,
            verify_certs=not config.debug_mode,
            ssl_assert_hostname=not config.debug_mode,
            ssl_show_warn=not config.debug_mode,
        ) as client:
            response = await client.index(
                index=index_name,
                body=document,
                id=doc_id,
                refresh=True,
            )

    async def delete_document(self, doc_id: int | str, index_name: str = config.open_search_files_index):
        async with AsyncOpenSearch(
            hosts=[{'host': self.host, 'port': self.port}],
            http_compress=True,
            http_auth=auth,
            use_ssl=True,
            verify_certs=not config.debug_mode,
            ssl_assert_hostname=not config.debug_mode,
            ssl_show_warn=not config.debug_mode,
        ) as client:
            response = await client.delete(
                index=index_name,
                id=doc_id,
            )

    async def search_and_delete_files(self, path: str, collection_id: int, collection_name: str, index_name: str = config.open_search_files_index):
        path = path.strip('/')
        # path = path.replace('/', '\/')
        print(f'path: /{path}')
        query = {
            'query': {
                'bool': {
                    'must': [
                        {'term': {'collection_id': collection_id}},
                        {'bool': {
                            'should': [
                                {'term': {'path.keyword': f'/{path}'}},
                                {'prefix': {'path.keyword': f'/{path}/'}}
                            ]
                        }}
                    ]
                }
            }
        }
        async with AsyncOpenSearch(
            hosts=[{'host': self.host, 'port': self.port}],
            http_compress=True,
            http_auth=auth,
            use_ssl=True,
            verify_certs=not config.debug_mode,
            ssl_assert_hostname=not config.debug_mode,
            ssl_show_warn=not config.debug_mode,
        ) as client:
            response = await client.delete_by_query(
                body=query,
                index=index_name,
            )
        print(response)

    async def get_document(self, doc_id: int | str, index_name: str = config.open_search_files_index) -> dict | None:
        async with AsyncOpenSearch(
            hosts=[{'host': self.host, 'port': self.port}],
            http_compress=True,
            http_auth=auth,
            use_ssl=True,
            verify_certs=not config.debug_mode,
            ssl_assert_hostname=not config.debug_mode,
            ssl_show_warn=not config.debug_mode,
        ) as client:
            try:
                response = await client.get(
                    index=index_name,
                    id=doc_id,
                )
                return response['_source']
            except NotFoundError:
                return None
