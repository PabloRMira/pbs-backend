import ssl

import requests
from urllib3 import poolmanager


class CustomHttpAdapter(requests.adapters.HTTPAdapter):
    "Transport adapter that allows us to use custom ssl_context"

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=self.ssl_context
        )


def get_oecd_response(url: str):
    "Custom API fetch because OECD has not updated API to new SSL standards"
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4
    session = requests.session()
    session.mount("https://", CustomHttpAdapter(ctx))
    return session.get(url)
