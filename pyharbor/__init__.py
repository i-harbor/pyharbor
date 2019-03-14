from .config import set_global_auth_key, set_global_settings, configs
from .api import Client, Directory
from .core import ApiCore


def get_client():
    return Client()




