from .config import set_global_auth_key, set_global_settings, configs
from .api import Client


DEFAULT_CLIENT = None

def get_client():
    global DEFAULT_CLIENT

    if not DEFAULT_CLIENT:
        DEFAULT_CLIENT = Client()

    return DEFAULT_CLIENT


