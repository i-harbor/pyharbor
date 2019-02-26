

DEFAULT_CONFIGS = {
    'VERSION': 'v1',
    'DOMAIN_NAME': 'http://obs.casearth.cn/',
    'OBJ_API_PREFIX': 'obj',
    'DIR_API_PREFIX': 'dir',
    'BUCKET_API_PREFIX': 'buckets',
}

def set_global_settings(settings):
    '''全局设置'''
    global configs
    cfgs = _prepare_settings(settings)
    configs._configs = cfgs

def join_url_with_slash(s, *args):
    '''
    以斜线拼接url，拼接的url结尾不含‘/’

    :param s: 第一个url部分
    :param args: 其他url部分
    :return:
    '''
    li = [item.strip('/') for item in args ]
    s = s.rstrip('/')
    li.insert(0, s)
    return '/'.join(li)

def _prepare_settings(settings):
    for key, value in DEFAULT_CONFIGS.items():
        if key not in settings:
            settings[key] = value

    domain_name = settings['DOMAIN_NAME']
    api_version = settings['VERSION']
    api_version_url = join_url_with_slash(domain_name, 'api', api_version)
    settings['API_VERSION_URL'] = api_version_url

    # 对象API基url
    obj_api_prefix = settings['OBJ_API_PREFIX']
    obj_api_url_base = join_url_with_slash(api_version_url, obj_api_prefix)
    settings['OBJ_API_URL_BASE'] = obj_api_url_base

    # 目录API基url
    dir_api_prefix = settings['DIR_API_PREFIX']
    dir_api_url_base = join_url_with_slash(api_version_url, dir_api_prefix)
    settings['DIR_API_URL_BASE'] = dir_api_url_base

    # 存储桶API基url
    bucket_api_prefix = settings['BUCKET_API_PREFIX']
    bucket_api_url_base = join_url_with_slash(api_version_url, bucket_api_prefix)
    settings['BUCKET_API_URL_BASE'] = bucket_api_url_base

    return settings


def set_global_auth_key(access_key, secret_key):
    '''
    配置全局的访问秘钥
    '''    
    if not isinstance(access_key, str) or not isinstance(secret_key, str):
        raise ValueError('access_key and secret_key must be string.')

    global configs

    configs.ACCESS_KEY = access_key
    configs.SECRET_KEY = secret_key


class Configs(object):
    def __init__(self, configs=None):
        self._configs = configs

    def __getattr__(self, name):
        """
        Return the value of a config and cache it in self.__dict__.
        """
        if self._configs is None:
            raise AttributeError('Please use the function set_global_settings for global settings first.')

        val = self._configs.get(name)
        self.__dict__[name] = val
        return val

    def __setattr__(self, name, value):
        """
        Set the value of config. Clear all cached values if _configs changes
        or clear single values when set.
        """
        if name == '_configs':
            self.__dict__.clear()
            self.__dict__['_configs'] = value
        else:
            self.__dict__.pop(name, None)
            self._configs[name] = value
            

    def __delattr__(self, name):
        """
        Delete a setting and clear it from cache if needed.
        """
        if name == '_configs':
            raise TypeError("can't delete _configs.")

        self._configs.pop(name)
        self.__dict__.pop(name, None)
        
        
configs = Configs()

