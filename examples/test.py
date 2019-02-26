from pyharbor import set_global_auth_key, set_global_settings, configs
from pyharbor import request, api

DOMAIN_NAME = 'http://10.0.86.213:8000/'
BUCKET_NAME = 'sfaf'
OBJ_BASE_URL = DOMAIN_NAME + 'api/v1/obj/'


set_global_settings({
    'VERSION': 'v1',
    'DOMAIN_NAME': 'http://10.0.86.213:8000/',
    'ACCESS_KEY': '4203ecc034d411e9b31bc800a000655d',
    'SECRET_KEY': '93c74b39396abd09cb0720a1af52c5c27690a2b8',
    })


# set_global_auth_key(access_key='4203ecc034d411e9b31bc800a000655d',
#                     secret_key='93c74b39396abd09cb0720a1af52c5c27690a2b8')

# ok = api.put_file(obj_url=OBJ_BASE_URL + BUCKET_NAME + '/Sublime Text Build 3176 x64 Setup.exe/', filename='./Sublime Text Build 3176 x64 Setup.exe')
# print(ok)

# ok = api.download_file(obj_url=OBJ_BASE_URL + BUCKET_NAME + '/Sublime Text Build 3176 x64 Setup.exe/', filename='./Sublime Text Build 3176 x64 Setup.exe')
# print(ok)

api.create_one_dir(bucket_name=BUCKET_NAME, dir_name='test')
ok = api.create_path(bucket_name=BUCKET_NAME, dir_path='1/2/3/4', base_dir='test')
print(ok)
