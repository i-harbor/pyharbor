import os

import pyharbor


# 配置
pyharbor.set_global_settings({
    'SCHEME': 'http',   # 或'https', 默认'http'
    'DOMAIN_NAME': '10.0.86.213:8000', # 默认 'obs.casearth.cn'
    'ACCESS_KEY': '4203ecc034d411e9b31bc800a000655d',
    'SECRET_KEY': '93c74b39396abd09cb0720a1af52c5c27690a2b8',
    })


client = pyharbor.get_client()

# 上传一个文件
filename = './test.py'
ok, offset, msg = client.bucket('gggg').dir('u/rrth').put_object(obj_name='test.py', filename=filename)
# 或者
# ok, offset, msg = client.put_object(bucket_name='gggg', obj_name='u/rrth/test.py', filename=filename)
if os.path.getsize(filename) == offset:
    print('上传成功：' + msg)
else:
    print('上传失败：' + msg)


# 设置对象访问权限为公有权限，公开7天
ok, msg = client.bucket('gggg').dir('u/rrth').share_object(obj_name='test.py', share=True, days=7)
# 或者
# ok, msg = client.share_object(bucket_name='gggg', obj_name='u/rrth/test.py', share=True, days=7)
if os.path.getsize(filename) == offset:
    print('设置成功：' + msg)
else:
    print('设置失败：' + msg)


# 获取一个对象元数据
data, code, msg = client.bucket('gggg').dir('u/rrth').get_obj_info('test.py')
# 或者
# data, code, msg = client.get_obj_info(bucket_name='gggg', obj_name='u/rrth/test.py')
if data:
    print(data)
else:
    print('获取对象元数据失败：' + msg)


# 删除一个对象
ok, msg = client.bucket('gggg').dir('u/rrth').delete_object('test.py')
# 或者
# ok, msg = client.delete_object(bucket_name='gggg', obj_name='u/rrth/test.py')
if ok:
    print('删除成功：' + msg)
else:
    print('删除失败：' + msg)


# 创建一个目录
ok, msg = client.bucket('gggg').dir().create_dir('testdir')
# 或者
# ok, msg = client.create_dir(bucket_name='gggg', dir_name='u/rrth/testdir')
if ok:
    print('创建成功：' + msg)
else:
    print('创建失败：' + msg)


# 删除一个目录
ok, msg = client.bucket('gggg').dir().delete_dir('testdir')
# 或者
# ok, msg = client.delete_dir(bucket_name='gggg', dir_name='u/rrth/testdir')
if ok:
    print('删除成功：' + msg)
else:
    print('删除失败：' + msg)
