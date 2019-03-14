import os

import pyharbor


# 配置
pyharbor.set_global_settings({
    'SCHEME': 'http',   # 或'https', 默认'https'
    # 'DOMAIN_NAME': '10.0.86.213:8000', # 默认 'obs.casearth.cn'
    'ACCESS_KEY': '3b03b89e44a411e98dfcc8000a00c8d4',
    'SECRET_KEY': 'eeef1cb1f6839c071df44507243e957f1aa548a6',
    })


client = pyharbor.get_client()

# 创建一个目录
# ok, msg = client.bucket('www').dir().create_dir('testdir')
# 或者
ok, msg = client.create_dir(bucket_name='www', dir_name='testdir')
if ok:
    print('创建成功：' + msg)
else:
    print('创建失败：' + msg)

# 上传一个文件
filename = './examples.py'
# ok, offset, msg = client.bucket('www').dir('testdir').put_object(obj_name='examples.py', filename=filename)
# 或者
ok, offset, msg = client.put_object(bucket_name='www', obj_name='testdir/examples.py', filename=filename)
if os.path.getsize(filename) == offset:
    print('上传成功：' + msg)
else:
    print('上传失败：' + msg)


# 设置对象访问权限为公有权限，公开7天
# ok, msg = client.bucket('www').dir('testdir').share_object(obj_name='examples.py', share=True, days=7)
# 或者
ok, msg = client.share_object(bucket_name='www', obj_name='testdir/examples.py', share=True, days=7)
if ok:
    print('设置成功：' + msg)
else:
    print('设置失败：' + msg)


# 获取一个对象元数据
# data, code, msg = client.bucket('www').dir('testdir').get_obj_info('examples.py')
# 或者
data, code, msg = client.get_obj_info(bucket_name='www', obj_name='testdir/examples.py')
if data:
    print(data)
else:
    print('获取对象元数据失败：' + msg)


# 下载一个对象
# ok, offset, msg = client.bucket('www').dir('testdir').download_object(obj_name='examples.py', filename='./download')
# 或者
ok, offset, msg = client.download_object(bucket_name='www', obj_name='testdir/examples.py', filename='./download')
if ok:
    print('下载对象元数据成功：' + msg)
else:
    print('获取对象元数据失败：' + msg)


# 获取目录下的子目录和对象信息
testdir = client.bucket('www').dir('testdir')
data, code, msg = testdir.get_objs_and_subdirs()
if data:
    objs = testdir.get_objs(data)
    print('目录下的对象：', objs)
else:
    print('获取目录下的子目录和文件信息失败')


# 删除一个对象
# ok, msg = client.bucket('www').dir('testdir').delete_object('examples.py')
# 或者
ok, msg = client.delete_object(bucket_name='www', obj_name='testdir/examples.py')
if ok:
    print('删除成功：' + msg)
else:
    print('删除失败：' + msg)


# 删除一个目录
# ok, msg = client.bucket('www').dir().delete_dir('testdir')
# 或者
ok, msg = client.delete_dir(bucket_name='www', dir_name='testdir')
if ok:
    print('删除成功：' + msg)
else:
    print('删除失败：' + msg)


# 获取bucket列表
import json
buckets_data, msg = client.get_buckets()
if buckets_data:
    print('获取bucket列表成功：\n', json.dumps(buckets_data, indent=4))
else:
    print('获取bucket列表失败')


# 获取桶id
bucket_name = 'www'
bucket = client.bucket(bucket_name)
print(f'bucket({bucket_name}) id = {bucket.id}')

# 设置桶为公有访问权限
ok, msg = bucket.set_permission(public=True)
if ok:
    print('桶公有权限设置成功')
else:
    print('桶公有权限设置失败')

# 设置桶为私有访问权限
ok, msg = client.bucket_permission(bucket_name=bucket_name, public=False)
if ok:
    print('桶私有权限设置成功')
else:
    print('桶私有权限设置失败')

