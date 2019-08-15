import os

import pyharbor


# 配置
pyharbor.set_global_settings({
    'SCHEME': 'http',   # 或'https', 默认'https'
    # 'DOMAIN_NAME': '10.0.86.213:8000', # 默认 'obs.casearth.cn'
    'ACCESS_KEY': '0a4898b83a6911e9a040c8000a00c8d',
    'SECRET_KEY': '5e3c1affe3900146b2795283c9a4110e925b0ee',
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


# 获取一个对象或目录元数据
# data, code, msg = client.bucket('www').dir('testdir').get_metadata('examples.py')
# 或者
data, code, msg = client.get_metadata(bucket_name='www', filename='testdir/examples.py')
if data:
    print(data)
else:
    print('获取对象元数据失败：' + msg)


# 下载一个对象
# ok, offset, msg = client.bucket('www').dir('testdir').download_object(obj_name='examples.py', filename='./download')
# 或者
ok, offset, msg = client.download_object(bucket_name='www', obj_name='testdir/examples.py', filename='./download')
if ok:
    print('下载对象成功：' + msg)
else:
    print('获取对象失败：' + msg)


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


# 分页获取目录下子目录和对象列表
# page = client.bucket('www').dir('upload test').list(per_page=100)
# 或者
page = client.list_dir(bucket_name='www', dir_name='upload test', per_page=2)
if page is not None:
    objs = page.get_list()
    print(json.dumps(objs, indent=4))

    # 获取下一页
    if page.has_next():
        next_page = page.next_page()
        objs = next_page.get_list()
        print(json.dumps(objs, indent=4))


# 上传一个数据块到对象
ok, msg = client.write_one_chunk(bucket_name='www', obj_name='upload test/test_chunk', offset=0, chunk=b'hello')
if ok:
    print('已成功上传一个数据块到对象,', msg)
else:
    print('上传一个数据块到对象失败,', msg)

# 从对象读取一个指定大小的数据块
ok, data = client.read_one_chunk(bucket_name='www', obj_name='upload test/test_chunk', offset=0, size=10)
if ok:
    chunk = data.get('chunk')
    obj_size = data.get('obj_size')
    print('已成功从对象读取一个指定大小的数据块:' + chunk.decode() + ';对象总大小：{0}'.format(obj_size))
else:
    print('从对象读取一个指定大小的数据块失败,', msg)


# 重命名对象
ok, data = client.rename_object(bucket_name='wwww', obj_name='dd/test.txt', rename='test2.txt')
# or
# ok, data = client.bucket('wwww').dir('dd').rename_object(obj_name='test.txt', rename='test2.txt')
if ok:
    print('重命名成功:', json.dumps(data, indent=4))
    obj = data.get('obj') # 移动后对象信息
else:
    print('重命名失败:', json.dumps(data, indent=4))

# 移动一个对象
ok, data = client.move_object(bucket_name='wwww', obj_name='dd/test2.txt', to='cc')
# or
# ok, data = client.bucket('wwww').dir('dd').move_object(obj_name='test2.txt', to='cc')
if ok:
    print('移动成功:', json.dumps(data, indent=4))
    obj = data.get('obj') # 移动后对象信息
else:
    print('移动失败:', json.dumps(data, indent=4))


# 移动并重命名对象
ok, data = client.move_object(bucket_name='wwww', obj_name='cc/test2.txt', to='dd', rename='test.txt')
# or
# ok, data = client.bucket('wwww').dir('cc').move_object(obj_name='test2.txt', to='dd', rename='test.txt')
if ok:
    print('移动重命名成功:', json.dumps(data, indent=4))
    obj = data.get('obj') # 移动后对象信息
else:
    print('移动重命名失败:', json.dumps(data, indent=4))

# 是否是目录
ok = client.isdir(bucket_name='wwww', dir_name='cc/dd')
print(ok)

# 是否是文件对象
ok = client.isfile(bucket_name='wwww', filename='cc/dd/api.py')
print(ok)
