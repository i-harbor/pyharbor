## Require
```
pip install requests
```

## Quick Start
#### 全局配置 
首先使用前要先进行配置，包括访问密钥（access_key和secret_key）,域名等   
```python
import pyharbor

pyharbor.set_global_settings({
    'SCHEME': 'http',   # 或'https', 默认'https'
    'DOMAIN_NAME': '10.0.86.213:8000', # 默认 'obs.casearth.cn'
    'ACCESS_KEY': '4203ecc034d411e9b31bc800a000655d',
    'SECRET_KEY': '93c74b39396abd09cb0720a1af52c5c27690a2b8',
    })
```
#### 上传一个文件
```python
import os
import pyharbor

client = pyharbor.get_client()

filename = './test.py'
ok, offset, msg = client.bucket('gggg').dir('u/rrth').put_object(obj_name='test.py', filename=filename)
# 或者
# ok, offset, msg = client.put_object(bucket_name='gggg', obj_name='u/rrth/test.py', filename=filename)
if os.path.getsize(filename) == offset:
    print('上传成功：' + msg)
else:
    print('上传失败：' + msg)

```

#### 删除一个对象 

```python
import pyharbor

client = pyharbor.get_client()
ok, msg = client.bucket('gggg').dir('u/rrth').delete_object('test.py')
# 或者
# ok, msg = client.delete_object(bucket_name='gggg', obj_name='u/rrth/test.py')
if ok:
    print('删除成功：' + msg)
else:
    print('删除失败：' + msg)
```

#### 创建一个目录
```python
import pyharbor

client = pyharbor.get_client()
ok, msg = client.bucket('gggg').dir().create_dir('testdir')
# 或者
# ok, msg = client.create_dir(bucket_name='gggg', dir_name='u/rrth/testdir')
if ok:
    print('创建成功：' + msg)
else:
    print('创建失败：' + msg)
```

#### 删除一个目录
```python
import pyharbor

client = pyharbor.get_client()
ok, msg = client.bucket('gggg').dir().delete_dir('testdir')
# 或者
# ok, msg = client.delete_dir(bucket_name='gggg', dir_name='u/rrth/testdir')
if ok:
    print('删除成功：' + msg)
else:
    print('删除失败：' + msg)
```


#### 获取一个对象元数据
```python
import pyharbor

client = pyharbor.get_client()
data, code, msg = client.bucket('gggg').dir('u/rrth').get_obj_info('test.py')
# 或者
# data, code, msg = client.get_obj_info(bucket_name='gggg', obj_name='u/rrth/test.py')
if data:
    print(data)
else:
    print('获取对象元数据失败：' + msg)
```

#### 下载一个对象
```python
import pyharbor

client = pyharbor.get_client()
# ok, offset, msg = client.bucket('www').dir('testdir').download_object(obj_name='examples.py', filename='./download')
# 或者
ok, offset, msg = client.download_object(bucket_name='www', obj_name='testdir/examples.py', filename='./download')
if ok:
    print('下载对象元数据成功：' + msg)
else:
    print('获取对象元数据失败：' + msg)
```

#### 设置对象访问权限为公有权限，时限7天
```python
import pyharbor

client = pyharbor.get_client()
# ok, msg = client.bucket('www').dir('testdir').share_object(obj_name='examples.py', share=True, days=7)
# 或者
ok, msg = client.share_object(bucket_name='www', obj_name='testdir/examples.py', share=True, days=7)
if ok:
    print('设置成功：' + msg)
else:
    print('设置失败：' + msg)
```

#### 获取桶id和访问权限
```python
import pyharbor

client = pyharbor.get_client()

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

```

#### 分页获取目录下子目录和对象列表
```python
import json
import pyharbor

client = pyharbor.get_client()
# page = client.bucket('www').dir('upload test').list(per_page=100)
# 或者
page = client.list_dir(bucket_name='www', dir_name='upload test', per_page=100)
if page is not None:
    objs = page.get_list()
    print(json.dumps(objs, indent=4))
    
    # 获取下一页
    if page.has_next():
        next_page = page.next_page()
        objs = next_page.get_list()
        print(json.dumps(objs, indent=4))
```

 #### 上传一个数据块到对象
 ```python
import pyharbor

client = pyharbor.get_client()
ok, msg = client.write_one_chunk(bucket_name='www', obj_name='upload test/test_chunk', offset=0, chunk=b'hello')
if ok:
    print('successful', msg)
else:
    print('failed,', msg)
```

#### 从对象读取一个指定大小的数据块
```python
import pyharbor

client = pyharbor.get_client()
ok, data = client.read_one_chunk(bucket_name='www', obj_name='upload test/test_chunk', offset=0, size=10)
if ok:
    chunk = data.get('chunk') # 数据块 bytes
    obj_size = data.get('obj_size') # 对象总大小
    print('successful')
else:
    print('failed,', data)
```

#### 重命名对象
```python
import json
import pyharbor

client = pyharbor.get_client()
ok, data = client.rename_object(bucket_name='wwww', obj_name='dd/test.txt', rename='test2.txt')
# or
# ok, data = client.bucket('wwww').dir('dd').rename_object(obj_name='test.txt', rename='test2.txt')
if ok:
    print('重命名成功:', json.dumps(data, indent=4))
    obj = data.get('obj') # 移动后对象信息
else:
    print('重命名失败:', json.dumps(data, indent=4))
```
#### 移动一个对象
```python
import json
import pyharbor

client = pyharbor.get_client()
ok, data = client.move_object(bucket_name='wwww', obj_name='dd/test2.txt', to='cc')
# or
# ok, data = client.bucket('wwww').dir('dd').move_object(obj_name='test2.txt', to='cc')
if ok:
    print('移动成功:', json.dumps(data, indent=4))
    obj = data.get('obj') # 移动后对象信息
else:
    print('移动失败:', json.dumps(data, indent=4))
```

#### 移动并重命名对象
```python
import json
import pyharbor

client = pyharbor.get_client()
ok, data = client.move_object(bucket_name='wwww', obj_name='cc/test2.txt', to='dd', rename='test.txt')
# or
# ok, data = client.bucket('wwww').dir('cc').move_object(obj_name='test2.txt', to='dd', rename='test.txt')
if ok:
    print('移动重命名成功:', json.dumps(data, indent=4))
    obj = data.get('obj') # 移动后对象信息
else:
    print('移动重命名失败:', json.dumps(data, indent=4))
```

#### 是否是目录
```python
import pyharbor

client = pyharbor.get_client()
ok = client.isdir(bucket_name='wwww', dir_name='cc/dd')
print(ok) # True or False
```


#### 是否是文件对象
```python
import pyharbor

client = pyharbor.get_client()
ok = client.isfile(bucket_name='wwww', filename='cc/dd/api.py')
print(ok) # True or False
```
