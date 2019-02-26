import os

from . import request
from . import configs
from .config import join_url_with_slash


def chunks(fd, offset=0, chunk_size=5*1024**2):
    '''
    Read the file and yield chunks of ``chunk_size`` bytes

    :param fd: 文件描述符(file descriptor)
    :param offset: 开始读取的偏移量
    :param chunk_size: 分片大小
    :return:
    '''
    fd.seek(offset)

    while True:
        data = fd.read(chunk_size)
        if not data:
            break
        yield data

def get_size(fd):
    '''
    获取文件大小

    :param fd: 文件描述符(file descriptor)
    :return:
    '''
    if hasattr(fd, 'size'):
        return fd.size
    if hasattr(fd, 'name'):
        try:
            return os.path.getsize(fd.name)
        except (OSError, TypeError):
            pass
    if hasattr(fd, 'tell') and hasattr(fd, 'seek'):
        pos = fd.tell()
        fd.seek(0, os.SEEK_END)
        size = fd.tell()
        fd.seek(pos)
        return size
    raise AttributeError("Unable to determine the file's size.")

def get_response_msg(response, key='code_text'):
    '''
    获取http响应回复结果的描述信息
    '''
    try:
        data = response.json()
        if key in data:
            msg = data[key]
        elif 'code_text' in data:
            msg = data['code_text']
        elif 'detail' in data:
            msg = data['detail']
        else:
            msg = response.text
    except ValueError:
        msg = response.text

    return msg if msg else ''


def upload_one_chunk(obj_url, offset, chunk, **kwargs):
    '''
    上传一个分片

    :param obj_url: 对象url
    :param offset: 分片偏移量
    :param chunk: 分片
    :return:
        success: (True, code, msg)
        failure: (False, code, msg)
        可能参数有误，目录路径不存在等各种原因不具备上传条件: (None, 0, msg)
    '''
    try:
        r = request.put(obj_url, files={'chunk': chunk},
            data={"chunk_offset": offset, "chunk_size": len(chunk)}, **kwargs)
    except request.ConnectionError as e:
        return (False, 0, str(e))

    msg = get_response_msg(r)

    if r.status_code == 200:
        return (True, 200, msg)
    elif r.status_code == 404: # 可能目录路径不存在
        return (None, 404, msg)
    elif 400 <= r.status_code < 500:
        return (None, r.status_code, msg)

    return False, r.status_code, msg


def upload_file(obj_url, filename, start=0):
    '''
    上传一个文件

    :param obj_url: 对象url
    :param filename: 要上传文件的绝对路径
    :param start: 开始上传的偏移量
    :return:
        (ok, offset, msg)
        ok: True or False, 指示上传是否成功
        offset: 已上传文件的偏移量
        msg: 上传结果描述字符串

    '''
    if not os.path.exists(filename):
        raise FileNotFoundError() 

    offset = start
    with open(filename, 'rb') as f:
        size = get_size(f)
        for chunk in chunks(f, offset=start):
            if not chunk:
                if offset >= size:
                    break
                continue
                  
            ok, code, msg = upload_one_chunk(obj_url=obj_url, offset=offset, chunk=chunk)
            if not ok:
                return False, offset, 'upload failed:' + msg

            offset += len(chunk)

        return True, offset, 'upload successfull'


def download_one_chunk(obj_url, offset, size):
    '''
    下载一个分片

    :param obj_url: 对象url
    :param offset: 分片偏移量
    :param size: 要下载的分片大小
    :return:
        success: (True, {'chunk': chunk, 'obj_size': xx})
        failure: (False, msg)
        404: (None, msg)
    '''
    try:
        r = request.get(obj_url, params={'offset': offset, 'size': size})
    except Exception as e:
        return (False, str(e))

    if r.status_code == 200:
        chunk = r.content
        chunk_size = int(r.headers.get('evob_chunk_size', None))
        obj_size = int(r.headers.get('evob_obj_size', 0))

        if chunk_size is not None and chunk_size != len(chunk):
            return (False, '读取的数据和服务器返回的数据大小不一致')

        return (True, {'chunk': chunk, 'obj_size': obj_size})

    msg = get_response_msg(r)
    if r.status_code in [400, 404]:
        return (None, msg)

    return (False, msg)

def _download_chunk(obj_url, offset, size):
    '''
    下载一个分片，失败重试一次

    :param obj_url: 对象url
    :param offset: 分片偏移量
    :param size: 要下载的分片大小
    :return:
        success: (True, {'chunk': chunk, 'obj_size': xx})
        failure: (False, msg)
        404: (None, msg)
    '''
    ok, result = download_one_chunk(obj_url=obj_url, offset=offset, size=size)
    if ok is False:
        ok, result = download_one_chunk(obj_url=obj_url, offset=offset, size=size)

    return ok, result

def download_obj(obj_url, filename, start=0):
    '''
    下载一个对象

    :param obj_url: 对象url
    :param filename: 对象保存的绝对路径文件名
    :param start: 开始下载的偏移量
    :return:
        (ok, offset, msg)
        ok: True or False, 指示下载是否成功
        offset: 已下载对象的偏移量
        msg: 操作结果描述字符串
    '''
    offset = start
    chunk_size = 5*1024*1024

    # 目录路径不存在存在则创建
    dir_path = os.path.dirname(filename)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    with open(filename, 'wb') as f:
        while True:
            ok, result = _download_chunk(obj_url=obj_url, offset=offset, size=chunk_size)
            if ok is None: # 文件不存在
                return (False, 0, result)
            elif not ok:
                return (False, offset, 'downloading interrupt')

            chunk = result.get('chunk', None)
            obj_size = result.get('obj_size', 0)

            f.seek(offset)
            f.write(chunk)

            offset += len(chunk)
            if offset >= obj_size: # 下载完成
                return  (True, offset, 'download ok')


def create_dir(dir_url):
    '''
    创建一个目录

    :param dir_url: 目录url
    :return: (ok, code, msg)
        ok: True or False, 指示请求是否成功
        code: 请求返回的状态码
        msg: 请求结果描述字符串

    '''
    try:
        r = request.post(dir_url)
    except request.ConnectionError as e:
        return (False, 0, str(e))

    msg = get_response_msg(r)
    if r.status_code == 201:
        return (True, 201, msg)
    elif r.status_code == 400:
        data = r.json()
        if data.get('existing', '') is True:
            return (True, 400, msg)

    return (False, r.status_code, msg)

def create_one_dir(bucket_name, dir_path='', dir_name=''):
    '''
    创建一个目录

    :param bucket_name: 桶名
    :param dir_path: 父目录路径
    :param dir_name:  目录名
    :return: (ok, code, msg)
        ok: True or False, 指示请求是否成功
        code: 请求返回的状态码
        msg: 请求结果描述字符串
    '''
    api_dir_base = configs.DIR_API_URL_BASE
    if dir_path:
        bucket_dir_name = join_url_with_slash(bucket_name, dir_path, dir_name)
    else:
        bucket_dir_name = join_url_with_slash(bucket_name, dir_name)

    dir_url = join_url_with_slash(api_dir_base, bucket_dir_name) + '/'

    return create_dir(dir_url)


def get_path_breadcrumb(path=None, base_dir=''):
        '''
        路径面包屑
        :param base_dir: 基目录路径
        :return: list([dir_name，parent_dir_path])
        '''
        breadcrumb = []
        if not isinstance(path, str):
            raise ValueError('path must be a string.')
        _path = path
        if _path == '':
            return breadcrumb

        base = [base_dir] if base_dir else []
        _path = _path.strip('/')
        dirs = _path.split('/')
        for i, key in enumerate(dirs):
            breadcrumb.append([key, '/'.join(base + dirs[0:i])])
        return breadcrumb

def create_path(bucket_name=None, dir_path='', base_dir=''):
    '''
    创建目录路径
    :param bucket_name: 目录所在的存储桶名称
    :param base_dir: 要创建的路径dir_path的基路经
    :param dir_path: 目录路径
    :return:
        success: True
        failure: False
    '''
    if dir_path == '':
        return True

    bucket_name = bucket_name
    dirs = get_path_breadcrumb(dir_path, base_dir=base_dir)

    # 尝试从头创建整个路径
    for dir_name, p_dir_path in dirs:
        ok, *_ = create_one_dir(bucket_name=bucket_name, dir_path=p_dir_path, dir_name=dir_name)
        if not ok:
            # 再次尝试
            ok, *_ = create_one_dir(bucket_name=bucket_name, dir_path=p_dir_path, dir_name=dir_name)
            if not ok:
                return False

    return True


# class Directory():
#     def __init__(self, bucket_name=None, dir_api_base_url=None):
#         '''
#         :param bucket_name: 目录操作对应的存储桶名称
#         :param dir_api_base_url: 目录API的基url
#         '''
#         self._bucket_name = bucket_name or EVHARBOR_STORAGE_BUCKET_NAME
#         self._dir_api_base = dir_api_base_url or API_V1_DIR_BASE_URL
#
#     def create(self, dir_name, bucket_name=None, dir_path=''):
#         '''
#         创建一个目录
#         :param bucket_name: 目录所在的存储桶名称
#         :param dir_path: 目录的父目录节点路径
#         :param dir_name: 要创建的目录
#         :return:
#             success: (True, code, msg)
#             failure: (False, code, msg)
#         '''
#         bucket_name = bucket_name or self._bucket_name
#
#         p = [bucket_name, dir_path, dir_name]
#         if dir_path == '':
#             p.pop(1)
#         dir_path_name = '/'.join(p) + '/'
#         dir_path_name = quote(dir_path_name)
#         dir_url = urljoin(API_V1_DIR_BASE_URL, dir_path_name)
#
#         return create_dir(dirurl)
#
#
#     def get_objs(self, response_json):
#         objs_and_subdirs = response_json.get('files', [])
#
#         if not isinstance(objs_and_subdirs, list):
#             return None
#         return [o for o in objs_and_subdirs if o.get('fod')]
#
#     def get_objs_path_list(self, response_json):
#         objs_and_subdirs = response_json.get('files', [])
#         path = response_json.get('dir_path')
#
#         if not isinstance(objs_and_subdirs, list):
#             return None
#
#         return [(o.get('name'), '/'.join([path, o.get('name')]).lstrip('/')) for o in objs_and_subdirs if o.get('fod')]
#
#     def get_objs_and_subdirs(self, bucket_name=None, dir_path='', params={}):
#         '''
#         获取目录下的对象和子目录
#         :param bucket_name: 存储桶名称
#         :param dir_path: 目录路径
#         :param params: url query参数
#         :return:
#         '''
#         dir_path_name = bucket_name or self._bucket_name
#         if dir_path:
#             dir_path_name += '/' + dir_path
#
#         dir_path_name = quote(dir_path_name) + '/'
#
#         query = urlencode(query=params)
#         if query:
#             dir_path_name += '?' + query
#
#         dir_url = urljoin(API_V1_DIR_BASE_URL, dir_path_name)
#         return self.get_objs_and_subdirs_by_url(dir_url)
#
#     def get_objs_and_subdirs_by_url(self, dir_url):
#         '''
#         获取目录下的对象和子目录
#         :param dir_url: 目录url
#         :return:
#         '''
#         r = requests.get(dir_url, headers={'Authorization': 'Token ' + EVHARBOR_AUTH_TOKEN})
#         if r.status_code == 200:
#             return r.json()
#
#         return None




