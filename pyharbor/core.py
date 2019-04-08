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
    except ValueError:
        return response.text

    if response.ok:
        if key in data:
            msg = data[key]
        elif 'code_text' in data:
            msg = data['code_text']
        else:
            msg = 'successfull'

    elif 'detail' in data:
        msg = data['detail']
    else:
        msg = response.text

    return msg if msg else ''

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

class ApiUrlBuilder():
    '''
    API url构建类
    '''
    def __init__(self):
        self._DIR_API_URL_BASE = configs.DIR_API_URL_BASE
        self._OBJ_API_URL_BASE = configs.OBJ_API_URL_BASE
        self._BUCKET_API_URL_BASE = configs.BUCKET_API_URL_BASE
        self._MOVE_API_URL_BASE = configs.MOVE_API_URL_BASE
        self._METADATA_API_URL_BASE = configs.METADATA_API_URL_BASE

    def build_obj_url(self, bucket_name, path, obj_name):
        '''
        构建对象url

        :param bucket_name: 桶名
        :param path: 父目录路径
        :param obj_name:  对象名
        '''
        return join_url_with_slash(self._OBJ_API_URL_BASE, bucket_name, path, obj_name) + '/'

    def build_dir_url(self, bucket_name, path='', dir_name=''):
        '''
        构建目录url

        :param bucket_name: 桶名
        :param path: 父目录路径
        :param dir_name:  目录名
        '''
        return join_url_with_slash(self._DIR_API_URL_BASE, bucket_name, path, dir_name) + '/'

    def get_bucket_api_base_url(self):
        return self._BUCKET_API_URL_BASE if self._BUCKET_API_URL_BASE.endswith('/') else self._BUCKET_API_URL_BASE + '/'

    def build_bucket_url(self, bucket_id=None):
        '''
        构建桶url

        :param bucket_id: 桶ID
        :return:
        '''
        base_url = self.get_bucket_api_base_url()
        if not bucket_id:
            return base_url

        if not isinstance(bucket_id, str):
            bucket_id = str(bucket_id)

        return join_url_with_slash(base_url, bucket_id) + '/'

    def build_move_url(self, bucket_name, path, obj_name):
        '''
        构建对象移动重命名url

        :param bucket_name: 桶名
        :param path: 父目录路径
        :param obj_name:  对象名
        '''
        return join_url_with_slash(self._MOVE_API_URL_BASE, bucket_name, path, obj_name) + '/'

    def build_metadata_url(self, bucket_name, path):
        '''
        构建元数据url

        :param bucket_name: 桶名
        :param path: 对象或目录路径
        '''
        return join_url_with_slash(self._METADATA_API_URL_BASE, bucket_name, path) + '/'


class ApiCore():
    '''
    EVHarbor API 封装
    '''
    def __init__(self):
        self._url_builder = ApiUrlBuilder()

    def upload_one_chunk(self, obj_url, offset, chunk, **kwargs):
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

    def write_one_chunk(self, bucket_name, path, obj_name, offset, chunk, **kwargs):
        '''
        上传一个分片

        :param bucket_name: 桶
        :param path:  对象所在目录路径
        :param obj_name: 对象名称
        :param offset: 分片偏移量
        :param chunk: 分片
        :return:
            success: (True, code, msg)
            failure: (False, code, msg)
            可能参数有误，目录路径不存在等各种原因不具备上传条件: (None, 0, msg)
        '''
        obj_url = self._url_builder.build_obj_url(bucket_name=bucket_name, path=path, obj_name=obj_name)
        return self.upload_one_chunk(obj_url=obj_url, offset=offset, chunk=chunk, **kwargs)

    def upload_obj_by_url(self, obj_url, filename, start=0):
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

                ok, code, msg = self.upload_one_chunk(obj_url=obj_url, offset=offset, chunk=chunk)
                if not ok:
                    return False, offset, 'upload failed:' + msg

                offset += len(chunk)

            return True, offset, 'upload successfull'

    def upload_obj(self, bucket_name, path, obj_name, filename, start=0):
        '''
        上传一个文件

        :param bucket_name: 存储桶名称
        :param path: 目录路径
        :param obj_name: 对象名称
        :param filename: 要上传文件的绝对路径
        :param start: 开始上传的偏移量
        :return:
            (ok, offset, msg)
            ok: True or False, 指示上传是否成功
            offset: 已上传文件的偏移量
            msg: 上传结果描述字符串
        '''
        obj_url = self._url_builder.build_obj_url(bucket_name=bucket_name, path=path, obj_name=obj_name)
        return self.upload_obj_by_url(obj_url=obj_url, filename=filename, start=start)

    def read_one_chunk(self, bucket_name, path, obj_name, offset, size):
        '''
        下载一个分片

        :param bucket_name: 桶
        :param path:  对象所在目录路径
        :param obj_name: 对象名称
        :param offset: 分片偏移量
        :param size: 要下载的分片大小
        :return:
            success: (True, {'chunk': chunk, 'obj_size': xx})
            failure: (False, msg)
            404: (None, msg) 资源不存在
        '''
        obj_url = self._url_builder.build_obj_url(bucket_name=bucket_name, path=path, obj_name=obj_name)
        return self.download_one_chunk(obj_url=obj_url, offset=offset, size=size)

    def download_one_chunk(self, obj_url, offset, size):
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

    def _download_chunk(self, obj_url, offset, size):
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
        ok, result = self.download_one_chunk(obj_url=obj_url, offset=offset, size=size)
        if ok is False:
            ok, result = self.download_one_chunk(obj_url=obj_url, offset=offset, size=size)

        return ok, result

    def download_obj_by_url(self, obj_url, filename, start=0):
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
                ok, result = self._download_chunk(obj_url=obj_url, offset=offset, size=chunk_size)
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

    def download_obj(self, bucket_name, path, obj_name, filename, start=0):
        '''
        下载一个对象

        :param bucket_name: 存储桶名称
        :param dir_path: 目录路径
        :param obj_name: 对象名称
        :param filename: 对象保存的绝对路径文件名
        :param start: 开始下载的偏移量
        :return:
            (ok, offset, msg)
            ok: True or False, 指示下载是否成功
            offset: 已下载对象的偏移量
            msg: 操作结果描述字符串
        '''
        obj_url = self._url_builder.build_obj_url(bucket_name=bucket_name, path=path, obj_name=obj_name)
        return self.download_obj_by_url(obj_url=obj_url, filename=filename, start=start)

    def delete_obj_by_url(self, obj_url):
        '''
        删除一个对象

        :param obj_url: 对象url
        :return:
            (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        try:
            r = request.delete(url=obj_url)
        except Exception as e:
            return (False, None, str(e))

        if r.status_code == 204:
            return (True, 204, 'delete successful')

        msg = get_response_msg(r)
        return (False, r.status_code, msg)

    def delete_obj(self, bucket_name, dir_path, obj_name):
        '''
        删除一个对象

        :param bucket_name: 存储桶名称
        :param dir_path: 目录路径
        :param obj_name: 对象名称
        :return:
            (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        obj_url = self._url_builder.build_obj_url(bucket_name=bucket_name, path=dir_path, obj_name=obj_name)
        return self.delete_obj_by_url(obj_url=obj_url)

    def get_obj_info_by_url(self, obj_url):
        '''
        获取一个对象的元数据

        :param obj_url: 对象url
        :return:
            (data, code, msg)
            data: 指示请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码或None
            msg: 结果描述字符串
        '''
        try:
            r = request.get(url=obj_url, params={'info': True})
        except Exception as e:
            return (None, None, str(e))

        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError as e:
                return (None, None, '获取无效的json数据：' + str(e))

            return (data, 200, "Get object's metedata successful.")

        msg = get_response_msg(r)
        return (False, r.status_code, msg)

    def get_obj_info(self, bucket_name, dir_path, obj_name):
        '''
        获取一个对象的元数据

        :param bucket_name: 存储桶名称
        :param dir_path: 目录路径
        :param obj_name: 对象名称
        :return:
            (data, code, msg)
            data: 指示请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码或None
            msg: 结果描述字符串
        '''
        obj_url = self._url_builder.build_obj_url(bucket_name=bucket_name, path=dir_path, obj_name=obj_name)
        return self.get_obj_info_by_url(obj_url=obj_url)

    def get_metadata(self, bucket_name, path):
        '''
        获取元数据
        :param bucket_name: 桶名
        :param path: 对象或目录路径
        :return:  (data, code, msg)
            data: 指示请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码或None
            msg: 结果描述字符串
        '''
        url = self._url_builder.build_metadata_url(bucket_name=bucket_name, path=path)
        try:
            r = request.get(url=url)
        except Exception as e:
            return (None, None, str(e))

        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError as e:
                return (None, None, '获取无效的json数据：' + str(e))

            return (data, 200, "Get metedata successful.")

        msg = get_response_msg(r)
        return (False, r.status_code, msg)

    def share_obj_by_url(self, obj_url, share=True, days=0):
        '''
        设置对象私有或公有访问权限

        :param obj_url: 对象url
        :param share: 是否分享，用于设置对象公有或私有, true(公有)，false(私有)
        :param days: 对象公开分享天数(share=true时有效)，0表示永久公开，负数表示不公开，默认为0
        :return:
            (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        try:
            r = request.patch(url=obj_url, params={'share': share, 'days': days})
        except Exception as e:
            return (False, None, str(e))

        if r.status_code == 200:
            return (True, 200, 'Set object permission successful.')

        msg = get_response_msg(r)
        return (False, r.status_code, msg)

    def share_obj(self, bucket_name, dir_path, obj_name, share=True, days=0):
        '''
        设置对象私有或公有访问权限

        :param bucket_name: 存储桶名称
        :param dir_path: 目录路径
        :param obj_name: 对象名称
        :param share: 是否分享，用于设置对象公有或私有, true(公有)，false(私有)
        :param days: 对象公开分享天数(share=true时有效)，0表示永久公开，负数表示不公开，默认为0
        :return:
            (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        obj_url = self._url_builder.build_obj_url(bucket_name=bucket_name, path=dir_path, obj_name=obj_name)
        return self.share_obj_by_url(obj_url=obj_url, share=share, days=days)

    def create_dir_by_url(self, dir_url):
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

    def create_dir(self, bucket_name, base_dir='', dir_name=''):
        '''
        创建一个目录

        :param bucket_name: 桶名
        :param base_dir: 父目录路径
        :param dir_name:  目录名
        :return: (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        if '/' in dir_name:
            return (False, None, '目录名不能包含“/”')

        dir_url = self._url_builder.build_dir_url(bucket_name=bucket_name, path=base_dir, dir_name=dir_name)
        return self.create_dir_by_url(dir_url)

    def create_path(self, bucket_name=None, base_dir='', dir_path=''):
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
            ok, *_ = self.create_dir(bucket_name=bucket_name, base_dir=p_dir_path, dir_name=dir_name)
            if not ok:
                # 再次尝试
                ok, *_ = self.create_dir(bucket_name=bucket_name, base_dir=p_dir_path, dir_name=dir_name)
                if not ok:
                    return False

        return True

    def get_objs_and_subdirs_by_url(self, dir_url, limit=None, offset=None):
        '''
        获取目录下的对象和子目录

        :param dir_url: 目录url
        :param limit: 获取目标 数量限制
        :param offset: 获取目标 起始偏移量
        :return:
            (data, code, msg)
            data: 指示请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码或None
            msg: 结果描述字符串
        '''
        params = {}
        if limit:
            params['limit'] = limit

        if offset:
            params['offset'] = offset

        try:
            r = request.get(dir_url, params=params)
        except Exception as e:
            return (None, None, str(e))

        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError as e:
                return (None, None, '获取无效的json数据：' + str(e))

            return (data, 200, "Get data successful.")

        msg = get_response_msg(r)
        return (False, r.status_code, msg)

    def get_objs_and_subdirs(self, bucket_name, dir_name, limit=None, offset=None):
        '''
        获取目录下的对象和子目录

        :param bucket_name: 存储桶名称
        :param dir_name: 目录绝对路径
        :param limit: 获取目标 数量限制
        :param offset: 获取目标 起始偏移量
        :return:
            (data, code, msg)
            data: 请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码或None
            msg: 结果描述字符串
        '''
        dir_url = self._url_builder.build_dir_url(bucket_name=bucket_name, path=dir_name)
        return self.get_objs_and_subdirs_by_url(dir_url=dir_url, limit=limit, offset=offset)

    def delete_dir_by_url(self, dir_url):
        '''
        删除一个目录

        :param dir_url: 目录url
        :return: (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        try:
            r = request.delete(dir_url)
        except request.ConnectionError as e:
            return (False, 0, str(e))

        msg = get_response_msg(r)
        if r.status_code == 204:
            return (True, 204, msg)

        return (False, r.status_code, msg)

    def delete_dir(self, bucket_name, base_dir='', dir_name=''):
        '''
        创建一个目录

        :param bucket_name: 桶名
        :param base_dir: 父目录路径
        :param dir_name:  目录名
        :return: (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        if '/' in dir_name:
            return (False, None, '目录名不能包含“/”')

        dir_url = self._url_builder.build_dir_url(bucket_name=bucket_name, path=base_dir, dir_name=dir_name)
        return self.delete_dir_by_url(dir_url)

    def create_bucket(self, bucket_name):
        '''
        创建一个存储桶

        :param bucket_name: 存储桶名称
        :return: (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        url = self._url_builder.build_bucket_url()
        try:
            r = request.post(url=url, data={'name': bucket_name})
        except request.RequestException as e:
            return (False, None, str(e))

        msg = get_response_msg(r)
        if r.status_code == 201:
            return (True, 201, msg)

        return (False, r.status_code, msg)

    def get_buckets(self):
        '''
        获取存储桶列表

        :return: (data, code, msg)
            data: 请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        url = self._url_builder.build_bucket_url()
        try:
            r = request.get(url=url)
        except request.RequestException as e:
            return (None, None, str(e))

        msg = get_response_msg(r)
        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError as e:
                return (None, None, '获取无效的json数据：' + str(e))

            return (data, 200, msg)

        return (None, r.status_code, msg)

    def bucket_permission(self, bucket_id, public=False):
        '''
        设置存储桶公有私有权限

        :param bucket_id: 存储桶id
        :param public: True(公有)，False(私有)
        :return: (ok, code, msg)
            ok: True or False, 指示请求是否成功
            code: 请求返回的状态码
            msg: 请求结果描述字符串
        '''
        url = self._url_builder.build_bucket_url(bucket_id=bucket_id)
        try:
            r = request.patch(url=url, params={'public': public})
        except request.RequestException as e:
            return (False, None, str(e))

        msg = get_response_msg(r)
        if r.status_code == 200:
            return (True, 200, msg)

        return (False, r.status_code, msg)

    def move_obj(self, bucket_name, path, obj_name, move_to=None, rename=None):
        '''
        移动或重命名一个对象

        :param bucket_name: 桶名称
        :param path:  对象所在父目录路径
        :param obj_name: 对象名称
        :param move_to: 移动对象到此目录路径，None为不移动
        :param rename: 重命名对象
        :return: (ok, data)
            ok: True or False, 指示请求是否成功
            data: dict{
                    code: xx,   # 请求返回的状态码
                    msg: xx,    # 请求结果描述字符串
                    obj: { ]    # 移动后的对象信息，此数据仅移动成功时存在
                }
        '''
        params = {}
        if move_to:
            params['move_to'] = move_to

        if rename:
            params['rename'] = rename

        url = self._url_builder.build_move_url(bucket_name=bucket_name, path=path, obj_name=obj_name)
        try:
            r = request.post(url=url, params=params)
        except request.RequestException as e:
            return False, {'code': None, 'msg': str(e)}

        msg = get_response_msg(r)
        if r.status_code == 201:
            try:
                data = r.json()
            except ValueError as e:
                return True, {'code': None, 'msg': '获取无效的json数据：' + str(e)}

            return True, {'code': 201, 'msg': msg, 'obj': data.get('obj')} ,

        return False, {'code': r.status_code, 'msg': msg}

