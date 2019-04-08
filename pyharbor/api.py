from .core import ApiCore


class Directory():
    def __init__(self, bucket_name, cur_dir_path):
        '''
        :param bucket_name: 目录操作对应的存储桶名称
        :param cur_dir_path: 当前目录绝对路径
        '''
        self._bucket_name = bucket_name
        self._cur_dir_path = cur_dir_path.rstrip('/')
        self._paginater = None
        self.apicore = ApiCore()

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def cur_dir_path(self):
        return self._cur_dir_path

    @cur_dir_path.setter
    def cur_dir_path(self, value):
        self._cur_dir_path = value

    def __str__(self):
        return 'Directory(bucket={0}, path={1})'.format(self._bucket_name, self.cur_dir_path)

    def create_dir(self, dir_name):
        '''
        创建一个目录

        :param dir_name: 要创建的目录名
        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        bucket_name = self.bucket_name
        cur_dir_path = self.cur_dir_path

        ok, code, msg = self.apicore.create_dir(bucket_name=bucket_name, base_dir=cur_dir_path, dir_name=dir_name)
        return ok, msg

    def delete_dir(self, dir_name):
        '''
        删除当前目录下的一个目录
        :param dir_name: 要删除的目录名
        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        bucket_name = self.bucket_name
        cur_dir_path = self.cur_dir_path

        ok, code, msg = self.apicore.delete_dir(bucket_name=bucket_name, base_dir=cur_dir_path, dir_name=dir_name)
        return ok, msg

    def delete(self):
        '''
        删除当前目录

        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        bucket_name = self.bucket_name
        cur_dir_path = self.cur_dir_path
        if cur_dir_path == '':
            return False, '无法删除，当前为存储桶下根目录.'

        ok, code, msg = self.apicore.delete_dir(bucket_name=bucket_name, base_dir=cur_dir_path)
        return ok, msg

    def get_objs_and_subdirs(self, offset=None, limit=None):
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
        return self.apicore.get_objs_and_subdirs(bucket_name=self.bucket_name, dir_name=self.cur_dir_path,
                                                 offset=offset, limit=limit)

    def get_objs(self, response_data):
        '''
        从请求响应数据中获取所有对象信息列表

        :param response_data: 请求响应数据
        :return:
            对象信息列表
        '''
        objs_and_subdirs = response_data.get('files', [])

        if not isinstance(objs_and_subdirs, list):
            return None
        return [o for o in objs_and_subdirs if o.get('fod')]

    def get_objs_path_list(self, response_data):
        '''
        从请求响应数据中获取所有对象的绝对路径列表

        :param response_data: 请求响应数据
        :return:
            对象的绝对路径列表
        '''
        objs_and_subdirs = response_data.get('files', [])
        path = response_data.get('dir_path')

        if not isinstance(objs_and_subdirs, list):
            return None

        return [(o.get('name'), '/'.join([path, o.get('name')]).lstrip('/')) for o in objs_and_subdirs if o.get('fod')]

    def _put_obj(self, bucket_name, path, obj_name, filename, offset=0):
        '''
        上传一个对象

        :param obj_name: 对象名称
        :param filename: 上传的文件绝对路径
        :param offset: 文件上传的起始偏移量
        :return:
            (ok, offset, msg)
            ok: True or False, 指示上传是否成功
            offset: 已上传文件的偏移量
            msg: 上传结果描述字符串
        '''
        # size = os.path.getsize(filename)
        mark_offset = offset
        i = 0
        while True:
            ok, offset, msg = self.apicore.upload_obj(bucket_name=bucket_name, path=path,
                                                      obj_name=obj_name, filename=filename, start=offset)
            # 上传成功
            if ok:
                return True, offset, msg
            # 上传失败多次，或一点也未上传成功（可能参数有误，或网络问题等不具备上传条件），放弃上传，上传失败
            elif i > 5 or offset == 0:
                return False, offset, msg
            # 上传部分，继续尝试上传
            else:
                if mark_offset == offset:  # 同一偏移量处上传失败，次数++
                    i += 1
                else:
                    mark_offset = offset
                continue

    def put_object(self, obj_name, filename, offset=0):
        '''
        上传一个对象到当前目录

        :param obj_name: 对象的名称
        :param filename: 要上传的文件的绝对路径
        :param offset: 文件上传的起始偏移量
        :return:
            (ok, offset, msg)
            ok: True or False, 指示上传是否成功
            offset: 已上传文件的偏移量
            msg: 上传结果描述字符串
        '''
        if '/' in obj_name:
            return (False, 0, 'Object names cannot contain "/" characters.')

        return self._put_obj(bucket_name=self.bucket_name, path=self.cur_dir_path, obj_name=obj_name,
                             filename=filename, offset=offset)

    def _download_obj(self, bucket_nmae, path, obj_name, filename, offset=0):
        '''
        下载一个对象

        :param bucket_name: 存储桶名称
        :param dir_path: 目录路径
        :param obj_name: 对象名称
        :param filename: 下载的文件保存的绝对路径文件名
        :param offset: 对象下载的起始偏移量
        :return:
            (ok, offset, msg)
            ok: True or False, 指示下载是否成功
            offset: 已下载对象的偏移量
            msg: 操作结果描述字符串
        '''
        mark_offset = offset
        i = 0
        while True:
            ok, offset, msg = self.apicore.download_obj(bucket_name=bucket_nmae, path=path, obj_name=obj_name,
                                                        filename=filename, start=offset)
            i += 1
            # 下载成功
            if ok:
                return (True, offset, msg)
            # 下载失败多次，或一点也未上传成功（可能参数有误，或网络问题等不具备下载条件），放弃下载，下载失败
            elif i > 5 or offset == 0:
                return (False, offset, msg)
            # 下载了部分，继续尝试下载
            else:
                if mark_offset == offset:  # 同一偏移量处下载失败，次数++
                    i += 1
                else:
                    mark_offset = offset
                continue

    def download_object(self, obj_name, filename, offset=0):
        '''
        下载一个对象

        :param obj_name:  对象名称
        :param filename:  对象要保存的文件名绝对路径
        :param offset: 对象下载的起始偏移量
        :return:
            (ok, offset, msg)
            ok: True or False, 指示下载是否成功
            offset: 已下载对象的偏移量
            msg: 操作结果描述字符串
        '''
        if '/' in obj_name:
            return (False, 0, 'Object names cannot contain "/" characters.')

        return self._download_obj(bucket_nmae=self.bucket_name, path=self.cur_dir_path, obj_name=obj_name,
                                  filename=filename, offset=offset)

    def delete_object(self, obj_name, is_sub=False):
        '''
        删除当前目录下的一个对象

        :param obj_name:  对象名
        :param is_sub: 是否是子目录下的对象
        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        if not is_sub:
            if '/' in obj_name:
                return (False, 0, 'Object names cannot contain "/" characters.')

        ok, code, msg = self.apicore.delete_obj(bucket_name=self.bucket_name, dir_path=self.cur_dir_path, obj_name=obj_name)
        return ok, msg

    def share_object(self, obj_name, share=True, days=0):
        '''
        设置对象私有或公有访问权限

        :param obj_name: 对象名
        :param share: 是否分享，用于设置对象公有或私有, true(公有)，false(私有)
        :param days: 对象公开分享天数(share=true时有效)，0表示永久公开，负数表示不公开，默认为0
        :return:
            (ok, msg)
            ok: True or False, 指示请求是否成功
            msg: 请求结果描述字符串
        '''
        if '/' in obj_name:
            return (False, 0, 'Object names cannot contain "/" characters.')

        ok, code, msg = self.apicore.share_obj(bucket_name=self.bucket_name, dir_path=self.cur_dir_path,
                                               obj_name=obj_name, share=share, days=days)
        return ok, msg

    def get_obj_info(self, obj_name):
        '''
        获取一个对象的元数据

        :param obj_url: 对象url
        :return:
            (data, code, msg)
            data: 指示请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码或None
            msg: 结果描述字符串
        '''
        if '/' in obj_name:
            return (False, 0, 'Object names cannot contain "/" characters.')

        return self.apicore.get_obj_info(bucket_name=self.bucket_name, dir_path=self.cur_dir_path, obj_name=obj_name)

    def get_paginater(self, per_page=None):
        '''
        当前目录分页器

        :param per_page: 每页数据数量
        :return:
        '''
        if not self._paginater:
            self._paginater = ListDirPaginater(directory=self, per_page=per_page)
        return self._paginater

    def list(self, per_page=None):
        '''
        当前目录下的目录和对象列表的第一页

        :param per_page:  每页数据数量
        :return:
            success: ListDirPage()
            failed: None    网路问题或目录不存在等请求失败
        '''
        return self.get_paginater(per_page=per_page).first_page()

    def move_object(self, obj_name, to, rename=None):
        '''
        移动重命名对象

        :param obj_name: 要移动的对象名
        :param to:  对象移动目标目录路径， '/'和空字符串表示bucket桶下根目录
        :param rename:  重命名对象新名称， 默认不重命名
        :return:  (ok, data)
            ok: True or False, 指示请求是否成功
            data: dict{
                    code: xx,   # 请求返回的状态码
                    msg: xx,    # 请求结果描述字符串
                    obj: { ]    # 移动后的对象信息，此数据仅移动成功时存在
                }
        '''
        if '/' in obj_name or (rename and '/' in  rename):
            return False, '对象名称不能包含“/”字符'
        if rename and len(rename) > 255:
            return False, '对象名称长度不能大于255个字符'

        return self.apicore.move_obj(bucket_name=self.bucket_name, path=self.cur_dir_path, obj_name=obj_name,
                                              move_to=to, rename=rename)

    def rename_object(self, obj_name, rename):
        '''
        重命名对象

        :param obj_name: 要移动的对象名
        :param rename: 新对象名称
        :return: (ok, data)
            ok: True or False, 指示请求是否成功
            data: dict{
                    code: xx,   # 请求返回的状态码
                    msg: xx,    # 请求结果描述字符串
                    obj: { ]    # 移动后的对象信息，此数据仅移动成功时存在
                }
        '''
        if '/' in obj_name or (rename and '/' in  rename):
            return False, '对象名称不能包含“/”字符'
        if rename and len(rename) > 255:
            return False, '对象名称长度不能大于255个字符'

        return self.apicore.move_obj(bucket_name=self.bucket_name, path=self.cur_dir_path, obj_name=obj_name,
                                              rename=rename)


class Bucket():
    def __init__(self, bucket_name, *args, **kwargs):
        self._bucket_name = bucket_name
        self._id = None # bucket id

    @property
    def bucket_name(self):
        return self._bucket_name

    @bucket_name.setter
    def bucket_name(self, value):
        if isinstance(value, str) and (3 <= len(value) <= 64):
            self._bucket_name = value
        else:
            raise ValueError('bucket name must be a string with 3 to 64 characters.')

    def __str__(self):
        return 'Bucket({0})'.format(self._bucket_name)

    def dir(self, dir_path=''):
        '''
        返回一个目录类对象

        :param dir_path:  当前目录路径
        '''
        return Directory(bucket_name=self.bucket_name, cur_dir_path=dir_path)

    def get_bucket_id(self, bucket_name):
        '''
        通过名称获取bucket的id

        :param bucket_name: 桶名称
        :param buckets: self.get_buckets()获取的data
        :return:
            success: id
            failed: False
            error: None
        '''
        buckets, _, msg = ApiCore().get_buckets()
        if not buckets:
            return None

        bs = buckets.get('buckets')
        for b in bs:
            if b.get('name') == bucket_name:
                return b.get('id')

        return False

    @property
    def id(self):
        '''
        :return:
            success: bucket id
            failed: None
        '''
        if not self._id:
            bid = self.get_bucket_id(bucket_name=self.bucket_name)
            if bid:
                self._id = bid

        return self._id

    def set_permission(self, public=False):
        '''
        设置存储桶公有私有权限

        :param public: True(公有)，False(私有)
        :return: (ok, msg)
            ok: True or False, 指示请求是否成功
            msg: 请求结果描述字符串
        '''
        bucket_id = self.id
        ok, _, msg = ApiCore().bucket_permission(bucket_id=bucket_id, public=public)
        if not ok:
            return False, msg

        return True, msg


class BasePage():
    '''
    分页基类
    '''
    def __init__(self, data):
        self.apicore = ApiCore()
        self.__Initialize(data)

    @property
    def current_page_number(self):
        return self._current_page_number

    def __set_current_page_number(self, value):
        self._current_page_number = value

    def __Initialize(self, data):
        '''
        __Initialize()  can be implemented.
        根据data数据结构初始化一下类属性
        '''
        page = data.get('page')
        self.__set_current_page_number(1)
        self._pages = page.get('final')

        self._current_page = data.get('files')
        self._count = data.get('count')

        self._next_url = data.get('next')
        self._previous_url = data.get('previous')

    def has_next(self):
        '''是否有下一页'''
        if self._next_url:
            return True

        return False

    def has_previous(self):
        '''是否有上一页'''
        if self._previous_url:
            return True
        return False

    @property
    def next_page_number(self):
        '''是否有下一页'''
        if not self.has_next():
            return None

        return self.current_page_number + 1 if self.current_page_number else None

    @property
    def previous_page_number(self):
        '''是否有上一页'''
        if not self.has_previous():
            return None

        return self.current_page_number - 1 if self.current_page_number else None

    def get_list(self):
        return self._current_page

    def next_page(self):
        raise NotImplementedError('`next_page()` must be implemented.')

    def previous_page(self):
        raise NotImplementedError('`previous_page()` must be implemented.')


class ListDirPage(BasePage):
    '''
    目录下子目录和对象信息列表分页类
    '''
    def __Initialize(self, data):
        page = data.get('page')
        self.__set_current_page_number(page.get('current'))
        self._pages = page.get('final')

        self._current_page = data.get('files')
        self._count = data.get('count')

        self._next_url = data.get('next')
        self._previous_url = data.get('previous')

    def next_page(self):
        '''
        下一页

        :return:
            success: Page()
            failed: None     请求失败或者没有下一页
        '''
        if not self.has_next():
            return None

        data, code, msg = self.apicore.get_objs_and_subdirs_by_url(dir_url=self._next_url)
        if not data:
            return None

        return ListDirPage(data)

    def previous_page(self):
        '''
        上一页

        :return:
            success: Page()
            failed: None     请求失败或者没有上一页
        '''
        if not self.has_previous():
            return None

        data, code, msg = self.apicore.get_objs_and_subdirs_by_url(dir_url=self._previous_url)
        if not data:
            return None

        return ListDirPage(data)


class ListDirPaginater():
    def __init__(self, directory, per_page=None):
        '''
        :param directory: Directory class object
        :param per_page: number per page
        '''
        self.dir = directory
        self._per_page = per_page or 200
        self._page = None

    @property
    def dir(self):
        return self._dir

    @dir.setter
    def dir(self, value):
        if not isinstance(value, Directory):
            raise ValueError('value must be a Directory class object')

        self._dir = value
        self._page = None

    def first_page(self):
        '''
        目录下子目录和对象信息列表第一页
        :return:
            success: ListDirPage()
            failed: None    网路问题或目录不存在等请求失败
        '''
        if not self._page:
            data, code, msg = self.dir.get_objs_and_subdirs(self, limit=self._per_page)
            if not data:
                return None
            else:
                self._page = ListDirPage(data)

        return self._page


def get_path_and_name(p):
    '''
    分割一个路径

    :param p: 路径字符串
    :return:
        （path, basename）
    '''
    if '/' in p:
        path, name = p.rsplit('/', 1)
    else:
        path, name = '', p

    return path, name

class Client():
    def __init__(self):
        pass

    def bucket(self, bucket_name):
        '''
        返回一个Bucket类对象

        :param bucket_name:  存储桶名称
        '''
        return Bucket(bucket_name)

    def put_object(self, bucket_name, obj_name, filename):
        '''
        上传一个对象

        :param bucket_nmae:  存储桶名称
        :param obj_name:  对象全路径名称
        :param filename:  上传文件绝对路径
        :return:
            (ok, offset, msg)
            ok: True or False, 指示上传是否成功
            offset: 已上传文件的偏移量
            msg: 上传结果描述字符串
        '''
        path, name = get_path_and_name(obj_name)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).put_object(obj_name=name, filename=filename)

    def download_object(self, bucket_name, obj_name, filename):
        '''
        下载一个对象

        :param bucket_nmae:  存储桶名称
        :param obj_name:  对象全路径名称
        :param filename:  对象保存文件名绝对路径
        :return:
            (ok, offset, msg)
            ok: True or False, 指示下载是否成功
            offset: 已下载对象的偏移量
            msg: 下载结果描述字符串
        '''
        path, name = get_path_and_name(obj_name)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).download_object(obj_name=name, filename=filename)

    def delete_object(self, bucket_name, obj_name):
        '''
        删除一个对象

        :param bucket_nmae:  存储桶名称
        :param obj_name:  对象全路径名称
        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        path, name = get_path_and_name(obj_name)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).delete_object(obj_name=name)

    def share_object(self, bucket_name, obj_name, share=True, days=0):
        '''
        分享公开一个对象访问权限

        :param bucket_nmae: 存储桶名称
        :param obj_name:  对象全路径名称
        :param share: 是否分享，用于设置对象公有或私有, true(公有)，false(私有)
        :param days: 对象公开分享天数(share=true时有效)，0表示永久公开，负数表示不公开，默认为0
        :return:
            (ok, msg)
            ok: True or False, 指示请求是否成功
            msg: 请求结果描述字符串
        '''
        path, name = get_path_and_name(obj_name)
        ok, msg = Directory(bucket_name=bucket_name, cur_dir_path=path).share_object(obj_name=name, share=share, days=days)
        return ok, msg

    def get_obj_info(self, bucket_name, obj_name):
        '''
        获取一个对象的元数据

        :param bucket_name: 存储桶名称
        :param obj_name: 对象全路径名称
        :return:
            (data, code, msg)
            data: 指示请求成功时字典类型的数据，失败时为None
            code: 请求返回的状态码或None
            msg: 结果描述字符串
        '''
        path, name = get_path_and_name(obj_name)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).get_obj_info(obj_name=name)


    def create_dir(self, bucket_name, dir_name):
        '''
        创建一个文件夹

        :param bucket_name:  存储桶名称
        :param dir_name:  目录名全路径
        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        path, name = get_path_and_name(dir_name)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).create_dir(dir_name=name)

    def delete_dir(self, bucket_name, dir_name):
        '''
        删除一个文件夹

        :param bucket_name:  存储桶名称
        :param dir_name:  目录名全路径
        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        return Directory(bucket_name=bucket_name, cur_dir_path=dir_name).delete()

    def get_buckets(self):
        '''
        获取存储桶列表

        :return: (data, msg)
            data: 请求成功时字典类型的数据，失败时为None
            msg: 请求结果描述字符串
        '''
        data, _, msg = ApiCore().get_buckets()
        if not data:
            return None, msg

        return data, msg

    def bucket_permission(self, bucket_name, public=False):
        '''
        设置存储桶公有私有权限

        :param bucket_name: 存储桶名称
        :param public: True(公有)，False(私有)
        :return: (ok, msg)
            ok: True or False, 指示请求是否成功
            msg: 请求结果描述字符串
        '''
        return Bucket(bucket_name).set_permission(public=public)

    def list_dir(self, bucket_name, dir_name='', per_page=None):
        '''
        删除一个文件夹

        :param bucket_name:  存储桶名称
        :param dir_name:  目录名全路径
        :param per_page:  每页数据项数
        :return:
            success: ListDirPage()
            failed: None    网路问题或目录不存在等请求失败
        '''
        return Directory(bucket_name=bucket_name, cur_dir_path=dir_name).list(per_page=per_page)

    def write_one_chunk(self, bucket_name, obj_name, offset, chunk):
        '''
        上传一个分片

        :param bucket_name: 桶
        :param obj_name: 对象绝对路径
        :param offset: 分片偏移量
        :param chunk: 分片数据 bytes或者二进制方式打开的文件描述符, 数据不等大于20MB
        :return:
            success: (True,  msg)
            failure: (False, msg)
            可能参数有误，目录路径不存在等各种原因不具备上传条件: (None, msg)
        '''
        ok, code, msg = ApiCore().write_one_chunk(bucket_name=bucket_name, path=obj_name, obj_name='', offset=offset, chunk=chunk)
        return ok, msg

    def read_one_chunk(self, bucket_name, obj_name, offset, size):
        '''
        下载一个分片

        :param bucket_name: 桶
        :param obj_name: 对象绝对路径
        :param offset: 分片偏移量
        :param size: 要下载的分片大小
        :return:
            success: (True, {
                                'chunk': chunk, # bytes
                                'obj_size': xx  # 对象总大小
                            })
            failure: (False, msg)
            404: (None, msg) 资源不存在
        '''
        return ApiCore().read_one_chunk(bucket_name=bucket_name, path=obj_name, obj_name='', offset=offset, size=size)

    def move_object(self, bucket_name, obj_name, to, rename=None):
        '''
        移动重命名对象

        :param bucket_name: 对象所在bucket桶名
        :param obj_name: 要移动的对象名全路径
        :param to:  对象移动目标目录路径， '/'和空字符串表示bucket桶下根目录
        :param rename:  重命名对象新名称， 默认不重命名
        :return:  (ok, data)
            ok: True or False, 指示请求是否成功
            data: dict{
                    code: xx,   # 请求返回的状态码
                    msg: xx,    # 请求结果描述字符串
                    obj: { ]    # 移动后的对象信息，此数据仅移动成功时存在
                }
        '''
        path, name = get_path_and_name(obj_name)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).move_object(obj_name=name, to=to, rename=rename)

    def rename_object(self, bucket_name, obj_name, rename):
        '''
        重命名对象

        :param bucket_name: 对象所在bucket桶名
        :param obj_name: 要移动的对象名
        :param rename: 新对象名称
        :return:(ok, data)
            ok: True or False, 指示请求是否成功
            data: dict{
                    code: xx,   # 请求返回的状态码
                    msg: xx,    # 请求结果描述字符串
                    obj: { ]    # 移动后的对象信息，此数据仅移动成功时存在
                }
        '''
        path, name = get_path_and_name(obj_name)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).rename_object(obj_name=name, rename=rename)

    def isdir(self, bucket_name, dir_name):
        '''
        是否是目录
        :param bucket_name: 桶名
        :param dir_name: 全路径目录名
        :return:
            True: 是目录
            False: 对象或不存在此路径
        '''
        data, code, msg = ApiCore().get_metadata(bucket_name=bucket_name, path=dir_name)
        try:
            if data and data.get('data').get('fod') == False:
                return True
        except AttributeError:
            pass

        return False

    def isfile(self, bucket_name, filename):
        '''
        是否是对象
        :param bucket_name: 桶名
        :param filename: 全路径文件名
        :return:
            True: 是文件
            False: 目录或不存在此路径
        '''
        data, code, msg = ApiCore().get_metadata(bucket_name=bucket_name, path=filename)
        try:
            if data and data.get('data').get('fod'):
                return True
        except AttributeError:
            pass

        return False

