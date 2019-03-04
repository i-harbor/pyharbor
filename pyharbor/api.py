from .import core
from .config import join_url_with_slash, configs


def put_file(obj_url, filename):
    '''
    上传一个文件

    :param obj_url:
    :param filename:
    :return:
        (ok, offset, msg)
        ok: True or False, 指示上传是否成功
        offset: 已上传文件的偏移量
        msg: 上传结果描述字符串
    '''
    # size = os.path.getsize(filename)
    offset = 0
    mark_offset = offset
    i = 0
    while True:
        ok, offset, msg = core.upload_file(obj_url=obj_url, filename=filename, start=offset)
        # 上传成功
        if ok:
            return True, offset, msg
        # 上传失败多次，或一点也未上传成功（可能参数有误，或网络问题等不具备上传条件），放弃上传，上传失败
        elif i > 5 or offset == 0:
            return False, offset, msg
        # 上传部分，继续尝试上传
        else:
            if mark_offset == offset: # 同一偏移量处上传失败，次数++
                i += 1
            else:
                mark_offset = offset
            continue

def download_file(obj_url, filename):
    '''
    下载一个文件

    :param obj_url: 对象url
    :param filename: 下载的文件保存的绝对路径文件名
    :return:
        (ok, offset, msg)
        ok: True or False, 指示下载是否成功
        offset: 已下载对象的偏移量
        msg: 操作结果描述字符串
    '''
    offset = 0
    i = 0
    while True:
        ok, offset, msg = core.download_obj(obj_url=obj_url, filename=filename, start=offset)
        i += 1
        # 下载成功
        if ok:
            return (True, offset, msg)
        # 下载失败多次，或一点也未上传成功（可能参数有误，或网络问题等不具备下载条件），放弃下载，下载失败
        elif i > 10 or offset == 0:
            return (False, offset, msg)
        # 下载了部分，继续尝试下载
        else:
            continue


class Directory():
    def __init__(self, bucket_name, cur_dir_path):
        '''
        :param bucket_name: 目录操作对应的存储桶名称
        :param cur_dir_path: 当前目录绝对路径
        '''
        self._bucket_name = bucket_name
        self._cur_dir_path = cur_dir_path.rstrip('/')
        self._dir_api_url_base = configs.DIR_API_URL_BASE
        self._obj_api_url_base = configs.OBJ_API_URL_BASE

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def cur_dir_path(self):
        return self._cur_dir_path

    @cur_dir_path.setter
    def cur_dir_path(self, value):
        self._cur_dir_path = value

    def get_cur_dir_url(self):
        '''
        获取当前目录的url
        :return:
        '''
        return join_url_with_slash(self._dir_api_url_base, self.bucket_name, self.cur_dir_path) + '/'

    def get_cur_dir_obj_url(self):
        '''
        获取当前目录对应的对象api的url
        :return:
        '''
        return join_url_with_slash(self._obj_api_url_base, self.bucket_name, self.cur_dir_path) + '/'

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

        ok, code, msg = core.create_dir(bucket_name=bucket_name, base_dir=cur_dir_path, dir_name=dir_name)
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

        ok, code, msg = core.delete_dir(bucket_name=bucket_name, base_dir=cur_dir_path, dir_name=dir_name)
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

        ok, code, msg = core.delete_dir(bucket_name=bucket_name, base_dir=cur_dir_path)
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
        dir_url = self.get_cur_dir_url()
        return core.get_objs_and_subdirs_by_url(dir_url=dir_url, offset=offset, limit=limit)

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

    def put_object(self, obj_name, filename):
        '''
        上传一个对象到当前目录

        :param obj_name: 对象的名称
        :param filename: 要上传的文件的绝对路径
        :return:
            (ok, offset, msg)
            ok: True or False, 指示上传是否成功
            offset: 已上传文件的偏移量
            msg: 上传结果描述字符串
        '''
        if '/' in obj_name:
            return (False, 0, 'Object names cannot contain "/" characters.')

        dir_url = self.get_cur_dir_obj_url()
        obj_url = dir_url + obj_name + '/'
        return put_file(obj_url=obj_url, filename=filename)

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

        dir_url = self.get_cur_dir_obj_url()
        obj_url = dir_url + obj_name + '/'

        ok, code, msg = core.delete_obj(obj_url=obj_url)
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

        dir_url = self.get_cur_dir_obj_url()
        obj_url = dir_url + obj_name + '/'
        ok, code, msg = core.share_obj(obj_url=obj_url, share=share, days=days)
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

        dir_url = self.get_cur_dir_obj_url()
        obj_url = dir_url + obj_name + '/'
        return core.get_obj_info(obj_url=obj_url)


class Bucket():
    def __init__(self, bucket_name, *args, **kwargs):
        self._bucket_name = bucket_name

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
        path, name = obj_name.rsplit('/', 1)
        return Directory(bucket_name=bucket_name, cur_dir_path=path).put_object(obj_name=name, filename=filename)

    def delete_object(self, bucket_name, obj_name):
        '''
        删除一个对象

        :param bucket_nmae:  存储桶名称
        :param obj_name:  对象全路径名称
        :return:
            success: (True,  msg)
            failure: (False, msg)
        '''
        path, name = obj_name.rsplit('/', 1)
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
        path, name = obj_name.rsplit('/', 1)
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
        path, name = obj_name.rsplit('/', 1)
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
        path, name = dir_name.rsplit('/', 1)
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

    def list_buckets(self):
        pass

    def create_bucket(self, name):
        pass

    def delete_bucket(self, name):
        pass


