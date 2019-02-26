from .import core
from .core import create_path, create_one_dir, create_dir

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
    i = 0
    while True:
        ok, offset, msg = core.upload_file(obj_url=obj_url, filename=filename, start=offset)
        i += 1
        # 上传成功
        if ok:
            return True, offset, msg
        # 上传失败多次，或一点也未上传成功（可能参数有误，或网络问题等不具备上传条件），放弃上传，上传失败
        elif i > 10 or offset == 0: 
            return False, offset, msg
        # 上传部分，继续尝试上传
        else:
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




