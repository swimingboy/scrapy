import queue
from datetime import datetime
from typing import List, Tuple
import requests
import io
from PIL import Image
from functools import reduce
from operator import mul, truediv
from assertpy import assert_that
from common.mysql_orm import ScrapStaffPhotos
from config import data_col

img_download_queue = queue.Queue()


def read_img_info_from_queue():
    while not img_download_queue.empty():
        url, path, filename, del_callback, _set_img_size = img_download_queue.get()
        print(datetime.now(), 'get: ', url, path)

        download_img(url, path, filename, del_callback, _set_img_size)


def download_img(img_url, img_path, filename, del_callback, set_img_size_callback):
    # DONE: 追踪异常的目录创建
    try:
        res = requests.get(img_url, timeout=10)

        if res.status_code == 200 and res.content:
            byte_stream = io.BytesIO(res.content)
            img: Image.Image = Image.open(byte_stream)
            img = img.convert('RGB')
            img_size = img.size
            set_img_size_callback(filename, img_size)
            # print(f'{img_size=}')
            if 0 and (reduce(mul, img_size) < 100 * 100 or truediv(*reversed(img_size)) > 5 or truediv(*img_size) > 5):
                # 目前先永远跳过这段
                del_callback(filename)
            elif img_size[0] <= 200 or img_size[1] <= 200:
                del_callback(filename)
            else:
                with open(f"{img_path}/{filename}", 'wb') as f:
                    img.save(f)
                    print('下载完成: ', f"{img_path}/{filename}")
        else:
            raise Exception(f'{res.status_code=}, {not res.content=}')
    except Exception as e:
        print("下载失败", img_url, img_path, e)
        # del_callback(filename)


def check_img_from_size(sizes: List[Tuple[int, int]]) -> Tuple[int, int]:
    '''
    输入一堆图片的size, 输出应该保留的size.
    未来再看吧
    :param sizes:
    :return: Tuple[int, int]
    '''
    return max(sizes, key=lambda size: sizes.count(size))

def test_check_img_from_size():
    assert_that(check_img_from_size(
        [(100, 100),
         (100, 100),
         (12, 100),
         (12, 100)]
    )).is_equal_to((100, 100))
    assert_that(check_img_from_size(
        [(100, 100),
         (12, 100)]
    )).is_equal_to((100, 100))
    assert_that(check_img_from_size(
        [(100, 100),
         (100, 100),
         (200, 100)]
    )).is_equal_to((100, 100))
    assert_that(check_img_from_size(
        [(100, 100),
         (100, 100),
         (100, 100),
         (200, 100),
         (200, 100)]
    )).is_equal_to((100, 100))
    assert_that(check_img_from_size(
        [(100, 100),
         (100, 100),
         (100, 100),
         (200, 100),
         (90, 100),
         (90, 100),
         (90, 100),
         (90, 100),
         (90, 100)]
    )).is_equal_to((90, 100))
    assert_that(check_img_from_size(
        [(100, 100),
         (100, 100),
         (100, 100),
         (200, 100),
         (50, 100),
         (50, 100),
         (50, 100),
         (50, 100),
         (50, 100)]
    )).is_equal_to((50, 100))

