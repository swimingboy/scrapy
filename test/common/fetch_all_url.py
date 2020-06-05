'''
链接提取器
相对绝对链接转换
'''
import requests
import json
import re
import time
from typing import List
try:
    import pytest
except:
    pass

# %%


def get_current_dir(url):
    # https://y-yasuragi.jp/comlist/ -> https://y-yasuragi.jp/comlist
    # https://y-yasuragi.jp/comlist -> https://y-yasuragi.jp
    # https://y-yasuragi.jp/comlist/asd/qwe/zxc -> https://y-yasuragi.jp/comlist/asd/qwe
    url, *args = url.split('?')
    url_split = url.split('/')

    ret = ''
    url_split_len = len(url_split)
    if url_split_len == 1 or url_split_len == 2:
        # 没有'/' 或 只有一个'/'
        ret = ''
    
    elif url.startswith('http') and url_split_len == 3:
        # 没有更前了
        ret = url
        if not ret.endswith('/'):
            ret += '/'

    elif url_split_len > 3:
        ret = "/".join(url_split[:-1]) + '/'
    else:
        ret = "/".join(url_split)
        if not ret.endswith('/'):
            ret += '/'
    return ret, args


def get_last_dir(url):
    # https://y-yasuragi.jp/comlist/ -> https://y-yasuragi.jp
    # https://y-yasuragi.jp/comlist -.> https://y-yasuragi.jp
    # https://y-yasuragi.jp/comlist/asd/qwe/zxc -> https://y-yasuragi.jp/comlist/asd/qwe
    url, *args = url.split('?')
    url_split = url.split('/')

    ret = ''
    if len(url_split) == 1:
        # 没有'/'
        ret = ''
    elif url.startswith('http') and len(url_split) <= 3:
        # 没有更前了
        ret = url
    else:
        ret = "/".join(url_split[:-2])
    return ret, args


def get_root_dir(url):
    if not url:
        return ""
    if url.startswith('//'):
        url = url[2:]
    if url.startswith('./') or url.startswith('/'):
        return ''
    if not url.startswith('http'):
        url = 'http://' + url
    return "/".join(url.split('/')[:3]) + '/'


def test_get_root_dir():
    test_cases = [
        ['https://baidu.com/a.html', 'https://baidu.com/'],
        ['https://baidu.com', 'https://baidu.com/'],
        ['https://baidu.com/', 'https://baidu.com/'],
        ['https://baidu.com/a.html///////', 'https://baidu.com/'],
        ['http://2.baidu.com/a.html///////', 'http://2.baidu.com/'],
        ['//2.baidu.com/a.html///////', 'http://2.baidu.com/'],
        ['./b.html', ''],
        ['/b.html', ''],
    ]
    for url, right_root in test_cases:
        result = get_root_dir(get_root_dir(url))
        print(url, right_root, result)
        assert right_root == result


# %%

def relative_to_absolute(url, parents_url, use_external_url=False):
    # 将可能的相对链接转为绝对链接
    root = get_root_dir(parents_url)

    # if not url.endswith('/'):
    #     url += '/'
    if url.startswith('//'):
        # 外部链接
        if use_external_url:
            return 'http://' + url[2:]
        else:
            return 'http://' + url[2:]
    elif url.startswith('http'):
        if use_external_url:
            return url
        else:
            if get_root_dir(url) == root:
                return url
            else:
                return url
    elif url.startswith('javascript') or url.startswith('/javascript'):
        # 可能有事件被忽略
        # T_ODO 加上警告
        return ""

    # 至此一定是相对链接

    while url.startswith('./'):
        # 相对本目录链接
        url = url[2:]
        return get_current_dir(parents_url)[0] + url

    current_dir, args = get_current_dir(parents_url)

    args = "?".join(args)
    flag = False
    while url.startswith('../'):
        # 相对本目录上级链接
        current_dir, _ = get_last_dir(current_dir)
        if current_dir in ['https:/', 'http:/']:
            # 本已经是根目录时, 获取上个目录姑且当做还是根目录吧. 
            current_dir, _ = get_current_dir(parents_url)
            # 去除后面的斜杠
            current_dir = current_dir[:-1]
        url = url[3:]
        flag = True

    if flag:
        url = current_dir + '/' + url
        if args:
            if '?' not in url:
                # 相对链接有?参数时, 不再使用父路径的参数
                url += "?" + args
        return url

    if url.startswith('/'):
        # 相对根目录链接
        return root[:-1] + url
    elif url.startswith('?'):
        # 相对本文件跳转链接

        return parents_url + url
    elif url.startswith('#'):
        # 相对本文件不跳转链接
        return parents_url
    else:
        # 相对本目录链接
        # print(get_current_dir(parents_url), url)
        url = get_current_dir(parents_url)[0] + url
        return url


def test_relative_to_absolute():
    test_cases = {
        'https://baidu.com/': [
            ['./a.html', 'https://baidu.com/a.html'],
            ['a.html', 'https://baidu.com/a.html'],
            ['/a.html', 'https://baidu.com/a.html'],
            ['./a.html?i=2', 'https://baidu.com/a.html?i=2'],
            ['./a.html?i=3&t=4', 'https://baidu.com/a.html?i=3&t=4'],
            ['https://img.baidu.com/asedf', 'https://img.baidu.com/asedf'],
            ['?a=2', 'https://baidu.com/?a=2'],
        ],
        'https://baidu.com/a/b.html': [
            ['./a.html', 'https://baidu.com/a/a.html'],
            ['a.html', 'https://baidu.com/a/a.html'],
            ['/a.html', 'https://baidu.com/a.html'],
            ['./a.html?i=2', 'https://baidu.com/a/a.html?i=2'],
            ['?a=2', 'https://baidu.com/a/b.html?a=2'],
            ['//www.qwe.com/t.jpg', 'http://www.qwe.com/t.jpg'],
            ['//cdn1.fu-kakumei.com/22/images/mikeiken.png',
                'http://cdn1.fu-kakumei.com/22/images/mikeiken.png'],
        ],
        'http://www.nukipara.net/girl/': [
            ['../profile/?id=171', 'http://www.nukipara.net/profile/?id=171']
        ],
        'http://www.nukipara.net/girl/a/b/c': [
            ['../../../profile/?id=171', 'http://www.nukipara.net/profile/?id=171']
        ],
        'http://www.s-orgel.net/pc/girls.php?shop=1': [
            ['./profile.php?id=123', 'http://www.s-orgel.net/pc/profile.php?id=123']
        ],
        'http://www.robo-deli.com/top.php': [
            ['/photo/20191129121051.jpg', 'http://www.robo-deli.com/photo/20191129121051.jpg']
        ],
        'https://kanamachi-gold.net/detail.html?id=rk7c0suE': [
            ['../girlimg/img1_rk7c0suE.jpg?1579788319', 'https://kanamachi-gold.net/girlimg/img1_rk7c0suE.jpg?1579788319']
        ]
    }
    for root_url in test_cases:
        print(root_url)
        for relative_path, right_path in test_cases[root_url]:
            result = relative_to_absolute(relative_path, root_url)
            print('\t', relative_path, right_path, result)
            assert right_path == result

# %%


def scan(start_url, parents_url, scanned, depth=1, show_url=False, max_depth=2):
    if depth >= max_depth:
        return

    try:
        if show_url:
            print('get:', start_url)
        ret = requests.get(start_url)
    except Exception as e:
        # T_ODO: record it
        print(e)
        return

    # if ret.status_code != 200:
    #     return

    urls: List[str] = re.findall(r'(?<=href=").+?(?=")', ret.text)
    urls.extend(re.findall(r'''(?<=window.open[(]').+?(?=['])''', ret.text))
    for url in urls:
        url = relative_to_absolute(url, start_url)
        if url in scanned:
            continue
        if not url:
            continue
        if show_url:
            print(url)
        scanned.add(url)
        for end in ['css', 'js', 'png', 'jpg']:
            if url.endswith(end):
                break
        scan(url, start_url, scanned, depth + 1, max_depth=max_depth)
        # time.sleep(0.1)


if __name__ == "__main__":
    import sys
    eval('pytest.main(sys.argv)')
