'''
网页信息提取器
'''

from typing import List
from lxml import etree
from common.pretreatment import pretreatment_text
from common.fetch_all_url import relative_to_absolute
import pytest
import re


class Extracter:
    selector = None
    special_urls = ['depita.com', 'dolce-kawasaki.com']  # iso-8859-1编码转换url
    def __init__(self, html, url=""):
        self.selector = etree.HTML(html)
        self.url = url  # 为某些提取开后门

    def transform_text(self, text):
        # 用于iso-8859-1编码转换
        if any([i in self.url for i in Extracter.special_urls]):
            try:
                text = text.encode("iso-8859-1").decode('utf-8')
            except:
                pass
        return text

    def extract_text_by_xpath(self, xpath, replace_甲乙丙丁=False):
        try:
            elems: List = self.selector.xpath(xpath)
            if not elems and '/tbody' in xpath:
                xpath = xpath.replace('/tbody', '')
                elems: List = self.selector.xpath(xpath)
        except:
            return ''
        else:
            if len(elems) > 0:
                text: str = "".join(elems[0].itertext() if not isinstance(
                    elems[0], str) else elems[0])
                text = self.transform_text(text)
                text = pretreatment_text(text)
                if replace_甲乙丙丁:
                    for s in '甲乙丙丁戊己庚辛壬癸子丑寅卯辰巳午未申酉戌亥':
                        text = text.replace(s, ' ')
            else:
                text = ''

            return text

    def extract_html_by_xpath(self, xpath, url=None):
        try:
            elems: List = self.selector.xpath(xpath)

            if not elems and '/tbody' in xpath:
                xpath = xpath.replace('/tbody', '')
                elems: List = self.selector.xpath(xpath)
        except:
            print('ssrrr')
            return ''
        else:
            print('extract:54', elems)
            if len(elems) > 0:
                HTML: str = "".join(
                    [etree.tostring(t, encoding='utf-8').decode('utf-8') for t in elems])

                HTML = self.transform_text(HTML)
                if url:
                    for i in re.findall(r'(?<=href=").+?(?=")', HTML):
                        HTML = HTML.replace(i, relative_to_absolute(i, url))
                    for i in re.findall(r'(?<=src=").+?(?=")', HTML):
                        HTML = HTML.replace(i, relative_to_absolute(i, url))
            else:
                HTML = ''
            return HTML

    @staticmethod
    def extract_img_url(url_dict: dict):
        appear_urls = {}
        for page_url in url_dict:
            for img_url in url_dict[page_url]:
                appear_urls[img_url] = appear_urls.setdefault(img_url, 0) + 1

        appear_urls = {key: value for key,
                       value in appear_urls.items() if value <= 10}
        appear_urls_set = set(appear_urls.keys())

        for item_url in url_dict:
            urls = appear_urls_set & set(url_dict[item_url])
            urls = [relative_to_absolute(i, item_url, True) for i in urls]
            print('urls', urls)
            url_dict[item_url] = urls

        return url_dict


class TestExtracter:

    def test_extract_img_url(self):
        with open(r'Z:\TEMP\8707481627618284\imgurls.url_dict') as f:
            import json
            url_dict = json.load(f)
        ret = Extracter.extract_img_url(url_dict)
        print(ret)
