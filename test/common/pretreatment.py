'''
网页文本预处理
'''
import re
import os
from lxml import etree
from typing import List

def fullwidth_to_halfwidth(text: str):
    half = ''
    for u in text:
        num = ord(u)
        if num == 0x3000:
            num = 32
        elif 0xFF01 <= num <= 0xFF5E:
            num -= 0xfee0
        u = chr(num)
        half += u
    for old, new in zip('【】《》〈〉『』〔〕、・…', '[]<><>[][],-.'):
        half = half.replace(old, new)
    return half


def pretreatment_text(text: str):
    text = text.strip()
    text = re.sub(r'\n', '', text)
    text = re.sub(r'\r', '', text)
    text = fullwidth_to_halfwidth(text)
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\t+', '\t', text)
    text = re.sub(r'\xa0', '', text)
    text = [i for i in text]

    count = 0
    replace = '甲乙丙丁戊己庚辛壬癸子丑寅卯辰巳午未申酉戌亥'
    for index, char in enumerate(text):
        if char in ' \t':
            text[index] = replace[count]
            count += 1
            if count >= len(replace):
                count = 0
    text = "".join(text)
    return text


def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file), os.path.join(root, file).replace(str(path), ''))


def pretreatment_mongo_result(res, fields=None):
    res: list = list(res)
    if not fields:
        fields = ['_id', 'file_id']
    for i in res:
        for filed in fields:
            if filed in i:
                i[filed] = str(i[filed])
    return res

