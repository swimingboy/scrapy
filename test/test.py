#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'linghang'


import requests as req
from bs4 import BeautifulSoup
from lxml import etree
url = 'https://www.prettystyle.net/schedule/'
xpath = '//div[@id="res"]/div/div'
res = req.get(url)
html = BeautifulSoup(res.text,'html.parser')
# selector = etree.HTML(html)
# elme = selector.xpath(xpath)
print(html)

import re
