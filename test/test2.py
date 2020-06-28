#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'linghang'

import requests as req
import re
from bs4 import BeautifulSoup
# from common.dict_project import dict_project
from common.extracter import Extracter
from common.fetch_all_url import scan
# from common.manage_spiders import Spider
# from common.manage_triggers import trigger
# from common.pretreatment import zipdir
from common.timesheet_extracter import TimesheetExtracter

results = {}

def get_elem():
    # request = {"url":"http://www.school-channel.com/girl/staff_profile.php?id=350","elem":["//*[@id=\"profile\"]/div[3]/table"],"ExtractTimesheet":True}
    # request = {"url": "https://www.sexy-gal.jp/profile.html?id=1514",
    #            "elem": ['//div[@id="content"]/section[2]'], "ExtractTimesheet": True}
    request = {"url": "https://www.yk-happy-matto.com/profile.html?girl=wkbpcmy5",
                "elem": ["//div[@id='main']/div[1]/div[2]/div[1]/div[1]/h3"]}

    url = request.get('url')
    # print(result)
    if url and url not in results:
        res = req.get(url)

        response = BeautifulSoup(res.text,'html.parser')
        with open('res.txt', 'w',encoding='utf-8') as f:
            f.write(response.text)
        # print(res.content)
        results[url] = res.content

    extracter = Extracter(results[url])
    # print(extracter)
    ExtractTimesheet = request.get('ExtractTimesheet', False)
    res = []
    try:
        for xpath in request.get('elem'):
            if ExtractTimesheet:
                # print('xpath:',request.get('elem'))
                string = extracter.extract_html_by_xpath(xpath, url)
                print('string:',string)
                timesheet = TimesheetExtracter.get_time_sheet(string)
                print('45:',timesheet)
                text: str = ", ".join([str(i) for i in timesheet])
                print('end',text)
                res.append(text)
                # print(res)
            else:
                res.append(extracter.extract_text_by_xpath(xpath))
    except Exception as e:
        print(e)
    print(res)

if __name__ == '__main__':
    get_elem()