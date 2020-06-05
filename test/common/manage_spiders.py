'''
管理爬虫和任务
'''
import json
import os
import random
import shutil

from config import client, crawlab_database, users_col, \
    tasks_col, spiders_col, nodes_col, MONGO_PATH, triggers_col
from bson.objectid import ObjectId
from datetime import datetime
import pymongo
from flask_apscheduler import APScheduler

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from pytz import utc
from time import sleep
import tempfile, pathlib

try:
    import pytest
except:
    pass


class Spider:

    def __init__(self, spiders_col_=None):
        if not spiders_col_:
            self.spiders_col_ = spiders_col
        else:
            self.spiders_col_ = spiders_col_

    def create_spider(self, name: str, remark: str, config: dict) -> pymongo.results.InsertOneResult:
        spider = {
            "name": name,
            "col": "data",
            "remark": remark,
            "lastrunts": None,
            "laststatus": None,
            "create_ts": datetime.utcnow(),
            "update_ts": datetime.utcnow(),
            "config": config,
            "triggers": []
        }
        return self.spiders_col_.insert_one(spider)

    def get_spiders(self, page_no=1, cols='default', sort=None, page_size=10, filter=None):
        if cols == 'all':
            cols = None
        elif cols == 'default':
            cols = {'name': 1, 'update_ts': 1, 'remark': 1, "_id": -1, 'lastrunts': 1, 'laststatus': 1}

        if not sort:
            sort = [("_id", -1)]
        if not filter:
            filter = {}

        ret = self.spiders_col_.find(
            filter, cols
        ).skip(page_size * (page_no - 1)).limit(page_size).sort(sort)
        return list(ret)

    def get_spider(self, cols='default', filter=None):
        if cols == 'all':
            cols = None
        elif cols == 'default':
            cols = {'display_name': 1, 'update_ts': 1, 'remark': 1, "_id": -1}

        if not filter:
            filter = {}
        return self.spiders_col_.find_one(filter, cols)

    def set_spider(self, filter: dict, kwargs: dict):
        return spiders_col.find_one_and_update(filter, {'$set': kwargs})

    def add_trigger(self, filter: dict, trigger_id: str):
        return self.spiders_col_.find_one_and_update(filter, {'$push': {'triggers': trigger_id}})

    def del_spiders_one(self, filter):
        return self.spiders_col_.delete_one(filter)

    def del_spiders_many(self, filter):
        if not filter:
            raise Exception("filter空")
        return self.spiders_col_.delete_many(filter)


class TestSpider:
    def setup_class(self):
        client = pymongo.MongoClient("127.0.0.1:27017")
        crawlab_database = client['crawlab_test']
        users_col = crawlab_database['users']
        tasks_col = crawlab_database['tasks']
        spiders_col = crawlab_database['spiders']
        nodes_col = crawlab_database['nodes']

        self.Spider = Spider(spiders_col)

    def test_create_get_spiders(self):
        test_name = 'test_name'
        test_remark = 'test_remark'
        test_config = '{"test_config": 1}'

        self.Spider.create_spider(test_name, test_remark, test_config)
        spiders = self.Spider.get_spiders(cols='all')
        assert len(spiders) > 0
        spider = spiders[0]
        assert spider['name'] == test_name
        assert spider['remark'] == test_remark
        assert spider['config'] == test_config
        _id = spider['_id']
        trigger_id = 'sedrgsdfgv'
        t = self.Spider.add_trigger({'_id': _id}, trigger_id)
        assert self.Spider.get_spider(cols='all', filter={'_id': _id})['triggers'] == [trigger_id]

    def test_del_spiders(self):
        test_name = 'test_name'
        test_remark = 'test_remark'
        test_config = '{"test_config": 1}'

        self.Spider.create_spider(test_name, test_remark, test_config)
        self.Spider.create_spider(test_name, test_remark, test_config)
        self.Spider.create_spider(test_name, test_remark, test_config)

        self.Spider.del_spiders_many({"name": test_name})
        assert self.Spider.get_spiders(filter={"name": test_name}) == []



