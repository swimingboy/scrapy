import json
import os
import random
import shutil

from config import client, crawlab_database, users_col, \
    tasks_col, spiders_col, nodes_col, MONGO_PATH, triggers_col, data_col, CURRENT_DIR
from bson.objectid import ObjectId
from datetime import datetime
import pymongo
from flask_apscheduler import APScheduler
from common.manage_spiders import Spider
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from pytz import utc
from time import sleep
import tempfile, pathlib



class Trigger:
    config = {}

    def __init__(self, client_=None):
        if client_:
            self.client = client_
        else:
            self.client = client

        jobstores = {
            'default': MongoDBJobStore(
                client=self.client,
                database='crawlab_test',
                collection='trigger'
            )
        }
        executors = {
            'default': ThreadPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 10
        }
        self.config = [
            jobstores, executors, job_defaults
        ]
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults
        )

    def start(self):

        self.scheduler.start()

    @staticmethod
    def trigger_func(
            spider_id, mongodb_address=MONGO_PATH,
            database_name='crawlab_test', col_name='spiders',
            dir_name=None
    ):
        # TODO 改为同一个client的引用
        print('trigger')
        client = pymongo.MongoClient(mongodb_address)
        crawlab_database = client[database_name]
        spiders_col = crawlab_database[col_name]

        spider = Spider(spiders_col)
        s = spider.get_spider(filter={'_id': spider_id}, cols='all')
        print(s)
        if s:
            # 创建爬虫目录
            directory_name = pathlib.Path(tempfile.gettempdir())
            directory_name = directory_name / (dir_name or str(random.random())[2:])

            if directory_name.is_dir():
                shutil.rmtree(directory_name)
            if os.path.isdir(pathlib.Path(CURRENT_DIR) / 'scrapy_template'):
                shutil.copytree(pathlib.Path(CURRENT_DIR) / 'scrapy_template', directory_name)
            else:
                # 测试
                pass

            with open(directory_name / 'config.json', 'w') as f:
                json.dump(s['config'], f)
            # 创建任务
            task = {
                "spider_id": s['_id'],
                "start_ts": datetime.utcnow(),
                "finish_ts": None,
                "status": 'doing',
                "export": False
            }
            tasks_col = crawlab_database['tasks']
            insert_ret = tasks_col.insert_one(task)
            os.environ['CRAWLAB_TASK_ID'] = str(insert_ret.inserted_id)
            os.environ['CRAWLAB_MONGO_HOST'] = mongodb_address
            os.environ['BACKEND_PATH'] = CURRENT_DIR

            log_path = f"./{insert_ret.inserted_id}.log"
            tasks_col.find_one_and_update(
                {'_id': insert_ret.inserted_id}, {"$set": {'log_path': log_path}}
            )
            try:
                # 启动任务
                spider.set_spider({"_id": spider_id}, {'laststatus': 'doing'})
                
                os.chdir(directory_name)
                os.system(f"scrapy crawl main > {log_path} 2>&1")
                os.chdir(CURRENT_DIR)
            except Exception as e:
                # 任务失败
                with open(log_path, 'a+') as f:
                    f.write(str(e))
                tasks_col.find_one_and_update(
                    {'_id': insert_ret.inserted_id},
                    {"$set": {
                        'status': 'error'
                    }}
                )
                spider.set_spider({"_id": spider_id}, {'laststatus': 'error'})

            else:
                # 任务完成
                tasks_col.find_one_and_update(
                    {'_id': insert_ret.inserted_id},
                    {"$set": {
                        'status': 'finished',
                        'finish_ts': datetime.utcnow(),
                        'result_count': data_col.find({'task_id': str(insert_ret.inserted_id)}).count(),
                    }}
                )
                spider.set_spider({"_id": spider_id}, {'laststatus': 'ok'})

            # 完成时间设置
            tasks_col.find_one_and_update(
                {'_id': insert_ret.inserted_id},
                {"$set": {
                    'finish_ts': datetime.utcnow()
                }}
            )
            spider.set_spider({"_id": spider_id}, {'lastrunts': datetime.utcnow()})

    def create_trigger(self, spider_id, hour, minute, second=0,
                       mongodb_address=MONGO_PATH, database_name='crawlab_test',
                       col_name='spiders', func=None, dir_name=None
                       ):
        # TODO 改为同一个client的引用
        client = pymongo.MongoClient(mongodb_address)
        crawlab_database = client[database_name]
        spiders_col = crawlab_database[col_name]
        spider = Spider(spiders_col)
        filter = {'_id': spider_id}
        s = spider.get_spider(filter=filter)
        if s:
            job = self.scheduler.add_job(
                func or self.trigger_func,
                trigger='cron',
                args=[spider_id, mongodb_address, database_name, col_name, dir_name],
                hour=hour,
                minute=minute,
                second=second
            )
            spider.add_trigger(filter, job.id)
            return job
        else:
            raise Exception("没有此爬虫")

    def del_trigger(self, filter: dict):
        return self.client['crawlab_test']['trigger'].delete_one(filter)


class TestTrigger:
    def __init__(self):
        self.client = pymongo.MongoClient("127.0.0.1:27017")
        self.Trigger = Trigger(self.client)
        self.Trigger.start()

    @staticmethod
    def t_func(name):
        print(name, datetime.now())

        client = pymongo.MongoClient("127.0.0.1:27017")
        crawlab_database = client['crawlab_test']
        spiders_col = crawlab_database['spiders']

        spider = Spider(spiders_col)
        spider.create_spider(name, '', '')

    def test_create_trigger(self):
        now = datetime.now()
        name = "asdqwe"
        test_dir_name = 'xzdcgsdrf'
        crawlab_database = self.client['crawlab_test']
        spiders_col = crawlab_database['spiders']

        spider = Spider(spiders_col)
        test_config = {"url_re": "[l][_][i][d][=].+$", "start_url": "https://www.chin2-t.com/top/girls/",
                       "body_parameter": {"name": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[1]",
                                                   "re": "[\\u0800-\\u9fa5a-zA-z]+(?=[[])"},
                                          "age": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[1]",
                                                  "re": "(?<=[(])\\d{2}(?=[\u6b73])"},
                                          "tall": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[2]",
                                                   "re": "(?<=[.])\\d{3}(?=[\u7532])"},
                                          "bust": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[2]",
                                                   "re": "(?<=[.])\\d{2}(?=[\u4e59])"},
                                          "cup": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[2]",
                                                  "re": "(?<=[(])[a-zA-Z](?=[)])"},
                                          "waist": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[2]",
                                                    "re": "(?<=[.])\\d{2}(?=[\u4e01])"},
                                          "hip": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[2]",
                                                  "re": "(?<=[.])\\d{2}(?=[\u620a])"},
                                          "blood": {"xpath": "//div[@id=\"widget-area-right\"]/div[2]/div[2]",
                                                    "re": "(?<=[\uff65])[ABO]+(?=[\u578b])"}},
                       "comment": {"manager_comment": "//div[@id=\"widget-area-right\"]/div[6]/div",
                                   "self_comment": "//div[@id=\"widget-area-right\"]/div[4]/div", "QA": [
                               ["//ul[@id=\"blog_list\"]/li[1]/div/div[2]/h6/a",
                                "//ul[@id=\"blog_list\"]/li[1]/div/div[2]/div[2]/table/tbody/tr/td"],
                               ["//ul[@id=\"blog_list\"]/li[2]/div/div[2]/h6/a",
                                "//ul[@id=\"blog_list\"]/li[2]/div/div[2]/div[2]/table/tbody/tr/td"]]},
                       "timesheet": "//*[@id=\"widget-area-left\"]/div[5]/div/table/tbody",
                       "spider_name": "20191128_\u30c1\u30f3\u30c1\u30f3\u30c8\u30ec\u30a4\u30f3\u5343\u6b73\u30fb\u82eb\u5c0f\u7267\u5e97",
                       "news_url": "https://www.chin2-t.com/top/event/",
                       "news_xpath": "//div[@id=\"content-inner\"]/div[1]/div[2]/div/div/div"}
        s = spider.create_spider(name, '', test_config)

        t = self.Trigger.create_trigger(
            s.inserted_id, now.hour, now.minute, now.second + 4,
            mongodb_address='127.0.0.1:27017',
            dir_name=test_dir_name
        )
        assert spider.get_spider(
            cols='all', filter={'_id': s.inserted_id}
        )['triggers'] == [t.id]

        sleep(10)
        directory_name = pathlib.Path(tempfile.gettempdir())
        directory_name = directory_name / test_dir_name
        assert os.path.isdir(directory_name)
        with open(directory_name / "config.json", 'r', encoding='utf-8') as f:
            config = json.load(f)
            assert config == test_config
        sleep(100)


class Task:

    def __init__(self, tasks_col_=None):
        if not tasks_col_:
            self.tasks_col_ = tasks_col
        else:
            self.tasks_col_ = tasks_col_

    def get_tasks(self, page_no=1, cols='default', sort=None, page_size=10, filter=None):
        if cols == 'all':
            cols = None
        elif cols == 'default':
            cols = {'name': 1, 'update_ts': 1, 'remark': 1, "_id": -1}

        if not sort:
            sort = [("_id", -1)]
        if not filter:
            filter = {}

        ret = self.tasks_col_.find(
            filter, cols
        ).skip(page_size * (page_no - 1)).limit(page_size).sort(sort)
        return list(ret)

    def get_task(self, cols='default', filter=None):
        if cols == 'all':
            cols = None
        elif cols == 'default':
            cols = {'name': 1, 'update_ts': 1, 'remark': 1, "_id": -1}

        if not filter:
            filter = {}
        return self.tasks_col_.find_one(filter, cols)


    def set_task(self, filter: dict, kwargs: dict):
        return self.tasks_col_.find_one_and_update(filter, {'$set': kwargs})

    def del_task(self, filter: dict):
        return self.tasks_col_.delete_one(filter)

    def del_tasks(self, filter: dict):
        return self.tasks_col_.delete_many(filter)

    @staticmethod
    def run_spider(spider_id: ObjectId):
        print('run_spider')
        t = trigger.scheduler.add_job(
            trigger.trigger_func,
            trigger='date',
            args=[spider_id]
        )
        print(f'{t=}')


trigger = Trigger()
