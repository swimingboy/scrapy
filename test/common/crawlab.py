'''
操作Crawlab, 今后废弃
'''
import requests
import random
from pymongo import MongoClient

from config import client, crawlab_database, users_col, tasks_col, spiders_col, nodes_col, HTTP_ROOT


class Crawlab:
    sess = requests.session()

    def login(self, username='admin', password='admin'):
        t = self.sess.post(HTTP_ROOT + '/login', json={
            'password': username,
            'username': password
        })
        self.sess.headers['Authorization'] = t.json()['data']
        return t.json()

    def upload_spider(self, spider_name, spider_file_name):
        file = {
            "file": (f"{spider_name}.zip", open(spider_file_name, "rb")),
        }
        t = self.sess.post(HTTP_ROOT + '/spiders', files=file)
        return t.json()

    @staticmethod
    def get_spider_by_name(name):
        return spiders_col.find_one({'name': name})

    @staticmethod
    def set_spider_by_name(name, kwargs):
        return spiders_col.find_one_and_update({'name': name}, {'$set': kwargs})

    def publish_spider_by_name(self, name):
        spider = self.get_spider_by_name(name)
        _id = spider['_id']
        _id = str(_id)
        return self.sess.post(HTTP_ROOT + f'/spiders/{_id}/publish')

    def __init__(self, username=None, password=None):
        if username and password:
            self.login(username, password)
