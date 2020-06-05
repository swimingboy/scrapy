'''
用于从mongodb导出数据到mysql
'''

from bson import ObjectId
import os
from config import MYSQL_PATH, users_col, data_col, nodes_col, \
    spiders_col, triggers_col, MYSQL_PASSWORD, HOST, tasks_col, \
    shop_info_col, CURRENT_DIR, MYSQL_HOST, shop_news_col, \
    export_data_log_col, export_img_log_col
import inspect
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Type
import hashlib
from common.download_img import img_download_queue, check_img_from_size
from typing import List, Tuple, Union, Any
from common.dict_project import dict_project, ChangeName
from common.mysql_orm import database, ScNews, ScrapMall, BaseModel, \
    database, ScrapStaff, ScStaffWork, ScrapStaffPhotos
try:
    import fcntl
    import ptvsd
    HAS_FCNTL = True
except:
    HAS_FCNTL = False
class Constant:
    modify_flg_default = 0
    modify_flg_upgrade = 1
    modify_flg_remove = 2

    manzoku_flg_to_export = 0
    manzoku_flg_exported = 1

    constant_blood_A = 1
    constant_blood_B = 2
    constant_blood_AB = 3
    constant_blood_O = 4

    staff_mongo_filed = [
        'name',
        'age',
        'tall',
        'bust',
        'cup',
        'waist',
        'hip',
        'blood',
        'manager_comment',
        'self_comment'
    ]
    staff_mysql_filed = [
        'sc_staff_name',
        'sc_staff_age',
        'sc_staff_tall',
        'sc_staff_bust',
        'sc_staff_cup',
        'sc_staff_waist',
        'sc_staff_hip',
        'sc_staff_blood',
        'sc_staff_manager_comment',
        'sc_staff_girl_comment'
    ]
    img_url_map = 'sc_photo_memo1'
    img_size_map = 'sc_photo_memo2'

    staff_url_map = 'sc_staff_memo9'
    staff_photo_export_map = 'sc_staff_memo8'

    mall_info_mongo_filed = [
        'mall_id_org',
        'mall_name',
        'mall_site_id',
        'pref_id'
    ]
    mall_info_mysql_filed = [
        'sc_mall_original_id',
        'sc_mall_name',
        'sc_mall_site_id',
        'sc_mall_pref_id'
    ]


class Export:
    '''
    以任务为单位导出数据到mysql, 每次处理一个任务

    export_T用于收集必要参数传入_export_T, _export_T可以处理预处理参数后传入_export_item
    收集                                  参数验证                        导出
    '''

    @staticmethod
    def _export_news(
            sc_news_id: int,
            sc_news_mall_id: int,
            sc_news_title: str,  # 目前拿不到, 按空
            sc_news_body: str,  # HTML体
            sc_news_startdate: datetime,  # 抓取时间
            # sc_news_modify_flg: int,  # 0-默认 首次创建  1-有更新  2-已移除
            sc_news_regdate: datetime  # 记录日期
    ):
        to_fill_fields = locals()
        return Export._export_item(
            ScNews, to_fill_fields,
            modify_flg_name='sc_news_modify_flg',
            contrast_exclusion_fields=['sc_news_regdate', 'sc_news_startdate']
        )

    @staticmethod
    def export_news(task, spider, config, mall_info, mall_id):

        if not shop_info_col.find_one(dict_project(mall_info, {'mall_id_org': 1})):
            # 之前没插入过的, 补票上车 2020/1/1
            shop_info_col.insert_one(mall_info)

        old_item = ScNews.get_or_none(ScNews.sc_news_mall_id == mall_id)
        new_shop_news = shop_news_col.find_one({'task_id': str(task.get('_id'))})

        if new_shop_news:
            new_shop_news_id: ObjectId = new_shop_news.get('_id')

            return Export._export_news(
                old_item.sc_news_id if old_item else None,
                mall_id,
                '',
                new_shop_news.get('news').strip(),
                new_shop_news_id.generation_time,
                # Constant.modify_flg_upgrade,
                datetime.utcnow()
            )

    @staticmethod
    def _trans_blood(text: Union[str, int]) -> int:
        if type(text) == int:
            return text
        else:
            sc_staff_blood = {
                "A": Constant.constant_blood_A,
                "B": Constant.constant_blood_B,
                "O": Constant.constant_blood_O,
                "AB": Constant.constant_blood_AB
            }.get(text, '')
            return sc_staff_blood

    @staticmethod
    def _export_staff(
            sc_staff_id: Optional[int],
            sc_staff_mall_id: int,
            sc_staff_name: str,
            sc_staff_age: str,
            sc_staff_tall: int,
            sc_staff_bust: int,
            sc_staff_cup: str,
            sc_staff_waist: int,
            sc_staff_hip: int,

            sc_staff_blood: Union[int, str],

            sc_staff_manager_comment: str,
            sc_staff_girl_comment: str,
            url: str,
            sc_staff_regdate: datetime = None,
            # sc_staff_modify_flg: int,
            QA: Dict[str, str] = None
    ):

        sc_staff_blood = Export._trans_blood(sc_staff_blood)
        if not sc_staff_regdate:
            sc_staff_regdate = datetime.utcnow()

        kwargs = locals()
        index = 1

        if not QA:
            QA = {}
        # print(QA)
        for Q, A in QA.items():
            kwargs[f'sc_staff_question{index}'] = Q
            kwargs[f'sc_staff_answer{index}'] = A
            index += 1
        del kwargs['QA']
        kwargs[Constant.staff_url_map] = url
        del kwargs['url']
        # kwargs = {key: value for key, value in kwargs.items() if value != ''}
        Export._export_item(
            ScrapStaff, kwargs,
            modify_flg_name='sc_staff_modify_flg',
            contrast_exclusion_fields=['sc_staff_regdate']
        )

    @staticmethod
    def export_staff(task, spider, config, mall_info, mall_id):
        '''
        获取本店所有未移除的员工, 分取之与最新的数据对比,
            不同:
                更新, 记录为已处理
            否则:
                不动, 记录url与id映射

        获取不在已处理列表的新数据
            分项处理
        '''
        processed_url = []  # 已处理的url
        old_item_url_id_map: Dict[str, int] = {}
        inser_data_log(f"获取已有员工", mall_id=mall_id)
        for old_item in ScrapStaff.select().where(
                ScrapStaff.sc_staff_mall_id == mall_id,
                ScrapStaff.sc_staff_modify_flg != Constant.modify_flg_remove
        ).dicts():
            this_url = old_item.get(Constant.staff_url_map)

            mongo_staff_item = data_col.find_one({
                'task_id': str(task.get('_id')),
                'url': this_url
            })
            if not mongo_staff_item:
                # mysql item 在 mongo 无对应物
                # 设置成已移除
                _itme: ScrapStaff = ScrapStaff.get(
                    ScrapStaff.sc_staff_id == old_item['sc_staff_id']
                )
                _itme.sc_staff_modify_flg = Constant.modify_flg_remove
                _itme.save()
                processed_url.append(this_url)
            else:
                # mysql item 在 mongo 有对应物
                # 记一记url和id, 待会不必再查了
                old_item_url_id_map[this_url] = old_item['sc_staff_id']
        inser_data_log(f"获取待新增员工", mall_id=mall_id)
        # 找一找新加的
        new_staffs = data_col.find({
            'task_id': str(task.get('_id')),
            'url': {"$nin": processed_url}
        })
        for staff in new_staffs:
            # print('新增')
            to_insert = {key_mysql: staff.get(key_mongo) for key_mongo, key_mysql in
                         zip(Constant.staff_mongo_filed, Constant.staff_mysql_filed)}
            url = staff.get('url')
            to_insert['sc_staff_id'] = old_item_url_id_map.get(url)

            to_insert['sc_staff_mall_id'] = mall_id
            to_insert['url'] = url
            to_insert['QA'] = dict(list(zip(
                staff.get('questions').split('|'),
                staff.get('answers').split('|')
            )))
            inser_data_log(f"构造staff并导出", staff=staff, mall_id=mall_id, to_insert=to_insert)

            Export._export_staff(**to_insert)

    @staticmethod
    def _export_timesheet(
            sc_staff_work_id: int,
            sc_staff_sc_id: int,
            sc_staff_work_mall_id: int,
            sc_staff_work_date: date,
            sc_staff_work_starttime: datetime,
            sc_staff_work_endtime: datetime,
            # sc_staff_work_modify_flg: int,
            sc_staff_work_regdate: datetime = None
    ):
        if not sc_staff_work_regdate:
            sc_staff_work_regdate = datetime.utcnow()
        kwargs = locals()

        Export._export_item(
            ScStaffWork, kwargs,
            modify_flg_name='sc_staff_work_modify_flg',
            contrast_exclusion_fields=['sc_staff_work_regdate', 'sc_staff_work_id']
        )

    @staticmethod
    def export_timesheet(task, spider, config, mall_info, mall_id):
        '''
        获取本店所有未移除的员工, 取其时间.
        小于今日者列为移除.
        按日往后迭代十四日:
            对比新数据和旧数据
            不同:
                update
            否则:
                不动
        '''

        for mysql_staff_item in ScrapStaff.select().where(
                ScrapStaff.sc_staff_mall_id == mall_id,
                ScrapStaff.sc_staff_modify_flg != Constant.modify_flg_remove
        ):
            mysql_staff_item: ScrapStaff
            staff_id = mysql_staff_item.sc_staff_id
            # 小于今日者设为移除
            ScStaffWork.update(
                sc_staff_work_modify_flg=Constant.modify_flg_remove
            ).where(
                ScStaffWork.sc_staff_sc_id == staff_id,
                ScStaffWork.sc_staff_work_date < date.today()
            )

            this_url = getattr(mysql_staff_item, Constant.staff_url_map)
            mongo_staff_item = data_col.find_one({
                'task_id': str(task.get('_id')),
                'url': this_url
            })
            # 数据转换到字典里方便检索

            new_staff_timesheet = mongo_staff_item.get('timesheet') or ""
            mongo_staff_timesheet: Dict[date, Tuple[datetime, datetime]] = {}
            if new_staff_timesheet:
                new_staff_timesheet = new_staff_timesheet.split(", ")
                new_staff_timesheet = [i.split('-') for i in new_staff_timesheet]
                new_staff_timesheet = [
                    [datetime.strptime(j, '%Y/%m/%d %H:%M') for j in i]
                    for i in new_staff_timesheet
                ]
                for start_date_new, end_date_new in new_staff_timesheet:
                    if start_date_new.year == 1970:
                        now = date(year=end_date_new.year, month=end_date_new.month, day=end_date_new.day)
                    else:
                        now = date(year=start_date_new.year, month=start_date_new.month, day=start_date_new.day)
                    mongo_staff_timesheet[now] = (start_date_new, end_date_new)
            # 开始迭代未来十四天
            for i in range(14):
                current_date = date.today() + timedelta(days=i)
                current_date_in_new = current_date in mongo_staff_timesheet
                # 找找这天mysql有无数据
                mysql_staff_timesheet = ScStaffWork.get_or_none(
                    ScStaffWork.sc_staff_work_modify_flg != Constant.modify_flg_remove,
                    ScStaffWork.sc_staff_work_date == current_date,
                    ScStaffWork.sc_staff_sc_id == staff_id
                )
                current_date_in_old = mysql_staff_timesheet is not None
                if not current_date_in_new and current_date_in_old:
                    # 有旧无新, 置为移除
                    mysql_staff_timesheet.sc_staff_work_modify_flg = Constant.modify_flg_remove
                    mysql_staff_timesheet.save()
                else:
                    # 有新无旧 或者 二者都有, 拼装结构体导出
                    start_date_new, end_date_new = mongo_staff_timesheet.get(current_date, [None, None])
                    if start_date_new and end_date_new:
                        Export._export_timesheet(
                            mysql_staff_timesheet.sc_staff_work_id if current_date_in_old else None,
                            staff_id, mall_id,
                            current_date,
                            start_date_new, end_date_new
                        )

    @staticmethod
    def _export_photos(
            sc_photo_id: Optional[int],
            sc_staff_sc_id: int,
            sc_photo_path: str,
            sc_photo_filename: str,
            sc_photo_order: int,
            # sc_photo_modify_flg: int,
            url: str,
            sc_photo_regdate: datetime = None,
    ):
        if not sc_photo_regdate:
            sc_photo_regdate = datetime.utcnow()
        kwargs = locals()
        kwargs[Constant.img_url_map] = url
        del kwargs['url']
        assert 'url' not in kwargs
        Export._export_item(
            ScrapStaffPhotos, kwargs,
            modify_flg_name='sc_photo_modify_flg',
            contrast_exclusion_fields=['sc_photo_regdate', 'sc_photo_order', 'sc_photo_id']
        )

    @staticmethod
    def export_photos(task, spider, config, mall_info, mall_id):
        '''

        :param task:
        :param spider:
        :param config:
        :param mall_info:
        :param mall_id:
        :return: none

        从mysql获取本店所有未移除的员工
        从mysql取其图URL.
        从mongo取其图url.
        mysql在而mongo无, 删除记录和文件
        导出数据
        爬图储之(..., flag)


        '''
        inser_data_log(f'导出 图片 开始寻找员工', mall_id=mall_id)
        for mysql_staff_item in ScrapStaff.select().where(
                ScrapStaff.sc_staff_mall_id == mall_id,
                ScrapStaff.sc_staff_modify_flg != Constant.modify_flg_remove
        ):
            mysql_staff_item: ScrapStaff
            staff_id = mysql_staff_item.sc_staff_id
            this_url = getattr(mysql_staff_item, Constant.staff_url_map)

            mongo_staff_item = data_col.find_one({
                'task_id': str(task.get('_id')),
                'url': this_url
            })
            mongo_photo_urls: List[str] = mongo_staff_item.get('img_url')
            print(f"初始{mongo_photo_urls=}")
            for mysql_photo_item in ScrapStaffPhotos.select().where(
                    ScrapStaffPhotos.sc_staff_sc_id == staff_id,
                    ScrapStaffPhotos.sc_photo_modify_flg != Constant.modify_flg_remove
            ):
                mysql_photo_item: ScrapStaffPhotos

                mysql_photo_url = getattr(mysql_photo_item, Constant.img_url_map)
                # TODO 给orm基类加一个字段映射, 避免这种getattr的写法
                if mysql_photo_url not in mongo_photo_urls:
                    # 旧在而新无, 删除记录和文件
                    path = mysql_photo_item.sc_photo_path
                    filename = mysql_photo_item.sc_photo_filename
                    try:
                        os.remove(f"{path}/{filename}")
                    except:
                        pass
                    finally:
                        mysql_photo_item.sc_photo_modify_flg = Constant.modify_flg_remove
                        mysql_photo_item.save()
                else:
                    mongo_photo_urls.remove(mysql_photo_url)
                    pass
            print(f"旧在而新无, 删除记录和文件后{mongo_photo_urls=}")
            # mongo_photo_urls
            # data_col.update_one({
            #     'task_id': str(task.get('_id')),
            #     'url': this_url
            # })
            inser_data_log(f'导出 图片 本次将导出{len(mongo_photo_urls)=}', mall_id=mall_id)
            for index, url in enumerate(mongo_photo_urls):
                mall_site_id = mall_info.get('mall_site_id', 0)
                filepath = f"{CURRENT_DIR}/static/{mall_site_id}/{mall_id}"
                if not os.path.exists(filepath):
                    os.makedirs(filepath)
                filename = hashlib.md5(bytes(url, encoding='utf-8')).hexdigest() + '.jpg'

                Export._export_photos(
                    None,
                    staff_id,
                    filepath,
                    filename,
                    index,
                    # modify_flag,
                    url
                )

                def _set_del_status(filename):
                    if not database.is_connection_usable():
                        database.connect()
                    item = ScrapStaffPhotos.get_or_none(
                        ScrapStaffPhotos.sc_photo_filename == filename
                    )
                    if item:
                        item.sc_photo_modify_flg = Constant.modify_flg_remove
                        item.save()
                        img_url = getattr(item, Constant.img_url_map)
                        # mongo里的img_url是img_urls
                        mongo_item = data_col.find_one(
                            {'img_url': {'$elemMatch': {'$eq': img_url}}}
                        )
                        if mongo_item:
                            data_col.update_many(
                                {'img_url': {'$elemMatch': {'$eq': img_url}}},
                                {"$pull": {'img_url': img_url}}
                            )
                    database.close()

                def _set_img_size(filename, size: Tuple[int, int]):
                    if not database.is_connection_usable():
                        database.connect()
                    item = ScrapStaffPhotos.get_or_none(
                        ScrapStaffPhotos.sc_photo_filename == filename
                    )
                    if item:
                        setattr(item, Constant.img_size_map, size)
                        item.save()
                    database.close()

                # 添加url到下载队列
                img_download_queue.put((
                    url, filepath, filename, _set_del_status, _set_img_size
                ))
        return

    @staticmethod
    def _export_wrapper(func):
        def wrapper(*args, **kw):
            local = locals()

            kwargs = {arg: local[arg] for arg in inspect.getfullargspec(func).args}

            return func(*args, **kw)

    @staticmethod
    def _export_item(
            table: Type[BaseModel], to_fill_fields: dict,
            modify_flg_name: str = '',
            contrast_exclusion_fields: List[str] = None
    ):
        if not contrast_exclusion_fields:
            contrast_exclusion_fields = []
        item = table.get_or_none(
            *[
                table._meta.fields[key] == value
                for key, value in to_fill_fields.items()
                if key not in contrast_exclusion_fields
            ]
        )
        # contrast_exclusion_fields 没有填写过主键, 假设了如果两个数据是全同的, 其主键也是相同的
        # 即不会发生关键数据都相同, 但是主键不同
        if item:
            # 存在全同对象, 过
            pass
        else:
            # 不存在全同对象, 按主键查询
            primary_key = table._meta.primary_key
            assert primary_key.column_name in to_fill_fields
            condition = primary_key == to_fill_fields[primary_key.column_name]
            item = table.get_or_none(condition)
            if item:
                # 无全同对象, 有同主键对象, 得更新
                if modify_flg_name:
                    to_fill_fields[modify_flg_name] = Constant.modify_flg_upgrade
                table.update(**{
                    key: value
                    for key, value in to_fill_fields.items()
                }).where(condition).execute()
            else:
                # 无全同对象, 无同主键对象, 得插入
                if modify_flg_name:
                    to_fill_fields[modify_flg_name] = Constant.modify_flg_default
                table.insert(**to_fill_fields).execute()

    @staticmethod
    def _export_mall(
            sc_mall_name: str,
            sc_mall_site_id: int,
            sc_mall_original_id: int,
            sc_mall_pref_id: int,
            sc_mall_regdate: datetime = None,
            sc_mall_id: int = None  # 为维护原代码, 此处id不使用自增, 而是同sc_mall_original_id
    ):
        if not sc_mall_regdate:
            sc_mall_regdate = datetime.utcnow()
        if not sc_mall_id:
            sc_mall_id = sc_mall_original_id
        kwargs = locals()
        return Export._export_item(ScrapMall, kwargs)

    @staticmethod
    def export_mall(task, spider, config, mall_info, mall_id):
        '''

        :param task:
        :param spider:
        :param config:
        :param mall_info:
        :param mall_id:
        :return: none

        找此处有无对应的mall_info于mysql,
        有则
            查看是否相同,
            同则:
                过
            否则:
                更新
        否则:
            插入
        '''
        item = dict_project(
            mall_info, {
                mongo_filed: ChangeName(mysql_filed)
                for mongo_filed, mysql_filed in zip(
                    Constant.mall_info_mongo_filed, Constant.mall_info_mysql_filed
                )
            }
        )
        return Export._export_mall(**item)

def inser_data_log(text, **other):
    export_data_log_col.insert_one({
        'text': text,
        **other
    })

def inser_img_log(text, **other):
    export_img_log_col.insert_one({
        'text': text,
        **other
    })

class FileLock:
    def __init__(self, task_id):
        self.f = open(f"/tmp/{task_id}.lock", 'w') if HAS_FCNTL else None
    def __enter__(self):
        HAS_FCNTL and fcntl.flock(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_tb:
            HAS_FCNTL and fcntl.flock(self.f, fcntl.LOCK_UN)
        else:
            inser_data_log('FileLock下报错', exc_tb=str(exc_tb))

        self.f.close()



def export_data_job():
    # 只有任务可以带来数据, 因此每次迭代所有任务未处理过的任务.
    # task加入一个导出状态
    # lock = locks_col.find_one({'name': 'export_data_job_lock'})
    # if lock:
    #     return
    # print(lock)
    # locks_col.insert_one({'name': 'export_data_job_lock'})
    # print(f"{tasks.count()=}")
    # database.execute("SET time_zone='+00:00'")
    # database.commit()


    if not database.is_connection_usable():
        database.connect()
    task = list(tasks_col.aggregate([
        {"$match": {"export": False, 'status': 'finished'}},
        {"$sample": {'size': 1}},
    ]))
    # print(task)
    # inser_data_log('获取task', num=len(task))
    if task:
        task = task[0]
        try:
            with FileLock(task.get('_id')):
                spider = spiders_col.find_one({"_id": task.get('spider_id')})
                def _del_task_item(text, task):
                    ret = tasks_col.delete_one({"_id": task.get('_id')})
                    inser_data_log(text, task=task, ret=ret)

                if not spider:
                    print(f"\tspider not exist, delete task")
                    _del_task_item('task对应spider不存在', task)
                    raise Exception('task对应spider不存在')

                config = spider.get('config', {})
                mall_info = config.get('mall_info')
                print(f"{mall_info=}", mall_info == {}, mall_info == None)
                inser_data_log('开始导出task', mall_info=mall_info, task_id=task.get('_id'))
                if tasks_col.find_one({"_id": task.get('_id'), "export": True, 'status': 'finished'}):
                    raise Exception('已经被处理')
                breakpoint()
                if mall_info == None or mall_info == {}:
                    print(f"\tmall_info not exist, delete task")
                    _del_task_item('task对应mall_info不存在', task)
                    raise Exception('task对应mall_info不存在')
                else:
                    mall_id = mall_info.get('mall_id_org')
                    inser_data_log(f'task mall id: {mall_id}', mall_id=mall_id)
                    print("导出数据", mall_info)
                    try:
                        # 导出商家信息 1
                        inser_data_log(f'导出 商家信息', mall_id=mall_id)
                        Export.export_mall(task, spider, config, mall_info, mall_id)
                    except Exception as e:
                        print(task, '导出商家信息时错误', e)
                    try:
                        # 新闻 1.1
                        inser_data_log(f'导出 新闻', mall_id=mall_id)
                        Export.export_news(task, spider, config, mall_info, mall_id)
                    except Exception as e:
                        print(task, '导出新闻时错误', e)
                    try:
                        # 员工参数 1.2
                        inser_data_log(f'导出 员工', mall_id=mall_id)
                        Export.export_staff(task, spider, config, mall_info, mall_id)
                    except Exception as e:
                        print(task, '导出员工时错误', e)
                    try:
                        # 时间表 1.2.1
                        inser_data_log(f'导出 时间表', mall_id=mall_id)
                        Export.export_timesheet(task, spider, config, mall_info, mall_id)
                    except Exception as e:
                        print(task, '导出时间表时错误', e)
                    try:
                        # 导出图片 1.2.2
                        inser_data_log(f'导出 图片', mall_id=mall_id)
                        Export.export_photos(task, spider, config, mall_info, mall_id)
                    except Exception as e:
                        print(task, '导出图片时错误', e)
                        # raise e
                tasks_col.update_one({"_id": task.get('_id')}, {"$set": {"export": True}})
        except Exception as e:
            inser_data_log(f'发生问题', exception=str(e))

            # locks_col.delete_one({'name': 'export_data_job_lock'})
            inser_data_log(f'导出完毕', task_id=task.get('_id'))
            database.close()



def check_img_job():
    # 异步获取图片大小猜测并清理可能非者
    inser_img_log("开始获取欲导出数据", type='check_img_job')
    for staff in ScrapStaff.select().where(
            (getattr(ScrapStaff, Constant.staff_photo_export_map) != True) |
            (getattr(ScrapStaff, Constant.staff_photo_export_map) == None)
    ):
        # mysql 部分
        staff: ScrapStaff
        staff_photos: List[ScrapStaffPhotos] = ScrapStaffPhotos.select().where(
            ScrapStaffPhotos.sc_staff_sc_id == staff.sc_staff_id,
            ScrapStaffPhotos.sc_photo_modify_flg != Constant.modify_flg_remove
        ) # 这里忘记筛选非删除部分了
        inser_img_log("获得所有图片", staff_id=staff.sc_staff_id, type='check_img_job')
        staff_photo_sizes = [getattr(i, Constant.img_size_map) for i in staff_photos]
        if staff_photo_sizes:
            
            right_size = check_img_from_size(staff_photo_sizes)
            inser_img_log(f"{right_size=}", staff_id=staff.sc_staff_id)
            right_size_urls = []  # 待会给mongo那边用
            for photo in staff_photos:
                photo: ScrapStaffPhotos
                if getattr(photo, Constant.img_size_map) != right_size:
                    photo.sc_photo_modify_flg = Constant.modify_flg_remove
                    photo.save()
                else:
                    right_size_urls.append(getattr(photo, Constant.img_url_map))
            
          # mongo部分
            url = getattr(staff, Constant.staff_url_map)
            filter = {'url': url}
            inser_img_log(f"查找mongodb", staff_id=staff.sc_staff_id, type='check_img_job')
            item = data_col.find_one(filter)
            if item:
                data_col.update_many(filter, {
                    '$set': {'img_url': right_size_urls}
                })
            inser_img_log(f"修改mongodb完成", staff_id=staff.sc_staff_id, type='check_img_job')

        setattr(staff, Constant.staff_photo_export_map, True)
        inser_img_log("迭代并修改图片标识", staff_id=staff.sc_staff_id, type='check_img_job')
        staff.save()
