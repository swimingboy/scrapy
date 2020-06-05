from __future__ import annotations

from peewee import *
from config import MYSQL_HOST, MYSQL_PASSWORD
from playhouse.shortcuts import model_to_dict
from typing import Union, Optional

database = MySQLDatabase('spider', **{
    'charset': 'utf8mb4',
    'sql_mode': 'PIPES_AS_CONCAT',
    'use_unicode': True,
    'host': MYSQL_HOST,
    'port': 3306,
    'user': 'root',
    'password': MYSQL_PASSWORD
})


class UnknownField(object):
    def __init__(self, *_, **__): pass


class BaseModel(Model):
    class Meta:
        database = database

    @classmethod
    def __build_sq(cls,  query, filters):
        sq = cls.select()
        if query:
            # Handle simple lookup using just the primary key.
            if len(query) == 1 and isinstance(query[0], int):
                sq = sq.where(cls._meta.primary_key == query[0])
            else:
                sq = sq.where(*query)
        if filters:
            sq = sq.filter(**filters)
        return sq

    @classmethod
    def get_dict(cls, *query, **filters) -> dict:
        sq = cls.__build_sq(query, filters)
        if sq.count():
            return sq.dicts().get()
        else:
            return {}

    @classmethod
    def get_or_none(cls, *query, **filters) -> Optional[BaseModel]:
        sq = cls.__build_sq(query, filters)
        if sq.count():
            return sq.get()
        else:
            return None

class ScNews(BaseModel):
    sc_news_body = TextField(null=True)
    sc_news_filename = TextField(null=True)
    sc_news_id = AutoField()
    sc_news_image = IntegerField(null=True)
    sc_news_jenre = IntegerField(null=True)
    sc_news_mall_id = IntegerField(null=True)
    sc_news_manzoku_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_news_memo1 = TextField(null=True)
    sc_news_memo2 = TextField(null=True)
    sc_news_memo3 = TextField(null=True)
    sc_news_memo4 = TextField(null=True)
    sc_news_memo5 = TextField(null=True)
    sc_news_modify_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_news_old_id = IntegerField(null=True)
    sc_news_path = TextField(null=True)
    sc_news_regdate = DateTimeField(null=True)
    sc_news_startdate = DateTimeField(null=True)
    sc_news_title = CharField(null=True)
    sc_news_update = DateTimeField(constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])

    class Meta:
        table_name = 'sc_news'


class ScStaffWork(BaseModel):
    sc_staff_sc_id = IntegerField()
    sc_staff_work_date = DateField(null=True)
    sc_staff_work_endtime = DateTimeField(null=True)
    sc_staff_work_flg = IntegerField(null=True)
    sc_staff_work_id = AutoField()
    sc_staff_work_mall_id = IntegerField(null=True)
    sc_staff_work_manzoku_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_staff_work_memo1 = TextField(null=True)
    sc_staff_work_memo2 = TextField(null=True)
    sc_staff_work_memo3 = TextField(null=True)
    sc_staff_work_memo4 = TextField(null=True)
    sc_staff_work_memo5 = TextField(null=True)
    sc_staff_work_modify_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_staff_work_regdate = DateTimeField(null=True)
    sc_staff_work_staff_id = IntegerField(null=True)
    sc_staff_work_starttime = DateTimeField(null=True)
    sc_staff_work_update = DateTimeField(constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])

    class Meta:
        table_name = 'sc_staff_work'


class ScapConstant(BaseModel):
    scap_default_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    scap_id = AutoField()
    scap_manzoku_id = IntegerField(null=True)
    scap_memo = CharField(null=True)
    scap_name = CharField(null=True)
    scap_type = CharField(null=True)

    class Meta:
        table_name = 'scap_constant'


class ScrapMall(BaseModel):
    sc_mall_amu_area_id = IntegerField(null=True)
    sc_mall_business_id = IntegerField(null=True)
    sc_mall_id = IntegerField(primary_key=True)
    sc_mall_memo1 = TextField(null=True)
    sc_mall_memo2 = TextField(null=True)
    sc_mall_memo3 = TextField(null=True)
    sc_mall_memo4 = TextField(null=True)
    sc_mall_memo5 = TextField(null=True)
    sc_mall_name = CharField(null=True)
    sc_mall_original_id = IntegerField(null=True)
    sc_mall_pref_id = IntegerField(null=True)
    sc_mall_regdate = DateTimeField(null=True)
    sc_mall_site_id = IntegerField(null=True)
    sc_mall_update = DateTimeField(constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])

    class Meta:
        table_name = 'scrap_mall'


class ScrapStaff(BaseModel):
    sc_mz_staff_id = IntegerField(null=True)
    sc_staff_age = IntegerField(null=True)
    sc_staff_answer1 = TextField(null=True)
    sc_staff_answer10 = TextField(null=True)
    sc_staff_answer11 = TextField(null=True)
    sc_staff_answer12 = TextField(null=True)
    sc_staff_answer13 = TextField(null=True)
    sc_staff_answer14 = TextField(null=True)
    sc_staff_answer15 = TextField(null=True)
    sc_staff_answer16 = TextField(null=True)
    sc_staff_answer17 = TextField(null=True)
    sc_staff_answer18 = TextField(null=True)
    sc_staff_answer19 = TextField(null=True)
    sc_staff_answer2 = TextField(null=True)
    sc_staff_answer20 = TextField(null=True)
    sc_staff_answer21 = TextField(null=True)
    sc_staff_answer22 = TextField(null=True)
    sc_staff_answer23 = TextField(null=True)
    sc_staff_answer24 = TextField(null=True)
    sc_staff_answer25 = TextField(null=True)
    sc_staff_answer26 = TextField(null=True)
    sc_staff_answer27 = TextField(null=True)
    sc_staff_answer28 = TextField(null=True)
    sc_staff_answer29 = TextField(null=True)
    sc_staff_answer3 = TextField(null=True)
    sc_staff_answer30 = TextField(null=True)
    sc_staff_answer4 = TextField(null=True)
    sc_staff_answer5 = TextField(null=True)
    sc_staff_answer6 = TextField(null=True)
    sc_staff_answer7 = TextField(null=True)
    sc_staff_answer8 = TextField(null=True)
    sc_staff_answer9 = TextField(null=True)
    sc_staff_birth_day = IntegerField(null=True)
    sc_staff_birth_month = IntegerField(null=True)
    sc_staff_birthplace = IntegerField(null=True)
    sc_staff_blog_url = CharField(null=True)
    sc_staff_blood = IntegerField(null=True)
    sc_staff_bust = IntegerField(null=True)
    sc_staff_cup = CharField(null=True)
    sc_staff_diarymail = CharField(null=True)
    sc_staff_diarymailto = CharField(null=True)
    sc_staff_diaryuid = CharField(null=True)
    sc_staff_entertainer = CharField(null=True)
    sc_staff_entrydate = DateTimeField(null=True)
    sc_staff_entrytype = IntegerField(constraints=[SQL("DEFAULT 0")])
    sc_staff_flash_flg = IntegerField(null=True)
    sc_staff_flash_sort = IntegerField(null=True)
    sc_staff_girl_comment = TextField(null=True)
    sc_staff_gravure_url = CharField(null=True)
    sc_staff_hip = IntegerField(null=True)
    sc_staff_id = AutoField()
    sc_staff_image1 = IntegerField(null=True)
    sc_staff_image2 = IntegerField(null=True)
    sc_staff_image3 = IntegerField(null=True)
    sc_staff_isman = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_staff_kana = CharField(null=True)
    sc_staff_mailstatus = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_staff_mall_id = IntegerField()
    sc_staff_manager_comment = TextField(null=True)
    sc_staff_manzoku_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_staff_memo1 = TextField(null=True)
    sc_staff_memo2 = TextField(null=True)
    sc_staff_memo3 = TextField(null=True)
    sc_staff_memo4 = TextField(null=True)
    sc_staff_memo5 = TextField(null=True)
    sc_staff_memo6 = TextField(null=True)
    sc_staff_memo7 = TextField(null=True)
    sc_staff_memo8 = TextField(null=True)
    sc_staff_memo9 = TextField(null=True)
    sc_staff_mobile_blog_url = CharField(null=True)
    sc_staff_mobile_disp = IntegerField(null=True)
    sc_staff_modify_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_staff_movie_url = CharField(null=True)
    sc_staff_name = CharField(null=True)
    sc_staff_new_flg = IntegerField(null=True)
    sc_staff_ng_flg = IntegerField(null=True)
    sc_staff_pc_disp = IntegerField(null=True)
    sc_staff_pref_id = IntegerField(null=True)
    sc_staff_question1 = TextField(null=True)
    sc_staff_question10 = TextField(null=True)
    sc_staff_question11 = TextField(null=True)
    sc_staff_question12 = TextField(null=True)
    sc_staff_question13 = TextField(null=True)
    sc_staff_question14 = TextField(null=True)
    sc_staff_question15 = TextField(null=True)
    sc_staff_question16 = TextField(null=True)
    sc_staff_question17 = TextField(null=True)
    sc_staff_question18 = TextField(null=True)
    sc_staff_question19 = TextField(null=True)
    sc_staff_question2 = TextField(null=True)
    sc_staff_question20 = TextField(null=True)
    sc_staff_question21 = TextField(null=True)
    sc_staff_question22 = TextField(null=True)
    sc_staff_question23 = TextField(null=True)
    sc_staff_question24 = TextField(null=True)
    sc_staff_question25 = TextField(null=True)
    sc_staff_question26 = TextField(null=True)
    sc_staff_question27 = TextField(null=True)
    sc_staff_question28 = TextField(null=True)
    sc_staff_question29 = TextField(null=True)
    sc_staff_question3 = TextField(null=True)
    sc_staff_question30 = TextField(null=True)
    sc_staff_question4 = TextField(null=True)
    sc_staff_question5 = TextField(null=True)
    sc_staff_question6 = TextField(null=True)
    sc_staff_question7 = TextField(null=True)
    sc_staff_question8 = TextField(null=True)
    sc_staff_question9 = TextField(null=True)
    sc_staff_ranking = IntegerField(null=True)
    sc_staff_reco_flg = IntegerField(null=True)
    sc_staff_regdate = DateTimeField(null=True)
    sc_staff_sendmailtime = DateTimeField(null=True)
    sc_staff_sex = IntegerField(null=True)
    sc_staff_sharebtn_hidden = IntegerField(constraints=[SQL("DEFAULT 0")])
    sc_staff_sort = IntegerField(null=True)
    sc_staff_tall = IntegerField(null=True)
    sc_staff_thumb_image = IntegerField(null=True)
    sc_staff_top3 = IntegerField(null=True)
    sc_staff_type1 = IntegerField(null=True)
    sc_staff_type2 = IntegerField(null=True)
    sc_staff_type3 = IntegerField(null=True)
    sc_staff_type4 = IntegerField(null=True)
    sc_staff_update = DateTimeField(constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])
    sc_staff_waist = IntegerField(null=True)
    sc_staff_wait = DateTimeField(null=True)
    sc_staff_widget = IntegerField(constraints=[SQL("DEFAULT 0")])

    class Meta:
        table_name = 'scrap_staff'


class ScrapStaffPhotos(BaseModel):
    sc_photo_desc = TextField(null=True)
    sc_photo_filename = TextField(null=True)
    sc_photo_id = AutoField()
    sc_photo_manzoku_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_photo_memo1 = TextField(null=True)
    sc_photo_memo2 = TextField(null=True)
    sc_photo_memo3 = TextField(null=True)
    sc_photo_memo4 = TextField(null=True)
    sc_photo_memo5 = TextField(null=True)
    sc_photo_modify_flg = IntegerField(constraints=[SQL("DEFAULT 0")], null=True)
    sc_photo_order = IntegerField()
    sc_photo_path = TextField(null=True)
    sc_photo_regdate = DateTimeField(null=True)
    sc_photo_update = DateTimeField(constraints=[SQL("DEFAULT CURRENT_TIMESTAMP")])
    sc_staff_sc_id = IntegerField()

    class Meta:
        table_name = 'scrap_staff_photos'



def test_get():
    ScStaffWork.get().dict