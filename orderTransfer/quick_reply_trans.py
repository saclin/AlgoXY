# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import sys
import ctypes
from DBConnector import DBConnecter


def a(x):
    if x is None:
        x = ''
    return x


def b(x):
    if x is None:
        x = 2
    return int(x)


def run(company_id, created_by, user_id):
    reload(sys)
    sys.setdefaultencoding('utf8')

    target_conn = DBConnecter().get_mysql_conn(conf=DBConnecter().target_conf)
    target_cursor = target_conn.cursor()

    sql = 'set NAMES utf8mb4;'
    target_cursor.execute(sql)
    mongo_debug_manager = DBConnecter()
    mongo_client = mongo_debug_manager.get_aliyun_mongo_client()
    data = mongo_client["51zan"]["chatReplyQuick"].find({"userId": user_id})

    for record in data:
        sql = "INSERT INTO reply_category (name, user_id, company_id, type, priority, " \
              "create_time, top) VALUES ('{}', '{}', '{}', '1', '8', '1539574119', '0');".format(record['groupName'], str(_created_by), str(company_id))
        target_cursor.execute(sql)
        target_conn.commit()

        sql = "select id from reply_category where company_id = '{}' and name='{}'".format(company_id, record['groupName'])
        target_cursor.execute(sql)
        category_id = target_cursor.fetchone()
        print category_id

        for i in record['contents']:

            sql = "INSERT INTO reply (user_id, content, content_type, create_time, type, category_id, company_id, " \
                  "priority, top) VALUES ('{}', '{}', '0', '1535805271'," \
                  " '1', '{}', '{}', '1', '0');".format(created_by, i['content'], category_id['id'], company_id )
            target_cursor.execute(sql)
            target_conn.commit()
    print '========================reply done==============================='
    target_conn.close()
    mongo_client.close()
    return 'done'
if __name__ == '__main__':

    _company_id = 151
    _created_by = 1546
    _user_id = '05cb046d-a67f-4556-adf6-48efe7f1094d'
    run(company_id=_company_id, created_by=_created_by, user_id=_user_id)

    print '--------------------------end---------------------------------'



