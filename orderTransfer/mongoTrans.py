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
    conn = DBConnecter().get_mysql_conn(conf=DBConnecter().wx_db_conf)
    cursor = conn.cursor()
    order_conn = DBConnecter().get_mysql_conn(conf=DBConnecter().order_db_conf)
    order_cursor = order_conn.cursor()

    sql = 'set NAMES utf8mb4;'
    target_cursor.execute(sql)
    mongo_debug_manager = DBConnecter()
    mongo_client = mongo_debug_manager.get_aliyun_mongo_client()
    data = mongo_client["51zan"]["chatTag"].find({"userId": user_id})

    tag_dict = {}
    print 'begin ----------------'
    for record in data:
        if record['tags']:
            for tag in record['tags']:
                if tag_dict.has_key(tag):
                    tag_dict[tag] += 1
                else:
                    tag_dict[tag] = 1
        if not record.has_key('phone'):
            continue
        for _uin in record['uins']:
            try:
                uin = ctypes.c_int32(int(_uin)).value
                sql = "select * from friend where remark_name='{}' and uin='{}'".format(
                    record['remarkName'], str(uin))
                cursor.execute(sql)
                wx_uin_data = cursor.fetchall()
                tags = ''

                if record['tags']:
                    for i in record['tags']:
                        tags += i.encode("utf-8")+','
                if not wx_uin_data:
                    ef = open('noRemarkName.txt', 'a+')
                    ef.write('{uin:' + str(uin) + ', remarkname:' + str(record['remarkName']) + '}\n')
                    continue
                for i in wx_uin_data:
                    sql = "select username from wechat where uin='{}'".format(uin)
                    cursor.execute(sql)
                    service_wx_id = cursor.fetchall()[0]['username']
                    sql = "insert into customer (name,name_pinyin_initial,name_pinyin_full,company_name,birth_year," \
                          "birth_month,birth_day,mobile,gender,qq,email,wx_id,service_wx_id, company_id, country," \
                          "province,city,remark,zodiac,tags,created_by,updated_by,create_time,update_time,is_delete," \
                          "delete_time,create_date_id) values('{}','','','',0,0,0,'','{}','','','{}','{}','{}','',''," \
                          "'','{}',0,'%s','{}','{}','{}','{}',0,0,0" \
                          ")".format(a(str(i['nickname'])), b(i['sex']),
                                     a(i['username']), a(service_wx_id),
                                     str(company_id), a(i['remark_name']), str(created_by), str(created_by),
                                     str(i['create_time']), str(i['update_time']), ) % (tags,)
                    target_cursor.execute(sql)
                    target_conn.commit()
                    sql = "select buyer_nick from t_tb_order where user_id = '{}' and receiver_mobile = '{}'".format(
                        str(user_id), str(record['phone']))
                    order_cursor.execute(sql)
                    platform_data = order_cursor.fetchone()
                    sql = "select id from customer where remark='{}' and company_id='{}'".format(
                        record['remarkName'], str(company_id))
                    target_cursor.execute(sql)
                    customer_id = target_cursor.fetchone()
                    if not platform_data:
                        ef = open('noBuyerNick.txt', 'a+')
                        ef.write(str(record) + '\n')
                        continue
                    sql = "insert into member(customer_id, platform, platform_user_id, created_by, create_time, " \
                          "company_id,remark) values('{}',1,'{}','{}','{}',{},''" \
                          ")".format(customer_id['id'], a(platform_data['buyer_nick']), str(created_by),
                                     str(i['create_time']), company_id, )
                    target_cursor.execute(sql)
                    target_conn.commit()

            except Exception as e:
                ef = open('error.txt', 'a+')
                ef.write(str(e) + str(record) + '\n')
    print '========================customer member done==============================='

    for k in tag_dict.keys():
        try:
            sql = "insert into tag (name, create_time, update_time, is_delete,delete_time,company_id,times) values('"\
                  + str(k) + "',1537164779,0,0,0,'" + str(company_id) + "','" + str(tag_dict[k]) + "')"
            target_cursor.execute(sql)
            target_conn.commit()
        except Exception as e:
            ef = open('insertTagErrorFromMongo.txt', 'a+')
            ef.write('{tag:' + str(k) + ', error:' + str(e) + '}\n')
    print '========================tag done==============================='

    target_conn.close()
    order_conn.close()
    conn.close()
    mongo_client.close()
    return 'done'

if __name__ == '__main__':

    _company_id = 151
    _created_by = 1546
    _user_id = '05cb046d-a67f-4556-adf6-48efe7f1094d'
    run(company_id=_company_id, created_by=_created_by, user_id=_user_id)

    print '--------------------------end---------------------------------'



