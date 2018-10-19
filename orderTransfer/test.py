# from __future__ import unicode_literals
import sys
import time
from mongoTrans import DBConnecter


def run(company_id, created_by, user_id):

    now = int(time.time())
    conn = DBConnecter().get_mysql_conn(conf=DBConnecter().old_prod_conf)
    cursor = conn.cursor()
    target_conn = DBConnecter().get_mysql_conn(conf=DBConnecter().target_conf)
    target_cursor = target_conn.cursor()

    sql = 'set NAMES utf8mb4;'
    target_cursor.execute(sql)
    sql = "select bind_time,owner_wx_id,wx_id,wx_number,nick_name,remark_name,sex,real_name,phone,address,qq,email," \
          "remark,create_time,update_time,age,birthday,platform,platform_user_id,status,receive_addr " \
          "from t_wx_customer where user_id='{}'".format(user_id)
    cursor.execute(sql)
    data = cursor.fetchall()
    error_wx_id = []
    for i in data:
        try:
            sql = "insert into customer (name,name_pinyin_initial,name_pinyin_full,company_name,birth_year," \
                  "birth_month,birth_day,mobile,gender,qq,email,wx_id,service_wx_id, company_id, country," \
                  "province,city,remark,zodiac,tags,created_by,updated_by,create_time,update_time,is_delete," \
                  "delete_time,create_date_id) values('{}','','','',0,0,0,'','{}','{}','{}','{}','{}','{}','',''" \
                  ",'','{}',0,'','{}',0,'{}','{}',0,0,0" \
                  ")".format(a(str(i['nick_name'])), b(i['sex']), a(i['qq']), a(i['email']), a(i['wx_id']),
                             a(i['owner_wx_id']), str(company_id), a(i['remark_name']), str(created_by), now, now,)
            target_cursor.execute(sql)
            target_conn.commit()
            sql = "select GROUP_CONCAT(name) tag, wx_id  from t_wx_customer_tag where user_id='{}'and wx_id " \
                  "= '{}' group by wx_id".format(str(user_id),str(i['wx_id']))
            cursor.execute(sql)
            customer_tag_data = cursor.fetchall()
            if customer_tag_data:
                customer_tag_data = customer_tag_data[0]
                sql = "update customer set tags = '{}' where company_id='{}'and wx_id = '{}'".format(customer_tag_data['tag'], str(company_id), str(i['wx_id']))
                target_cursor.execute(sql)
                target_conn.commit()
        except Exception as e:
            ef = open('insertCustomerError.txt', 'a+')
            ef.write('{wx_id:' + str(i['wx_id']) + ', error:' + str(e) + '}\n')
            error_wx_id.append(i['wx_id'])
    ef = open('insertCustomerError.txt', 'a+')
    ef.write('{error_wx_id:' + str(error_wx_id) + '\n')
    print '------------------------customer all done----------------------------'
    sql = "select bind_time,owner_wx_id,wx_id,wx_number,nick_name,remark_name,sex,real_name,phone,address,qq," \
          "email,remark,create_time,update_time,age,birthday,platform,platform_user_id,status,receive_addr " \
          "from t_wx_customer where user_id='{}' and platform_user_id is not null".format(user_id)
    cursor.execute(sql)
    data = cursor.fetchall()
    for i in data:
        try:
            sql = "select * from customer where company_id='{}'and wx_id = '{}'".format(company_id,
                                                                                        a(i['wx_id']))
            target_cursor.execute(sql)
            platform_data = target_cursor.fetchall()[0]
            sql = "insert into member(customer_id, platform, platform_user_id, created_by, create_time, " \
                  "company_id,remark) values('{}',1,'{}','{}','{}',{},'')".format(
                  platform_data['id'], a(i['platform_user_id']), str(created_by), now, company_id, )
            target_cursor.execute(sql)
            target_conn.commit()

        except Exception as e:
            ef = open('insertMemberError.txt', 'a+')
            ef.write('{tag:' + str(i) + ', error:' + str(e) + '}\n')

    sql = "select name,count(name) times from t_wx_customer_tag where user_id='{}' group by name;".format(user_id)
    cursor.execute(sql)
    tags_data = cursor.fetchall()
    for i in tags_data:
        try:
            sql = "insert into tag (name, create_time, update_time, is_delete,delete_time,company_id,times) values('"+ str(i['name'])+"',0,0,0,0,'" + str(company_id)+"','" + str(i['times'])+"')"
            target_cursor.execute(sql)
            target_conn.commit()
        except Exception as e:
            ef = open('insertTagError.txt', 'a+')
            ef.write('{tag:' + str(i) + ', error:' + str(e) + '}\n')


def a(x):
    if x is None:
        x = ''
    return x


def b(x):
    if x is None:
        x = 2
    return x


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    _company_id = 58
    _created_by = 1079
    _user_id = '0e82252f-0c33-4d82-a9c8-45836f86c7e1'
    run(company_id=_company_id, created_by=_created_by, user_id=_user_id)

