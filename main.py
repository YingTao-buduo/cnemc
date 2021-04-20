from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import requests
import re
import json
import pymysql
import time


# 输出时间
def getData():
    config = {
        'host': '1.14.132.157',
        'user': "yubowei",
        'password': "ybw123==",
        'db': "cnemc",
        'charset': 'utf8'
    }
    db = pymysql.connect(**config)
    cursor = db.cursor()
    try:
        url = 'http://106.37.208.243:8068/GJZ/Ajax/Publish.ashx'
        body = 'PageIndex=1&PageSize=2000&action=getRealDatas'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'ContentLength': str(len(body))
        }
        response = requests.post(url=url, data=body, headers=headers)
        json_str = response.text

        json_object = json.loads(json_str)

        real_data = [None] * 17
        for t in json_object['tbody']:
            count = 0
            try:
                for tt in t:
                    if '<' in tt and '>' in tt:
                        real_data[count] = re.findall('>.*?<', tt, re.S)[0][1:-1]
                    else:
                        real_data[count] = tt
                        if count == 3:
                            real_data[count] = str(list(time.localtime())[0]) + '-' + str(real_data[count]) + ':00'
                    count += 1
                print(real_data)
                sql = "INSERT IGNORE INTO cnemc_data VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', " \
                      "\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')" % \
                      (real_data[0], real_data[1], real_data[2], real_data[3], real_data[4], real_data[5], real_data[6],
                       real_data[7], real_data[8], real_data[9], real_data[10], real_data[11], real_data[12], real_data[13],
                       real_data[14], real_data[15], real_data[16])
                cursor.execute(sql)
                db.commit()
            except Exception as e:
                print(e)

        sql = 'INSERT INTO log VALUES(\'' + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))) + \
              '\', \'succeeded\', ' + str(len(json_object['tbody'])) + ')'
        print(sql)
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        print(e)
        sql = 'INSERT INTO log VALUES(\'' + str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))) + \
              '\', \'failed\', 0)'
        print(sql)
        cursor.execute(sql)
        db.commit()
    db.close()


# BlockingScheduler
scheduler = BlockingScheduler()
# scheduler.add_job(getData, 'interval', hours=1)
scheduler.add_job(getData, 'interval', seconds=10)
scheduler.start()
