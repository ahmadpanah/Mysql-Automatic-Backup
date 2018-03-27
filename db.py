# coding:utf-8
import json
import os
import tarfile
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

dump_cmd = "mysqldump --user={user} --password={password} --skip-lock-tables --host={host} {database} > {file}"

config = json.load(open('config.json', 'r'))['mysql']


def delete_expired():

    files = [f for f in os.listdir(config['path'])]
    for file in files:
        full_path = config['path'] + "/" + file

        is_expired = os.stat(full_path).st_mtime < (time.time() - eval(config['expired']))
        if is_expired:
         
            os.remove(full_path)
    print('[%s] delete expired dumped file(s) success' % get_custom_date())


def dump():


    filename = "{database}-{date}.sql".format(database=config['database'], date=get_custom_date(config['dateFormat']))

    file = "{path}/{filename}".format(path=config['path'], filename=filename)

    os.system(dump_cmd.format(user=config['user'], password=config['password'], host=config['host'],
                              database=config['database'], file=file))
 
    tar = tarfile.open(file[0:len(file) - 3] + 'gz', 'w')
    tar.add(file, arcname=filename)
    tar.close()

    os.remove(file)
    print('[%s] dump success' % get_custom_date())
    delete_expired()


def get_custom_date(date_format="%Y-%m-%d %H:%M:%S"):
    return datetime.now().strftime(date_format)


if __name__ == "__main__":

    params = config['schedulerParams']
 
    scheduler = BlockingScheduler()
    if params['cron']:
        scheduler.add_job(dump, 'cron', day_of_week=params['dayOfWeek'], hour=params['hour'], minute=params['minute'])
    else:
        scheduler.add_job(dump, 'interval', seconds=params['seconds'])
    print('[%s] starting mysql auto backup' % get_custom_date())
    scheduler.start()
