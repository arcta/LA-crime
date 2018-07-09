import os
import csv

from pylocal import mysql as db

"""
drop table if exists crime_recent;
create table crime_recent(
    id varchar(20) not null default '000-00000-0000',
    date datetime,
    category varchar(50),
    stat smallint default 0,
    street varchar(100),
    city varchar(50),
    zip char(5),
    lat float,
    lng float,
    primary key(id))
"""

########################################################################
###  daily sync runs on cron  ##########################################

home = os.getenv('HOME')
project = '%s/projects/LA-crime' % home

def main():
    mysql = db.MySQL(__file__)

    with open('%s/data/CURRENT.csv' % project, 'r', encoding='utf-8') as source:
        reader = csv.DictReader(source, delimiter=',', quotechar='"')
        for next in reader:
            if next['city'] != '':
                mysql.execute("""
                insert into crime_recent(id, date, category, stat, street, city, zip)
                values('{}','{}','{}',{},'{}','{}','{}')
                on duplicate key update
                    date = values(date),
                    category = values(category),
                    stat = values(stat),
                    street = values(street),
                    city = values(city),
                    zip = values(zip)
                """.format(next['incident_id'],
                           next['incident_date'],
                           next['category'],
                           next['stat'],
                           mysql.escape(next['address']).decode('utf-8'),
                           mysql.escape(next['city']).decode('utf-8'),
                           next['zip']))
        source.close()


if __name__ == '__main__':
    main()
