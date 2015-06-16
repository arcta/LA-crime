#(.env)python

import os
import re
import sys
import subprocess
import datetime
import calendar
import csv
import json
import pyproj
import nltk

import filters as app

from nltk.corpus import wordnet as wn

path = '/home/arctalex/projects'
sys.path.append(path)
from utilities import mysql as db


########################################################################
###  daily sync runs on cron  ##########################################

def main():
    mysql = db.MySQL(__file__)

    filters = app.AppFilters()
    categories = filters.cat_filter()



    ####################################################################
    ###  two logs generated for validation and statistics  #############

    log = open('%s/LA-crime/data/audit-log/current.csv' % path, 'wb')
    log.write('test,%s\n' % ','.join(categories))

    """
    drop table if exists crime_recent;
    create table crime_recent(
        id varchar(20) not null default '000-00000-0000',
        date datetime,
        category varchar(50),
        street varchar(100),
        city varchar(50),
        zip char(5),
        lat float,
        lng float,
        primary key(id)
    );
    """

    with open('%s/LA-crime/data/CURRENT.csv' % path,'r') as source:
        reader = csv.DictReader(source, delimiter=',', quotechar='"')
        missed_location = {}
        total = {}
        for next in reader:
            cat = filters.cat_filter(next['category'])
            if False != cat:
                if cat not in total:
                    total[cat] = 0.0
                    missed_location[cat] = 0.0
                total[cat] += 1
                location = filters.location_filter(next)
                if False != location:
                    loc = '[%.4f,%.4f]' % (location['longitude'], location['latitude'])
                    desc = filters.desc_filter(next)
                    mysql.execute("""
                    insert into crime_recent(id, date, category, street, city, zip, lat, lng)
                    values('%s','%s','%s','%s','%s','%s',%.4f,%.4f)
                    on duplicate key update
                        date = values(date),
                        category = values(category),
                        street = values(street),
                        city = values(city),
                        zip = values(zip),
                        lat = values(lat),
                        lng = values(lng)
                    """ % (next['lurn_sak'],
                           next['incident_date'],
                           cat,
                           mysql.escape(location['addr']),
                           mysql.escape(location['city']),
                           location['zip'],
                           location['latitude'],
                           location['longitude']))
                else:
                    missed_location[cat] += 1
        source.close()

    stats1 = []
    stats2 = []
    for cat in categories:
        stats1.append('%s' % round(100 * missed_location[cat]/total[cat]))
        stats2.append('%s' % total[cat])
    stamp = datetime.datetime.now().isoformat()
    log.write('missed coords %%,%s\n' % ','.join(stats1))
    log.write('total count,%s\n' % ','.join(stats2))
    log.close()


if __name__ == '__main__':
    main()
