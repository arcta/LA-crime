#(.env)python

import os
import re
import sys
import csv
import datetime
import nltk

import filters as app

from nltk.corpus import wordnet as wn

path = '/home/arctalex/projects'
sys.path.append(path)
from utilities import mysql as db

########################################################################
### initial download: one time job #####################################

def main():
    '''
    This archive is not a BIG data,
    textual fields are relatively small and semi-structured,
    store in Mongo plus some stats in MySQL
    '''
    mysql = db.MySQL(__file__)

    filters = app.AppFilters()
    categories = filters.cat_filter()

    ####################################################################
    ###  two logs generated for validation and statistics  #############

    status = open('data/audit-log/missing-coord.csv', 'wb')
    status.write('year,%s\n' % ','.join(categories))

    counts = open('data/audit-log/archive-stats.csv', 'wb')
    counts.write('year,%s\n' % ','.join(categories))

    mysql.query("""
    drop table if exists crime
    """)
    mysql.query("""
    create table crime(
        id varchar(20) not null default '000-00000-0000',
        date datetime,
        category varchar(50),
        street varchar(100),
        city varchar(50),
        zip char(5),
        lat float,
        lng float,
        gang bool,
        primary key(id)
    )
    """)
    mysql.execute("""
    drop table if exists crime_dictionary
    """)
    mysql.execute("""
    create table crime_dictionary (
        term varchar(100) not null default '',
        category varchar(50),
        word varchar(100),
        total int(11) default 0,
        primary key (term, category)
    )
    """)
    mysql.execute("""
    drop table if exists crime_wordnet
    """)
    mysql.execute("""
    create table crime_wordnet (
        term1 varchar(100) not null default '',
        term2 varchar(100) not null default '',
        category varchar(50),
        word1 varchar(100),
        word2 varchar(100),
        total int(11) default 0,
        primary key (term1, term2, category)
    )
    """)

    stopwords = nltk.corpus.stopwords.words('english')
    stemmer = nltk.stem.porter.PorterStemmer()

    id = 1
    for Y in range(2005,datetime.date.today().year):
        with open('data/%d.csv' % Y) as source:
            with open('data/%d.json' % Y, 'wb') as output:
                reader = csv.DictReader(source, delimiter=',', quotechar='"')
                missed_location = {}
                total = {}
                for next in reader:
                    cat = filters.cat_filter(next['category'])
                    if False != cat:
                        words = re.split(r'\W+', next['stat_desc'])
                        links = []
                        dict_insert = []
                        link_insert = []
                        for word in words:
                            if len(word) > 1 and not word.isdigit() and word not in cat.split() and word not in stopwords:
                                term = stemmer.stem_word(word)
                                links.append([term,word])
                                dict_insert.append("('%s', '%s', '%s', 1)" % (term, cat, mysql.escape(word)))

                        for i in range(len(links)):
                            for j in range(i+1,len(links)):
                                link_insert.append("('%s', '%s', '%s', '%s', '%s', 1)" %\
                                    (links[i][0], links[j][0], cat, mysql.escape(links[i][1]), mysql.escape(links[j][1])))

                        if len(dict_insert) > 0:
                            mysql.insert("""
                            insert into crime_dictionary(term, category, word, total)
                            values %s on duplicate key update total = crime_dictionary.total + 1
                            """, dict_insert)

                        if len(link_insert) > 0:
                            mysql.insert("""
                            insert into crime_wordnet(term1, term2, category, word1, word2, total)
                            values %s on duplicate key update total = crime_wordnet.total + 1
                            """, link_insert)

                        if cat not in total:
                            total[cat] = 0.0
                            missed_location[cat] = 0.0
                        total[cat] += 1
                        date = filters.date_filter(str(Y), next['incident_date'])
                        if False != date:
                            location = filters.location_filter(next)
                            if False != location:
                                loc = '[%.4f,%.4f]' % (location['longitude'], location['latitude'])
                                desc = filters.desc_filter(next)
                                gang = filters.gang_filter(next['gang_related'])
                                jsonstr = '{"year":%d,"month":%s,"day":%s,"hour":%.2f,"cat":"%s","desc":"%s","address":"%s","city":"%s","zip":"%s","gang":%d,"loc":%s}' %\
                                    (date['year'], date['month'], date['day'], date['hour'], cat, mysql.escape(desc), mysql.escape(location['addr']), mysql.escape(location['city']), location['zip'], gang, loc)
                                output.write('%s\n' % jsonstr)
                                mysql.execute("""
                                    insert into crime(id, date, category, street, city, zip, lat, lng, gang)
                                    values('%s','%s','%s','%s','%s','%s',%.4f,%.4f,%s)
                                """ % (next['incident_id'], date['date'], cat, mysql.escape(location['addr']), mysql.escape(location['city']), location['zip'], location['latitude'], location['longitude'], gang))
                                id +=1
                            else:
                                missed_location[cat] += 1
                output.close()
            source.close()

        stats1 = []
        stats2 = []
        for cat in categories:
            stats1.append('%s' % round(100 * missed_location[cat]/total[cat]))
            stats2.append('%s' % total[cat])
        status.write('%d,%s\n' % (Y, ','.join(stats1)))
        counts.write('%d,%s\n' % (Y, ','.join(stats2)))
        print '%d Complete. Next ID: %d' % (Y, id)

    status.close()
    counts.close()


if __name__ == '__main__':
    main()
