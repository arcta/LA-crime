import os
import re
import sys
import csv
import datetime
import nltk

from nltk.corpus import wordnet as wn

home = os.getenv('HOME')
project = '%s/projects/LA-crime' % home

from pylocal import mysql as db

########################################################################
### load data preprocessed by audit ####################################

def main():
    """Run as python archive-import.py to load all or python archive-import.py <Y1> <Y2> to update the range"""
    mysql = db.MySQL(__file__)
    
    Y1, Y2 = 2005, 2017 ### range with coordinates supplied in pre-2018 generated archive
    UPDATE = False
    
    if len(sys.argv) > 1 and int(sys.argv[1]) > 0:
        UPDATE = True
        Y1 = int(sys.argv[1])

        if len(sys.argv) > 2 and int(sys.argv[2]) > Y1:
            Y2 = int(sys.argv[2])

        mysql.query("""
        delete from crime WHERE YEAR(date) BETWEEN {} AND {}
        """.format(Y1, Y2))            
            
    else:
        mysql.query("""
        drop table if exists crime
        """)
        mysql.query("""
        create table crime(
            id varchar(20) not null default '000-00000-0000',
            date datetime,
            category varchar(50),
            stat smallint,
            city varchar(50),
            zip char(5),
            lat float,
            lng float,
            gang bool,
            primary key(id)
        )
        """)
        
    mysql.execute("""
    drop table if exists crime_taxonomy
    """)
    mysql.execute("""
    create table crime_taxonomy (
        stat smallint not null default 0,
        description varchar(250),
        category varchar(50),
        criminal tinyint(1) default 1,
        misdemeanor tinyint(1) default 0,
        violent tinyint(1) default 0,
        weapon tinyint(1) default 0,
        gun tinyint(1) default 0,
        primary key (stat)
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
    
    with open('{}/data/taxonomy.csv'.format(project)) as source:
        reader = csv.DictReader(source, delimiter=',', quotechar='"')
        values = []
        taxonomy = {}
        for next in reader:
            taxonomy[next['stat']] = ' '.join(re.split(r'\W+', str(next['description']).strip()))
            values.append("({},'{}','{}')".format(next['stat'], taxonomy[next['stat']], next['category']))
        mysql.insert("insert into crime_taxonomy(stat, description, category) values %s", values)
        source.close()

    stopwords = nltk.corpus.stopwords.words('english')
    stemmer = nltk.stem.porter.PorterStemmer()

    for Y in range(Y1, Y2):
        with open('{}/data/F{}.csv'.format(project, Y)) as source:
            reader = csv.DictReader(source, delimiter=',', quotechar='"')
            count = 0
            skipped = 0
            for next in reader:
                count += 1
                cat = next['category']
                stat = next['stat']
                words = taxonomy[next['stat']].split()
                links = []
                dict_insert = []
                link_insert = []
                for word in words:
                    if len(word) > 1 and not word.isdigit() and word not in cat.split() and word not in stopwords:
                        term = stemmer.stem(word)
                        links.append([term, word])
                        dict_insert.append("('{}','{}','{}', 1)".format(term, cat, word))

                for i in range(len(links)):
                    for j in range(i+1,len(links)):
                        link_insert.append("('{}','{}','{}','{}','{}', 1)".format(
                            links[i][0], links[j][0], cat, links[i][1], links[j][1]))

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

                if cat != '':
                    year, day, month = next['date'].split('-')
                    h = int(float(next['hour']))
                    m = int(60 * (float(next['hour']) - h))
                    date = '{} {:02d}:{:02d}:00'.format(next['date'], h, m)
                    if next['lat'] != '' or next['zip'] != '':
                        loc = '{:.4f},{:.4f}'.format(float(next['lng']), float(next['lat'])) \
                                        if next['lat'] != '' else 'null,null'
                        gang = next['gang']
                        mysql.execute("""
                        insert into crime(id, date, category, stat, city, zip, lng, lat, gang)
                        values('{}','{}','{}',{},'{}','{}',{},{})
                        on duplicate key update
                            date = if(values(date) is null, date, values(date)),
                            category = if(values(category) is null, category, values(category)),
                            stat = if(values(stat) is null, stat, values(stat)),
                            city = if(values(city) is null, city, values(city)),
                            zip = if(values(zip) is null, zip, values(zip)),
                            lng = if(values(lng) is null, lng, values(lng)),
                            lat = if(values(lat) is null, lat, values(lat)),
                            gang = if(values(gang) is null, gang, values(gang))
                        """.format(next['id'], date, cat, next['stat'], next['city'], next['zip'], loc, gang))
                    else:
                        skipped += 1
                else:
                    skipped += 1
            source.close()
        print('{} complete: skipped {:.2f}% total: {}'.format(Y, 100 * skipped/count, count))


if __name__ == '__main__':
    main()
