#(.env)python

import os
import re
import json
import csv
import sys
import datetime
import calendar
import random
import pyproj

sys.path.append('/home/arctalex/projects')
from utilities import mysql as db


########################################################################
###  based on the result of wrangling  #################################

class AppFilters(db.MySQL):

    def __init__(self):
        db.MySQL.__init__(self,__file__)



    def __del__(self):
        db.MySQL.__del__(self)



    def cat_filter(self, cat=None):
        '''
        based on data/category-field-audit-log.tsv
        common denominator with importance of location

        parameter cat: string categoty field from source file
        return: matching local category name or False if no match;

        if no parameter passed: returns local categories list
        '''
        categories = ['aggravated assault',
                      'arson',
                      'burglary',
                      'criminal homicide',
                      'drunk driving',
                      'felonies',
                      'forcible rape',
                      'forgery',
                      'fraud',
                      'grand theft auto',
                      'larceny theft',
                      'narcotics',
                      'non-aggravated assault',
                      'receiving stolen',
                      'robbery',
                      'sex offenses',
                      'vandalism',
                      'weapon laws']
        if None == cat:
            return categories

        key = False
        for name in categories:
            if cat[:len(name)] == name: key = name
        return key



    def desc_filter(self, fields):
        '''
        based on data/decription-field-audit-log.tsv

        PARAMETER fields: dictionary (labeled line of values from source csv)
        RETURN: string (stemmed stat_desc field)
        '''
        return fields['stat_desc'].split(':').pop().strip()


    def term_filter(self):
        """
        delete from crime_dictionary where term1 in ('ii','iii','fel','etc','veh','via','non','agg','adw','mkt','oz','less','grand');
        delete from crime_dictionary where term2 in ('ii','iii','fel','etc','veh','via','non','agg','adw','mkt','oz','less','grand');
        delete from crime_dictionary where word1 = 'vehicle' and category ='grand theft auto';
        delete from crime_dictionary where word2 = 'vehicle' and category ='grand theft auto';
        delete from crime_dictionary where word1 = 'felony' and category ='felonies';
        delete from crime_dictionary where word2 = 'felony' and category ='felonies';
        delete from crime_dictionary where word1 like 'auto%' or word2 like 'auto%';
        """
        pass



    def gang_filter(self, field):
        '''
        based on data/gang-related-field-audit-log.tsv

        PARAMETER field: string gang_related field from source csv
        RETURN int: 1/0 map of yes/no
        '''
        return 1 if str == 'yes' else 0



    def date_filter(self, file, field):
        '''
        based on data/date-field-audit-log.tsv

        PARAMETER field: string incident_date or incident_reported_date field from source csv
        RETURN dictionary: year, month, day integer id and decimal float in military (0-24) hour value
               if time is not available hour value is -1.0
        '''
        match = re.match('(\d+)/(\d+)/(\d+)(\s(\d+):(\d+):(\d+)(\s(am|pm))?)?', field)
        if None != match:
            month = int(match.group(1))
            day = int(match.group(2))
            year = int(file) if match.group(3)[2:] == file[2:] else int(match.group(3))
            hour = 0
            if (year == int(file)) and (month > 0 and month < 13) and (day > 0 and day <= calendar.monthrange(year,month)[1]):
                if None != match.group(4):
                    hour = float(match.group(5)) + float(match.group(6))/60
                    if None != match.group(8):
                        hour += (12 if match.group(9) == 'pm' else 0)
                    if hour >= 24: hour = -1.0
                else: hour = -1.0
                return {'year':year, 'month':month, 'day':day, 'hour':round(100*hour)/100, 'date':datetime.datetime(year, month, day, (int(hour) if hour > -1 else 0), (int(match.group(6)) if hour > -1 else 0), 0).isoformat()}

        else:
            ms = ('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec');
            match = re.match('(\d+)-([a-z]+)-(\d+)(\s(\d+):(\d+):(\d+))?', field)
            if None != match:
                month = ms.index(match.group(2)) +1
                day = int(match.group(1))
                year = int(file) if match.group(3) == file[2:] else int(match.group(3))
                hour = 0
                if (year == int(file)) and (month > 0 and month < 13) and (day > 0 and day <= calendar.monthrange(year,month)[1]):
                    if None != match.group(4):
                        hour = float(match.group(5)) + float(match.group(6))/60
                        if hour >= 24: hour = -1.0
                    else: hour = -1.0
                    return {'year':year, 'month':month, 'day':day, 'hour':round(100*hour)/100, 'date':datetime.datetime(year, month, day, (int(hour) if hour > -1 else 0), (int(match.group(6)) if hour > -1 else 0), 0).isoformat()}

        return False



    def coords_filter(self, x,y):
        '''
        using GDAL lib to translate original State Plane EPSG 4326
        to latitude and longitude for Google maps

        PARAMETER x: float coordinate_x in meters from source csv
        PARAMETER y: float coordinate_y in meters from source csv
        RETURN: dictionary (labeled pair latitude, longitude)
        '''
        projection = pyproj.Proj(r'+proj=lcc +lat_1=34.03333333333333 +lat_2=35.46666666666667 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000 +ellps=GRS80 +units=m +no_defs')
        coords = projection(x, y, inverse=True)
        if str(coords[0])[:2] == '-1' and str(coords[1])[0] == '3':
            return {'longitude': coords[0], 'latitude': coords[1]}
        return False



    def location_filter(self, fields):
        '''
        based on data/location-audit-log.tsv
        the application we are serving is location based, so,
        we try to validate/recover location if possible

        PARAMETER fields: dictionary (labeled line of values from source csv)
        RETURN: dictionary (city string, zip string, longitude float, latitude float, state plane float coords in meters, street addres string)
        '''
        data = db.MySQL(__file__)
        addr = re.sub('"','',fields['address'].upper())
        if fields['x_coordinate'] != '' and fields['y_coordinate'] != '':
            x = float(fields['x_coordinate']) * 0.3048
            y = float(fields['y_coordinate']) * 0.3048
            coords = self.coords_filter(x, y)
            if False != coords:
                find = data.query("""
                    select t.geoid as zip, z.primary_city
                    from us_zcta5 t, usps_zcta5 z
                    where st_contains(t.shape, point(%.6f, %.6f)) and t.geoid = z.zip
                    """ % (coords['longitude'], coords['latitude']))
                for res in find:
                    return {'city': res['primary_city'].upper(), 'zip':res['zip'], 'longitude':coords['longitude'], 'latitude':coords['latitude'], 'x':x, 'y':y, 'addr':addr}
            else:
                find = data.query("""
                    select longitude, latitude from usps_zcta5
                    where zip = '%s' and (primary_city = '%s' or acceptable_cities like '%%%s%%') and county = 'Los Angeles County'
                    """ % (fields['zip'], data.escape(fields['city']), data.escape(fields['city'])))
                if find and len(find) == 1:
                    return {'city': fields['city'].upper(), 'zip':fields['zip'], 'longitude':find[0]['longitude'], 'latitude':find[0]['latitude'], 'x':0, 'y':0, 'addr':addr}
        else:
            if None != re.match('9\d\d\d\d', fields['zip']):
                check = data.query("""
                    select primary_city, longitude, latitude from usps_zcta5
                    where zip = '%s' and county = 'Los Angeles County'
                    """ % fields['zip'])
                if check and len(check) == 1 and check[0]['primary_city'].lower() != fields['city']:
                    return {'city': check[0]['primary_city'].upper(), 'zip':fields['zip'], 'longitude':check[0]['longitude'], 'latitude':check[0]['latitude'], 'x':0, 'y':0, 'addr':addr}
            elif '' != fields['city']:
                find = data.query("""
                    select zip, longitude, latitude from usps_zcta5
                    where (primary_city = '%s' or acceptable_cities like '%%%s%%') and county = 'Los Angeles County'
                    """ % (data.escape(fields['city']), data.escape(fields['city'])))
                if find and len(find) == 1:
                    return {'city': fields['city'].upper(), 'zip':find[0]['zip'], 'longitude':find[0]['longitude'], 'latitude':find[0]['latitude'], 'x':0, 'y':0, 'addr':addr}
        return False



def main():
    '''
    filters tests
    '''
    proc = AppFilters()
    curr = datetime.datetime.now().year -1

    proc.timer('Categorizaton test')
    passed = True
    with open('data/category-field-audit-log.tsv') as source:
        for line in source:
            fields = line.strip().split('\t')
            year = fields[0]
            categories = []
            for i in range(1,len(fields)):
                category = proc.cat_filter(fields[i].split(':')[0])
                if False != category: categories.append(category)
            result = set(categories)
            N = len(proc.cat_filter())
            if len(result) != N: passed = False
            print year, (len(result) == N)
    print '================================================================= %s' % 'PASSED' if passed else 'FAILED'
    proc.timer()

    proc.timer('Description normalization test')
    passed = True
    for i in range(5):
        with open('data/decription-audit-log.tsv') as tsv:
            years = range(2005,curr)
            random.shuffle(years)
            desc = []
            for line in tsv:
                fields = line.strip().split('\t')
                if int(fields[0]) == years[0]: desc.append(fields.pop())

            true = 0.0
            false = 0.0
            with open('data/%s.csv' % years[1]) as source:
                reader = csv.DictReader(source, delimiter=',', quotechar='"')
                for next in reader:
                    if proc.desc_filter(next) not in desc: false += 1
                    else: true += 1
                norm = 100/(true + false)
                if false*norm >= 0.01: passed = False
                print 'LOG: %s VALUES: %s MATCH: %.4f%%  MISSED: %.4f%%' % (years[0], years[1], true*norm, false*norm)
    print '================================================================= %s' % 'PASSED' if passed else 'FAILED'
    proc.timer()

    proc.timer('Date parsing test')
    passed = True
    for i in range(5):
        year = random.randint(2005,curr)
        test = '%s/%s/%s %s:%s:%s am' % (random.randint(1,12), random.randint(1,28),random.randint(2005,curr), random.randint(0,11), random.randint(0,59), random.randint(0,59))
        date = proc.date_filter('%s' % year, test)
        res = (False == date or date['year'] == year)
        if False == res: passed = false
        print year, test, date, res

    for i in range(5):
        year = random.randint(2005,curr)
        test = '%s/%s/%s %s:%s:%s' % (random.randint(1,12), random.randint(1,28),random.randint(2005,curr), random.randint(0,100), random.randint(0,59), random.randint(0,59))
        date = proc.date_filter('%s' % year, test)
        res = (False == date or (date['year'] == year and (date['hour'] == -1.0  or date['hour'] < 24)))
        if False == res: passed = false
        print year, test, date, res

    test = '%s/%s/2009 %s:%s:%s pm' % (random.randint(1,12), random.randint(1,28), random.randint(0,11), random.randint(0,59), random.randint(0,59))
    date = proc.date_filter('2009', test)
    res = (False == date or (date['hour'] == -1.0  or date['hour'] < 24))
    if False == res: passed = false
    print 2009, test, date, res

    test = '2/31/%d' % curr
    date = proc.date_filter(str(curr), '2/31/%d' % curr)
    res = (False == date)
    if True != res: passed = False
    print curr, test, date, res
    print '================================================================= %s' % 'PASSED' if passed else 'FAILED'
    proc.timer()

    proc.timer('Coordinates conversion test')
    passed = True
    print '\n=== Coordinates conversion test: expect longitude -120 and latitude 34 ='
    test = proc.coords_filter(1815241.25377291, 557301.336190851)
    res = (round(test['longitude']) == -120 and round(test['latitude']) == 34)
    if True != res: passed = False
    print test, res
    print '================================================================= %s' % 'PASSED' if passed else 'FAILED'
    proc.timer()

    proc.timer('Location parsing test')
    passed = True
    data = db.MySQL()

    print '100% match'
    true = data.query("""
        select primary_city, zip, longitude, latitude from usps_zcta5
        where county = 'Los Angeles County'
        order by rand()
        limit 10
        """)
    for case in true:
        fields = case
        fields['address'] = 'test'
        projection = pyproj.Proj(r'+proj=lcc +latitude_1=34.03333333333333 +latitude_2=35.46666666666667 +latitude_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000 +ellps=GRS80 +units=m +no_defs')
        coords = projection(case['longitude'], case['latitude'])
        fields['x_coordinate'] = coords[0]/0.3048
        fields['y_coordinate'] = coords[1]/0.3048
        test = proc.location_filter(fields)
        print test
        if False == test: passed = False

    print '\nrecovered through zip/city with bad coordinates'
    true = data.query("""
        select primary_city, zip, longitude, latitude from usps_zcta5
        where county = 'Los Angeles County'
        order by rand()
        limit 10
        """)
    for case in true:
        fields = case
        fields['address'] = 'test'
        projection = pyproj.Proj(init='epsg:3734')
        coords = projection(case['longitude'], case['latitude'])
        fields['x_coordinate'] = coords[0]/0.3048
        fields['y_coordinate'] = coords[1]/0.3048
        test = proc.location_filter(fields)
        print test
        if False == test: passed = False

    print '\nrecovered through zip/city with missing coordinates'
    true = data.query("""
        select primary_city, zip, longitude, latitude from usps_zcta5
        where county = 'Los Angeles County'
        order by rand()
        limit 10
        """)
    for case in true:
        fields = case
        fields['address'] = 'test'
        fields['x_coordinate'] = ''
        fields['y_coordinate'] = ''
        test = proc.location_filter(fields)
        print test
        if False == test: passed = False

    print '\nrecovered through zip only'
    true = data.query("""
        select primary_city, zip, longitude, latitude from usps_zcta5
        where county = 'Los Angeles County'
        order by rand()
        limit 10
        """)
    for case in true:
        fields = case
        fields['city'] = ''
        fields['address'] = 'test'
        fields['x_coordinate'] = ''
        fields['y_coordinate'] = ''
        test = proc.location_filter(fields)
        print test
        if False == test: passed = False

    print '\nrecovered through city only'
    true = data.query("""
        select primary_city, zip, count(1) from usps_zcta5
        where county = 'Los Angeles County'
        group by primary_city
        having count(1) = 1
        order by rand()
        limit 10
        """)
    for case in true:
        fields = case
        fields['zip'] = ''
        fields['address'] = 'test'
        fields['x_coordinate'] = ''
        fields['y_coordinate'] = ''
        test = proc.location_filter(fields)
        print test
        if False == test: passed = False

    print '================================================================= %s' % 'PASSED' if passed else 'FAILED'
    proc.timer()



if __name__ == '__main__':
    main()
