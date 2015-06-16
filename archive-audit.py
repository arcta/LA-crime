#(.env)python

import os
import re
import json
import csv
import sys
import subprocess
import datetime
import calendar
import pyproj

path = '/home/arctalex/projects'
sys.path.append(path)
from utilities import mysql as db


########################################################################
###  data wrangling  ###################################################

class DataAudit(db.MySQL):

    def __init__(self, years=[2005]):
        '''
        data in semi-standardized format available from 2005
        '''
        db.MySQL.__init__(self,__file__)
        self.YEARS = years
        self.HEADER = self.cmd('head -n1 data/%s.csv' % self.YEARS[-1])



    def __del__(self):
        db.MySQL.__del__(self)



    def check_format(self, year):
        '''
        check if the source files are in expected (the same) format

        PARAMETER year str: file label without extention (corresponding year)
        RETURN: integer error count
        '''
        errors = 0
        with open('data/%s.csv' % year) as source:
            reader = csv.reader(source, delimiter=',', quotechar='"')
            titles = reader.next()
            if ','.join(titles) != self.HEADER: return False
            try:
                for next in reader:
                    if len(titles) != len(next): errors += 1
            except csv.Error as e:
                errors += 1
            source.close()
        return errors



    def field_stats(self, year, field):
        '''
        calculates field size statistics

        PARAMETER year: string file label without extention (corresponding year)
        PARAMETER field: strinf field name
        RETURN: string formated field size stats (min, max, mode, set length)
        '''
        stats = {}
        total = 0.0
        with open('data/%s.csv' % year) as source:
            reader = csv.DictReader(source, delimiter=',', quotechar='"')
            for next in reader:
                size = len(next[field])
                if size not in stats: stats[size] = 0.0
                stats[size] += 1
                total += 1
            source.close()
        l = len(stats)
        vals = sorted(stats.keys())
        mode = sorted(stats, key = stats.get).pop()
        return 'min: %s (%.4f%%)\tmax: %s (%.4f%%)\tmode: %s (%.4f%%)\tset: %s' % (\
                    vals[0], 100*stats[vals[0]]/total,
                    vals[-1], 100*stats[vals[-1]]/total,
                    mode, 100*stats[mode]/total, l)



    def audit_field(self, year, field):
        '''
        general field auditing

        PARAMETER year: string file label without extention (corresponding year)
        PARAMETER field: strinf field name
        RETURN: string formated result (value and %occurrence)
        '''
        stats = {}
        total = 0.0
        output = []
        with open('data/%s.csv' % year) as source:
            reader = csv.DictReader(source, delimiter=',', quotechar='"')
            for next in reader:
                if next[field] not in stats: stats[next[field]] = 0.0
                stats[next[field]] += 1
                total += 1
            source.close()
        for key in sorted(stats.keys()): output.append('%s: %.4f%%' % (key, 100*stats[key]/total))
        return '\t'.join(output)



    def audit_category(self):
        '''
        field specific auditing

        RETURN: string formated result (field values co.piled through all the files and %occurrence of each for each)
        '''
        with open('data/audit-log/category.tsv','wb') as log:
            stats = {}
            for year in self.YEARS:
                with open('data/%s.csv' % year) as source:
                    reader = csv.DictReader(source, delimiter=',', quotechar='"')
                    for next in reader:
                        if next['category'] not in stats: stats[next['category']] = 0.0
                    source.close()

            for year in self.YEARS:
                total = 0.0
                output = []
                for key in stats: stats[key] = 0.0
                with open('data/%s.csv' % year) as source:
                    reader = csv.DictReader(source, delimiter=',', quotechar='"')
                    for next in reader:
                        total += 1
                        stats[next['category']] += 1
                    source.close()

                    for key in sorted(stats.keys()): output.append('%s: %.4f%%' % (key, 100*stats[key]/total))
                    log.write('%s\t%s\n' % (year, '\t'.join(output)))
            log.close()
        return len(output)



    def audit_description(self):
        '''
        field specific auditing

        RETURN: string formated result (field values co.piled through all the files and %occurrence of each for each)
        '''
        def desc(fields):
            '''
            candidate for the future filter function
            '''
            return fields['stat_desc'].split(':').pop().strip()

        with open('data/audit-log/decription.tsv','wb') as log:
            stats = {}
            for year in self.YEARS:
                with open('data/%s.csv' % year) as source:
                    reader = csv.DictReader(source, delimiter=',', quotechar='"')
                    for next in reader:
                        description = desc(next)
                        if description not in stats: stats[description] = 0.0
                    source.close()

            for year in self.YEARS:
                total = 0.0
                output = []
                for key in stats: stats[key] = 0.0
                with open('data/%s.csv' % year) as source:
                    reader = csv.DictReader(source, delimiter=',', quotechar='"')
                    for next in reader:
                        description = desc(next)
                        total += 1
                        stats[description] += 1
                    source.close()

                    for key in sorted(stats.keys()): output.append('%s\t%.4f%%\t%s' % (year, 100*stats[key]/total, key))
                    log.write('\n'.join(output))
            log.close()
        return len(output)



    def audit_date(self, year, field):
        '''
        specific date type field auditing

        PARAMETER year: string file label without extention (corresponding year)
        PARAMETER field: strinf field name
        RETURN: string formated result (errors type and %occurrence, values literal and %stats)
        '''
        with open('data/%s.csv' % year) as source:
            reader = csv.DictReader(source, delimiter=',', quotechar='"')
            total = 0.0
            format_error = 0.0
            value_error = 0.0
            stats = {
                'month':{'min':1000000, 'max':0},
                'day':{'min':1000000, 'max':0},
                'year':{'min':1000000, 'max':0},
                'hours':{'min':1000000, 'max':0},
                'minutes':{'min':1000000, 'max':0},
                'seconds':{'min':1000000, 'max':0}}

            for next in reader:
                total += 1
                match = re.match('(\d+)/(\d+)/(\d+)(\s(\d+):(\d+):(\d+)(\s(am|pm))?)?', next[field])
                if None != match:
                    m = int(match.group(1))
                    if stats['month']['min'] > m: stats['month']['min'] = m
                    if stats['month']['max'] < m: stats['month']['max'] = m
                    d = int(match.group(2))
                    if stats['day']['min'] > d: stats['day']['min'] = d
                    if stats['day']['max'] < d: stats['day']['max'] = d
                    Y = int(year) if match.group(3)[2:] == year[2:] else int(match.group(3))
                    if stats['year']['min'] > Y: stats['year']['min'] = Y
                    if stats['year']['max'] < Y: stats['year']['max'] = Y

                    if (Y == int(year)) and (m > 0 and m < 13) and (d > 0 and d <= calendar.monthrange(Y,m)[1]):
                        if None != match.group(4):
                            minutes = int(match.group(6))
                            if stats['minutes']['min'] > minutes: stats['minutes']['min'] = minutes
                            if stats['minutes']['max'] < minutes: stats['minutes']['max'] = minutes
                            seconds = int(match.group(7))
                            if stats['seconds']['min'] > seconds: stats['seconds']['min'] = seconds
                            if stats['seconds']['max'] < seconds: stats['seconds']['max'] = seconds

                            hours = int(match.group(5))
                            if None != match.group(8):
                                hours += (12 if match.group(9) == 'pm' else 0)

                            if stats['hours']['min'] > hours: stats['hours']['min'] = hours
                            if stats['hours']['max'] < hours: stats['hours']['max'] = hours

                            if  not (hours >= 0 and hours < 24) or not (minutes >= 0 and minutes < 60) or not (seconds >= 0 and seconds < 60):
                                value_error += 1
                    else:
                        value_error += 1
                else:
                    ms = ('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec');
                    match = re.match('(\d+)-([a-z]+)-(\d+)(\s(\d+):(\d+):(\d+))?', next[field])
                    if None != match:
                        m = ms.index(match.group(2)) +1
                        if stats['month']['min'] > m: stats['month']['min'] = m
                        if stats['month']['max'] < m: stats['month']['max'] = m
                        d = int(match.group(1))
                        if stats['day']['min'] > d: stats['day']['min'] = d
                        if stats['day']['max'] < d: stats['day']['max'] = d
                        Y = int(year) if match.group(3) == year[2:] else int(match.group(3))
                        if stats['year']['min'] > Y: stats['year']['min'] = Y
                        if stats['year']['max'] < Y: stats['year']['max'] = Y

                        if (Y == int(year)) and (m > 0 and m < 13) and (d > 0 and d <= calendar.monthrange(Y,m)[1]):
                            if None != match.group(4):
                                minutes = int(match.group(6))
                                if stats['minutes']['min'] > minutes: stats['minutes']['min'] = minutes
                                if stats['minutes']['max'] < minutes: stats['minutes']['max'] = minutes
                                seconds = int(match.group(7))
                                if stats['seconds']['min'] > seconds: stats['seconds']['min'] = seconds
                                if stats['seconds']['max'] < seconds: stats['seconds']['max'] = seconds

                                hours = int(match.group(5))
                                if stats['hours']['min'] > hours: stats['hours']['min'] = hours
                                if stats['hours']['max'] < hours: stats['hours']['max'] = hours

                                if  not (hours >= 0 and hours < 24) or not (minutes >= 0 and minutes < 60) or not (seconds >= 0 and seconds < 60):
                                    value_error += 1
                        else:
                            value_error += 1
                    else:
                        format_error += 1

            return 'ERROR: format: %s (%.4f%%)\tvalue: %s (%.4f%%)\tSTATS: month: %s-%s\tday: %s-%s\tyear %s-%s\thours %s-%s\tminutes %s-%s\tseconds %s-%s' %\
                    (int(format_error), 100*format_error/total, int(value_error), 100*value_error/total,
                        stats['month']['min'], stats['month']['max'],
                        stats['day']['min'], stats['day']['max'],
                        stats['year']['min'], stats['year']['max'],
                        stats['hours']['min'], stats['hours']['max'],
                        stats['minutes']['min'], stats['minutes']['max'],
                        stats['seconds']['min'], stats['seconds']['max'])



    def audit_location(self, year):
        '''
        specific location related set of fields auditing

        PARAMETER year: string file label without extention (corresponding year)
        RETURN: string formated result (error if invalid location %occurrence, valid and recoverable cases %stats)
        '''
        def coords(x,y):
            '''
            parse lat,lng from state plane 5
            '''
            projection = pyproj.Proj(r'+proj=lcc +lat_1=34.03333333333333 +lat_2=35.46666666666667 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000 +ellps=GRS80 +units=m +no_defs')
            ll = projection(x, y, inverse=True)
            if str(ll[0])[:2] == '-1' and str(ll[1])[0] == '3':
                return {'lng': ll[0], 'lat': ll[1]}
            return False

        data = db.MySQL(__file__)
        with open('data/%s.csv' % year) as source:
            reader = csv.DictReader(source, delimiter=',', quotechar='"')
            total = 0.0
            all_match = 0.0
            rec_coords = 0.0
            rec_zip = 0.0
            rec_city = 0.0
            value_error = 0.0
            for next in reader:
                total += 1
                if next and next['x_coordinate'] != '' and next['y_coordinate'] != '':
                    x = float(next['x_coordinate']) * 0.3048
                    y = float(next['y_coordinate']) * 0.3048
                    ll = coords(x, y)
                    if False != ll:
                        find = data.query("""
                            select t.geoid as zip, z.primary_city
                            from us_zcta5 t, usps_zcta5 z
                            where st_contains(t.shape, point(%.6f, %.6f)) and t.geoid = z.zip
                            """ % (ll['lng'], ll['lat']))
                        if find and len(find) > 0:
                            if next['zip'] == find[0]['zip']: all_match += 1
                            else: rec_coords += 1
                        else:
                            value_error += 1                  ### not recoverable ###
                    else:
                        find = data.query("""
                            select latitude, longitude from usps_zcta5
                            where zip = '%s' and (primary_city = '%s' or acceptable_cities like '%%%s%%') and county = 'LOS ANGELES'
                            """ % (next['zip'], data.escape(next['city']), data.escape(next['city'])))
                        if find and len(find) == 1:
                            rec_city += 1                     ### recoverable through zip & city ###
                        else:
                            value_error += 1
                else:
                    if None != re.match('9\d\d\d\d', next['zip']):
                        check = data.query("""
                            select primary_city from usps_zcta5
                            where zip = '%s' and county = 'LOS ANGELES'
                            """ % next['zip'])
                        if check and len(check) == 1:
                            if check[0]['city'].lower() != next['city']:
                                rec_zip += 1                  ### recoverable through zip ###
                        else:
                            value_error += 1
                    elif '' != next['city']:
                        find = data.query("""
                            select zip from usps_zcta5
                            where (primary_city = '%s' or acceptable_cities like '%%%s%%') and county = 'LOS ANGELES'
                            """ % (data.escape(next['city']), data.escape(next['city'])))
                        if find and len(find) == 1:
                            rec_city += 1                     ### recoverable through city ###
                        else:
                            value_error += 1
            return 'ERROR: %.4f%%  ALL match: %.4f%% COORDS match: %.4f%% ZIP match: %.4f%% CITY match: %.4f%%' %\
                    (100*value_error/total, 100*all_match/total, 100*rec_coords/total, 100*rec_zip/total, 100*rec_city/total)



def main():
    '''
    initial archive download (one time job)
    '''

    ####################################################################
    ###  data filters will be based on generated logs  #################
    proc = DataAudit(range(2005,datetime.date.today().year))

    proc.timer('File format check')
    for Y in proc.YEARS:
        print '%s format errors: %d' % (Y, proc.check_format('%s' % Y))
    proc.timer()

    proc.timer('Category field stats')
    print '%s distinct categories: data/audit-log/category.tsv' % proc.audit_category()
    ### 42 distinct categories    ######################################
    proc.timer()

    proc.timer('Description text stats')
    print '%s distinct descriptions: data/audit-log/description.tsv' % proc.audit_description()
    ### 332 distinct descriptions ######################################
    proc.timer()

    proc.timer('Fields size stats')
    with open('data/audit-log/field-stats.tsv','wb') as log:
        ### looking into fields intended to use by App only ############
        for field in 'incident_date,category,stat_desc,address,street,city,zip,x_coordinate,y_coordinate,gang_related,deleted'.split(','):
            for Y in proc.YEARS:
                log.write('%s\t%s\t%s\n' % (Y, proc.field_stats('%s' % Y, field), field))
        log.close()
    print 'see data/audit-log/field-stats.tsv for fields size statistics'
    proc.timer()

    proc.timer('Date field stats')
    with open('data/audit-log/date.tsv','wb') as log:
        for Y in proc.YEARS:
            log.write('%s\t%s\n' % (Y, proc.audit_date('%s' % Y, 'incident_date')))
        log.close()
    print 'see data/audit-log/date.tsv for INCIDENT_DATE field statistics'
    proc.timer()

    proc.timer('Yes/No fields stats')
    for field in ['gang_related','deleted']:
        with open('data/audit-log/%s.tsv' % re.sub('_','-',field),'wb') as log:
            for Y in proc.YEARS:
                log.write('%s\t%s\n' % (Y, proc.audit_field('%s' % Y, field)))
            log.close()
    print 'see data/audit-log/gang-related.tsv and data/audit-log/deleted.tsv'
    proc.timer()

    proc.timer('Location parsing stats')
    with open('data/audit-log/location.tsv','wb') as log:
        for Y in proc.YEARS:
            print Y
            log.write('%s\t%s\n' % (Y, proc.audit_location('%s' % Y)))
        log.close()
    print 'see data/audit-log/location.tsv for location parsing statistics'
    proc.timer()



if __name__ == '__main__':
    main()
