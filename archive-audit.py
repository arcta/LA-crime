import os
import re
import sys
import csv
import datetime

import numpy as np
import pandas as pd
import MySQLdb as db

from pyproj import Proj, transform

# features to collect
fields =['id','stat','category','date','year','month','day','weekday','hour','address','city','zip','lng','lat','gang']

# local database
con = db.connect(os.environ['MYSQL_HOST'], os.environ['DATAUSER'],
                 os.environ['MYSQL_PASS'], os.environ['DATABASE'], int(os.environ['MYSQL_PORT']))
# lookup table
zipcodes = pd.read_sql("SELECT zip, longitude, latitude, primary_city as city FROM usps_zcta5 WHERE state = 'CA'", con = con)

# State Plane 5 (South California)
projection = Proj(' '.join(['+proj=lcc','+lat_1=34.03333333333333','+lat_2=35.46666666666667',
                            '+lat_0=33.5','+lon_0=-118','+x_0=2000000','+y_0=500000','+ellps=GRS80',
                            '+units=us-ft','+no_defs']), preserve_units = True)

# drop non-criminal and misdemeanors
drop = ['misdemeanors','sex offenses misdemeanors','miscellaneous non-criminal','misdemeanors miscellaneous',
        'juvenile non-criminal','commitments','disorderly conduct','mentally ill',
        'accidents miscellaneous','accidents traffic/veh./boat',
        'suicide and attempt','persons dead','persons missing','receiving stolen property']


def collapse(x):
    """short category: single word"""
    
    if x == 'criminal homicide':
        return 'homicide'
    if x == 'forcible rape':
        return 'rape'
    if x == 'grand theft auto':
        return 'gta'
    if x[:13] == 'drunk driving':
        return 'dui'
    if x == 'larceny theft':
        return 'theft'
    if x == 'offenses against family':
        return 'family'
    if 'assault' in x:
        return 'assault'
    return x.split()[0]


def yes_no(x):
    """binary values for Yes/No type"""
    
    x = x.lower()
    if x[0] == 'n':
        return 0
    if x[0] == 'y':
        return 1
    return None


def extract(d):
    """extract/cleanup date-time related features"""
    
    Y, M, D, W, H = (None for _ in range(5))
    
    def get_hour(groups):
        H, m, s = (int(x) for x in groups[4:7])
        if groups[8] == 'am' and H == 12:
            H = 0
        if groups[8] == 'pm' and 0 < H < 12:
            H += 12
        return H + m/60 + s/3600
        
    if type(d) == str:
        d = d.lower()
        match = re.match(r'^(\d+)/(\d+)/(20\d+)( (\d+):(\d+):(\d+)( (am|pm))?)?', d)
        if match is None:
            match = re.match(r'^(\d+)-([a-z]+)-(\d+)( (\d+):(\d+):(\d+)( (am|pm))?)?', d)
            if match is None:
                return
            else:
                month = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
                D = int(match.group(1))
                M = month.index(match.group(2)) + 1
                Y = 2000 + int(match.group(3))
                W = datetime.date(Y, M, D).timetuple()[6]
                if match.group(4) is None:
                    H = -1
                else:
                    H = get_hour(match.groups())
        else:
            M, D, Y = (int(x) for x in (match.groups())[:3])
            W = datetime.date(Y, M, D).timetuple()[6]
            if match.group(4) is None:
                H = -1
            else:
                H = get_hour(match.groups())
    return (Y, M, D, W, H)


def to_lnglat(r):
    """convert state plane coordinates to longitude-latitude"""
    
    x, y = r['x_coordinate'], r['y_coordinate']
    if np.isnan(x) or np.isnan(y):
        return None, None
    return projection(x, y, inverse = True)


def fix_location(r):
    """get the best possible coordinates + zipcode assessment"""
    
    # all is fine: just change zipcode datatype to str
    if not np.isnan(r['zip']) and not np.isnan(r['lat']):
        return [str(int(r['zip'])), r['lng'], r['lat']]
    
    # try to locate within zipcode polygons
    if not np.isnan(r['lat']):
        query = """
        SELECT t.geoid as zip, {} as lng, {} as lat
        FROM us_zcta5 t JOIN usps_zcta5 z ON t.geoid = z.zip
        WHERE ST_Contains(t.shape, ST_GeomFromText('POINT({} {})', 2))
        """
        res = pd.read_sql(query.format(r['lng'], r['lat'], r['lng'], r['lat']), con = con)
        if len(res) == 1:
            return res.values[0].tolist()

    # use zipcode center as location proxy: geocoding is prefered in this case, but might be quite expensive
    if not np.isnan(r['zip']):
        res = zipcodes[zipcodes['zip'] == str(int(r['zip']))]
        if len(res) == 1:
            return res.values[0].tolist()[:3]

    return [None, None, None]


def main():
    """parse and save features from downloaded archive (entire or the range)"""
    
    Y1, Y2 = 2005, 2017 ### range with coordinates supplied in pre-2018 generated archive

    if len(sys.argv) > 1 and int(sys.argv[1]) > 0:
        Y1 = int(sys.argv[1])
        
        if len(sys.argv) > 2 and int(sys.argv[2]) > Y1:
            Y2 = int(sys.argv[2])
    
    with open('data/audit.log','w') as output:
        for Y in range(Y1, Y2):
            df = pd.read_csv('data/{}.csv'.format(Y), low_memory = False)
            output.write('\n--- {} --------------------\n'.format(Y))

            # remove `deleted` records
            df['deleted'] = df['deleted'].apply(yes_no)
            df = df[df['deleted'] == 0]

            # remove misc misdemeanors
            df = df[~df['category'].isin(drop)]

            # validate date and expand into Y,N,D,W,H
            df['dt'] = df['incident_date'].apply(extract)
            df = df[~df['dt'].isnull()]

            # convert from plane state to longitude-latitude
            df['ll'] = df.apply(to_lnglat, axis = 1)

            # init features
            features = df.loc[:,['category','stat','address','city','zip']]
            features['id'] = df['incident_id']
            dt = ['year','month','day','weekday','hour']
            for i in range(len(dt)):
                features[dt[i]] = df['dt'].apply(lambda x: x[i] )

            features['lng'] = df['ll'].apply(lambda x: x[0])
            features['lat'] = df['ll'].apply(lambda x: x[1])

            features['gang'] = df['gang_related'].apply(yes_no)
            features['category'] = df['category'].apply(collapse)
            cat = set(features.groupby(['category']).size().reset_index(name='count')['category'].tolist())
            output.write('Categories: {}\n'.format(len(cat)))

            output.write('Date miss: {:.4f}%\n'\
                .format(100 * (1 - len(features[(features['year'] > 2000) & (~features['weekday'].isnull())])/len(features))))
            output.write('Location miss: {:.4f}%\n'\
                .format(100 * (1 - len(features[(features['zip'] > 0) | (features['lat'] > 0)])/len(features))))

            # keep records with valid date
            features['date'] = df['dt'].apply(lambda x: datetime.date(x[0], x[1], x[2]))
            features = features[(features['year'] > 2000) & (~features['weekday'].isnull())]
            output.write('Time miss: {:.4f}%\n'.format(100 * len(features[features['hour'] == -1])/len(features)))

            # potential `time-unknown` issue
            output.write('Hour ZERO: {:.4f}%\n'.format(100 * len(features[features['hour'] == 0])/len(features)))
            output.write('Hour NOON: {:.4f}%\n'.format(100 * len(features[features['hour'] == 12])/len(features)))

            features = features[(features['zip'] > 0) | (features['lat'] > 0)]

            # get the best possible coordinates + zipcode assessment
            features[['zip','lng','lat']] = features[['zip','lng','lat']].apply(fix_location, axis = 1)
            output.write('Failed location: {:.4f}%\n'.format(100 * len(features[features['zip'].isnull()])/len(features)))
            features = features[~features['zip'].isnull()]
            features['zip'] = df['zip'].apply(lambda x: str(x)[:5])
            
            # normalize city attr
            features = features.join(zipcodes[['zip','city']].set_index('zip'), on = 'zip', lsuffix = '_orig', rsuffix = '')
            features.loc[features['city'].isnull(), 'city'] = features.loc[features['city'].isnull(), 'city_orig']\
                .apply(lambda x: x if type(x) == float else ' '.join([l[0].upper() + l[1:] for l in x.split()]))

            # reduce to LA bounding-box
            features = features[(features['lng'] > -119) & (features['lng'] < -116)]
            features = features[(features['lat'] > 32) & (features['lat'] < 35)]

            # save csv
            features[fields].to_csv('data/F{}.csv'.format(Y), index = False)
            features[fields].to_json('data/F{}.json'.format(Y), orient = 'records')
        output.close()


if __name__ == '__main__':
    main()

