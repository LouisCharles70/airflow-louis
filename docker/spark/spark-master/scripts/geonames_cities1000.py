""" A Fast, Offline Reverse Geocoder in Python

A Python library for offline reverse geocoding. It improves on an existing library
called reverse_geocode developed by Richard Penman.
"""
import os
import sys
import csv
import traceback
from os import environ as osenv

if sys.platform == 'win32':
    # Windows C long is 32 bits, and the Python int is too large to fit inside.
    # Use the limit appropriate for a 32-bit integer as the max file size
    csv.field_size_limit(2 ** 31 - 1)
else:
    csv.field_size_limit(sys.maxsize)
import zipfile
import pandas as pd

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

conf = SparkConf()
conf.setAppName('Geonames Cities1000')

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
sc = spark.sparkContext

GN_URL = 'http://download.geonames.org/export/dump/'
GN_CITIES1000 = 'cities1000'
GN_ADMIN1 = 'admin1CodesASCII.txt'
GN_ADMIN2 = 'countryInfo.txt'
INCLUDED_FEATURE_CODES = ['PPLC', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLA5']

RC_MAPPINGS = {
    'CA.01': 'AB',
    'CA.02': 'BC',
    'CA.03': 'MB',
    'CA.04': 'NB',
    'CA.05': 'NL',
    'CA.07': 'NS',
    'CA.08': 'ON',
    'CA.09': 'PE',
    'CA.10': 'QC',
    'CA.11': 'SK',
    'CA.12': 'YT',
    'CA.13': 'NT',
    'CA.14': 'NU',
    'AU.01': 'ACT',
    'AU.02': 'NSW',
    'AU.03': 'NT',
    'AU.04': 'QLD',
    'AU.05': 'SA',
    'AU.06': 'TAS',
    'AU.07': 'VIC',
    'AU.08': 'WA'
}

# Schema of the GeoNames Cities with Population > 1000
GN_COLUMNS = {
    'geoNameId': 0,
    'name': 1,
    'asciiName': 2,
    'alternateNames': 3,
    'latitude': 4,
    'longitude': 5,
    'featureClass': 6,
    'featureCode': 7,
    'countryCode': 8,
    'cc2': 9,
    'admin1Code': 10,
    'admin2Code': 11,
    'admin3Code': 12,
    'admin4Code': 13,
    'population': 14,
    'elevation': 15,
    'dem': 16,
    'timezone': 17,
    'modificationDate': 18
}

# Schema of the GeoNames Admin 1/2 Codes
ADMIN_COLUMNS = {
    'concatCodes': 0,
    'name': 1,
    'asciiName': 2,
    'geoNameId': 3
}

COUNTRY_COLUMNS = {
    'cc': 0,
    'name': 4
}

# Schema of the cities file created by this library
RG_COLUMNS = [
    'lat',
    'lon',
    'name',
    'admin1',
    'admin2',
    'cc'
]

# Name of cities file created by this library
RG_FILE = 'rg_cities1000.csv'

# WGS-84 major axis in kms
A = 6378.137

# WGS-84 eccentricity squared
E2 = 0.00669437999014


def create_table(year_month_day_hour):
    return """
        CREATE EXTERNAL TABLE fan_search.geonames_cities1000(
            lat FLOAT,
            lon FLOAT,
            name STRING,
            admin1 STRING,
            admin2 STRING,
            cc STRING)
        STORED AS PARQUET
        LOCATION 's3://fan-bigdata/geonames_data/geonames_cities1000/{}';
    """.format(year_month_day_hour)


def percentage_change(start_count, end_count):
    return ((end_count - start_count) / start_count) * 100


def extract():
    gn_cities1000_url = GN_URL + GN_CITIES1000 + '.zip'
    # gn_admin1_url = GN_URL + GN_ADMIN1
    gn_admin2_url = GN_URL + GN_ADMIN2

    cities1000_zipfilename = GN_CITIES1000 + '.zip'
    cities1000_filename = GN_CITIES1000 + '.txt'

    try:  # Python 3
        import urllib.request
        urllib.request.urlretrieve(gn_cities1000_url, cities1000_zipfilename)
        # urllib.request.urlretrieve(gn_admin1_url, GN_ADMIN1)
        urllib.request.urlretrieve(gn_admin2_url, GN_ADMIN2)
    except ImportError:  # Python 2
        import urllib
        urllib.urlretrieve(gn_cities1000_url, cities1000_zipfilename)
        # urllib.urlretrieve(gn_admin1_url, GN_ADMIN1)
        urllib.urlretrieve(gn_admin2_url, GN_ADMIN2)

    _z = zipfile.ZipFile(open(cities1000_zipfilename, 'rb'))
    open(cities1000_filename, 'wb').write(_z.read(cities1000_filename))

    # if self.verbose:
    #     print('Loading admin1 codes...')
    # admin1_map = {}
    # t_rows = csv.reader(open(GN_ADMIN1, 'rt'), delimiter='\t')
    # for row in t_rows:
    #     admin1_map[row[ADMIN_COLUMNS['concatCodes']]] = row[ADMIN_COLUMNS['asciiName']]

    admin2_map = {}
    for row in csv.reader(open(GN_ADMIN2, 'rt'), delimiter='\t'):
        if row[0][0] == '#':
            continue
        admin2_map[row[COUNTRY_COLUMNS['cc']]] = row[COUNTRY_COLUMNS['name']]

    rows = []
    for row in csv.reader(open(cities1000_filename, 'rt'), delimiter='\t', quoting=csv.QUOTE_NONE):
        # Only Include Administrative Cities
        if row[GN_COLUMNS['featureCode']] not in INCLUDED_FEATURE_CODES:
            continue

        lat = row[GN_COLUMNS['latitude']]
        lon = row[GN_COLUMNS['longitude']]
        name = row[GN_COLUMNS['asciiName']]
        cc = row[GN_COLUMNS['countryCode']]

        admin1_c = row[GN_COLUMNS['admin1Code']]
        admin2_c = row[GN_COLUMNS['admin2Code']]

        cc_admin1 = cc + '.' + admin1_c
        cc_admin2 = cc + '.' + admin1_c + '.' + admin2_c

        admin1 = RC_MAPPINGS.get(cc_admin1, str(admin1_c))
        admin2 = ''

        # if cc_admin1 in admin1_map:
        #     admin1 = admin1_map[cc_admin1]
        if cc in admin2_map:
            admin2 = admin2_map[cc]

        write_row = {'lat': float(lat),
                     'lon': float(lon),
                     'name': name,
                     'admin1': admin1,
                     'admin2': admin2,
                     'cc': cc}
        rows.append(write_row)

    return rows


def rel_path(filename):
    """
    Function that gets relative path to the filename
    """
    return os.path.join(os.getcwd(), os.path.dirname(__file__), filename)


def main():
    if len(sys.argv) < 2:
        print("Args are not set for: geonames_cities1000.py")
        sys.exit()

    year_month_day_hour = sys.argv[1]
    try:
        rows = extract()
        if len(rows) > 30000:  # QUICK check for validity of data
            # Check if path already exists
            path = f'./data/{year_month_day_hour}'

            spark.sql("CREATE DATABASE IF NOT EXISTS fan_search")
            spark.sql("CREATE TABLE IF NOT EXISTS fan_search.geonames_cities1000 (lat Float, lon Float)")


            # Write csv to cloudera
            df = pd.DataFrame(rows)
            spark_df = spark.createDataFrame(df) \
                .withColumn("lat", col("lat").cast(FloatType()).alias("lat")) \
                .withColumn("lon", col("lon").cast(FloatType()).alias("lon"))
            spark_df.write.mode("overwrite").parquet(path)
            #
            query = f"""ALTER TABLE fan_search.geonames_cities1000 SET LOCATION '{path}'""".strip()
            spark.sql(query)
        else:
            print({
                'status': 'ERROR',
                'type': 'Geonames Data Error',
                'rows': len(rows)
            })
            sys.exit(-1)
    except Exception as Error:
        print({
            'status': 'ERROR',
            'type': 'Error Loading Geonames',
            'error': traceback.format_exc()
        })

        print("Error: ", Error)
        sys.exit(-1)

    spark.stop()
    exit()


if __name__ == "__main__":
    main()
