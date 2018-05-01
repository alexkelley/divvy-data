#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import sys
import pprint
import sqlite3


def execute_query(db_name, query):
    '''
    Takes a query string

    Returns a list of tuples
    '''
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(query)

    results = cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()

    return results


def create_table(db_name, table_name, attribute_dict):
    '''
    Define structure of attribute_dict
    {}
    '''
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))

    build_table_sql = 'CREATE TABLE {} (\n'.format(table_name)

    for key, value in attribute_dict.items():
        build_table_sql += "   {} {},\n".format(
            value['name'], value['data_type'])

    build_table_sql = build_table_sql[:-2] + '\n);'

    cursor.execute(build_table_sql)

    conn.commit()
    cursor.close()
    conn.close()


def generate_insert(table_name, value_dict):
    value_string = ''
    column_string = ''

    for label, value in value_dict.items():
        if value is None:
            value = 'NULL'

        if value != 'NULL' and isinstance(value, str):
            value_string += "'{}', ".format(value.replace("'", ""))
        elif isinstance(value, date):
            value_string += "'{}', ".format(value)
        elif isinstance(value, bool):
            value_string += "'{}', ".format(value)
        else:
            value_string += "{}, ".format(value)

        column_string += "{}, ".format(label.replace("'", ""))

        sql_string = "INSERT INTO {0} ({1}) VALUES ({2});".format(
            table_name, column_string[:-2], value_string[:-2])

    return sql_string



def load_data_into_table(db_name, table_name, sb_list):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    for record in sb_list:
        sql_call = generate_insert(table_name, record)

        try:
            cursor.execute(sql_call)
        except:
            print('load_data_into_table\n{}'.format(sys.exc_info()))
            print(sql_call)
            return False

    conn.commit()
    cursor.close()
    conn.close()
    return True


def parse_csv(main_directory, sub_directory):
    '''
    '''
    count = 1

    for i in range(9):
        directory = os.path.join(main_directory, sub_directory + str(i))
        for subdir, dirs, files in os.walk(directory):

            # build a DataFrame for station location details based on smallest file
            for file in files:
                filename = os.path.join(subdir, file)
                filesize = os.stat(filename).st_size
                if filesize < 100000:
                    station_details = pd.read_csv(filename, index_col=0)
                    # print(list(station_details))

            # build a trip DF for each file and subset on desired stations
            for file in files:
                filename = os.path.join(subdir, file)
                filesize = os.stat(filename).st_size
                if filesize > 100000:
                    df = pd.read_csv(filename)
                    # print(len(list(df)))
                    # print(list(df))

                    # landmark column did not appear until July 2014
                    # if len(list(df)) < 19:
                    #    df['landmark'] = np.zeros(len(df))

                    # subset temp df on desired station_ids
                    x = df[df['from_station_id'].isin(station_ids.keys())]

                    # choose to reduce the observations by
                    # total number (n) or % (frac)
                    x = df.sample(
                        n=1000
                        # frac=0.1
                    )
                    # join station_id data on temp df (x)
                    y = x.join(station_details[['name', 'latitude', 'longitude', 'dpcapacity']],
                               on='from_station_id', how='inner')

                    # create file if first file, append data if not
                    if count == 1:
                        print(y.dtypes)
                        with open(output_filename, 'w') as f:
                            y.to_csv(f, header=True)
                        print("\nRaw files processed: {}".format(count))
                    else:
                        print("Raw files processed: {}".format(count))
                        with open(output_filename, 'a') as f:
                            y.to_csv(f, header=False)

                    count += 1

##################################
## Set up file and folder names ##
##################################

db_name = 'local_divvy.db'

main_directory, sub_directory = 'divvy_main', 'divvy_data_'

###################
## Run functions ##
###################
