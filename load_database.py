#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import csv
from datetime import datetime, timedelta, date
import sys
import pprint
import sqlite3
from dateutil.parser import parse

def run_query(db_name, query):
    '''
    Runs a query on the designated database
    '''
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute(query)

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

        sql_string = "INSERT OR REPLACE INTO {0} ({1}) VALUES ({2});".format(
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



def parse_csv(filename):
    data_list = []

    with open(filename, 'r') as f:
        reader = csv.reader(f)

        data = [r for r in reader if r != '']

        headers = list(filter(None, data.pop(0)))

        replacements = {
            'birthday': 'birthyear',
            'starttime': 'start_time',
            'stoptime': 'end_time'
        }
        headers = [replacements.get(n, n) for n in headers if n in headers]

        # for testing
        #print(headers)
        #data = list(filter(None, data[:50]))
        #print(data)

        for row in data:
            d = {}
            for i in range(len(headers)):
                header = headers[i]
                field_value = row[i]

                # fix inconsistent date formats
                if header in ('start_time', 'end_time', 'online_date'):
                    field_value = parse(field_value)
                    field_value = datetime.strftime(field_value, '%Y-%m-%d %H:%M')

                d[header] = field_value

            data_list.append(d)

    return data_list


def walk_csv_files(main_directory, sub_directory, db_name):
    '''
    Traverse a directory of csv files and upload to database
    '''
    for i in range(9):
        directory = os.path.join(main_directory, sub_directory + str(i))
        for subdir, dirs, files in os.walk(directory):

            for file in files:
                filename = os.path.join(subdir, file)
                filesize = os.stat(filename).st_size
                if filesize > 100000:
                    print('Loading: {}'.format(filename))
                    data = parse_csv(filename)
                    print(load_data_into_table(db_name, 'trips', data))


def set_up_stations(db_name):
    run_query(db_name, 'DROP TABLE IF EXISTS stations;')

    with open('stations_table.sql', 'r') as f:
        stations = f.read()

    run_query(db_name, stations)

    # most recent stations detail file from Divvy Data
    stations = parse_csv('/home/alex/divvy-data/divvy_main/divvy_data_8/divvy_file_0.csv')

    print(load_data_into_table(db_name, 'stations', stations))


def set_up_trips(db_name):
    run_query(db_name, 'DROP TABLE IF EXISTS trips;')
    with open('trips_table.sql', 'r') as f:
        trips = f.read()

    run_query(db_name, trips)
    print('trips table built.')

    walk_csv_files(main_directory, sub_directory, db_name)


##################################
## Set up file and folder names ##
##################################

db_name = 'local_divvy.db'

main_directory, sub_directory = 'divvy_main', 'divvy_data_'

###################
## Run functions ##
###################

# set_up_stations(db_name)

set_up_trips(db_name)

failed = [
    'divvy_main/divvy_data_5/divvy_file_4.csv',
    'divvy_main/divvy_data_6/divvy_file_1.csv',
    'divvy_main/divvy_data_7/divvy_file_3.csv',
]
