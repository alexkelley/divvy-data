#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import urllib.request
from io import BytesIO
from zipfile import ZipFile


def download_divvy(links, main_directory, sub_directory):
    '''
    Download online Divvy data zip files and extract the csv files
    to a set of nested directories.
    '''
    start_time = time.time()

    dir_path = os.path.dirname(os.path.realpath(__file__))

    # TODO check if directory exists already and handle gracefully
    os.mkdir(main_directory)

    for i in range(len(links)):
        directory = os.path.join(
            os.path.sep, dir_path, main_directory, '{}{}'.format(sub_directory, i))
        os.mkdir(directory)
        url = urllib.request.urlopen(links[i])

        with ZipFile(BytesIO(url.read())) as my_zip_file:
            for i, contained_file in enumerate(my_zip_file.namelist()):
                if contained_file.endswith(".csv"):
                    with open(("{0}/divvy_file_{1}.csv".format(directory, i)), "wb") as output:
                        for line in my_zip_file.open(contained_file).readlines():
                            output.write(line)

        end_time = time.time()
        print('Directory created: {}\nElapsed time: {:.2f} seconds\n'.format(
            directory, end_time - start_time))

    end_time = time.time()
    print('All Divvy files saved.\nTotal time: {:.2f} seconds\n'.format(
        end_time - start_time))


##################################
## Set up file and folder names ##
##################################

links = [
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Stations_Trips_2013.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Stations_Trips_2014_Q1Q2.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Stations_Trips_2014_Q3Q4.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Trips_2015-Q1Q2.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Trips_2015_Q3Q4.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Trips_2016_Q1Q2.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Trips_2016_Q3Q4.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Trips_2017_Q1Q2.zip",
    "https://s3.amazonaws.com/divvy-data/tripdata/Divvy_Trips_2017_Q3Q4.zip"
    ]

main_directory, sub_directory = 'divvy_main', 'divvy_data_'

###################
## Run functions ##
###################

#download_divvy(links, main_directory, sub_directory)
