#! /usr/bin/env python3

import os
import time
import pandas as pd
import numpy as np


def load_data_to_sql():
    pass


def build_csv(main_directory, sub_directory, output_filename):
    '''
    Using the Pandas package, join station details to trip data for
    selected stations
    '''
    station_ids = {35: 'NavyPier',
                   # 91: 'Ogilvie',
                   # 114: 'WrigleyField'
                   }

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


def describe_dataset(filename):
    pass

##################################
## Set up file and folder names ##
##################################

main_directory, sub_directory = 'divvy_main', 'divvy_data_'
output_filename = 'final_divvy_data.csv'

###################
## Run functions ##
###################

#build_csv(main_directory, sub_directory, output_filename)
