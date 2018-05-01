import os
import time
from io import BytesIO
from zipfile import ZipFile
import urllib.request
import pandas as pd
import numpy as np


def download_divvy(main_directory, sub_directory):
    start_time = time.time()

    divvy_links = [
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

    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.mkdir(main_directory)

    for i in range(len(divvy_links)):
        directory = os.path.join(
            os.path.sep, dir_path, main_directory, '{}{}'.format(sub_directory, i))
        os.mkdir(directory)
        url = urllib.request.urlopen(divvy_links[i])

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

    for i in range(8):
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

download_divvy(main_directory, sub_directory)

#build_csv(main_directory, sub_directory, output_filename)
