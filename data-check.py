from gtfslite.gtfs import GTFS
from ted.gtfs import get_all_stops, summarize_gtfs_data
import pandas as pd
import zipfile
import datetime as dt
import os

gtfs_folder = "../region/WAS/gtfs/2020-02-24"
agencies_file = '../ted-data/agencies.csv'
gtfs = gtfs_folder

stops = get_all_stops(gtfs)
print(stops.head())
#summary = summarize_gtfs_data(gtfs)

feed_check_list = pd.read_csv(agencies_file)



# for date in os.listdir(gtfs_folder):

#     feeds = os.listdir(os.path.join(gtfs_folder, date))

#     for feed in feeds:

#Pseudo:
#go into dated ntry and look through all feed 
# use get_all_stops to get all the stops
# look into what summarize gtfs data does
# - add total # of stops
# - add total # of routes
# - List all unique route_type in routes.txt
# - make reccomendations?? or flag feeds that dont have all data

