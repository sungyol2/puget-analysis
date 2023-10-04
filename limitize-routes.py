import ted.gtfs as gtfs
import pandas as pd
import numpy as np
import os

premium_route_id = pd.read_csv("premium-routes")
premium_route_id_list = (premium_route_id.iloc[:,0]).tolist()

dir = "C:\Users\kaihe\Documents\KlumpentownConsulting\region\WAS\gtfs"
entries = os.listdir(dir)
index1 = 0

#Iterate through dated entries
while index1 < len(entries):

    curr_dated_entry = entries[index1]
    zip_entries = os.listdir(dir + r'{}\{}' + curr_dated_entry)
    index2 = 0

    while index2 < len(zip_entries):

        curr_zip_entry = zip_entries[index2]
        curr_dir = dir + r'{}\{}' + curr_dated_entry + r'{}\{}' + curr_zip_entry
        preparsed_gtfs = gtfs.load_zip(curr_dir)
        gtfs.remove_routes_from_gtfs(curr_dir,(curr_zip_entry + "-limited"),premium_route_id_list)
        index2 += 1

    index1 += 1


#1. Open the zipfile using the most recent version of GTFS-Lite
#2. Using the routes identified, use delete_routes() on the package and save as a modified file with a -LIMITED tag
#3. Upload the resulting limited GTFS files to the GDrive
