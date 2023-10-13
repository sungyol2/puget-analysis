from gtfslite.gtfs import GTFS
from ted.gtfs import remove_routes_from_gtfs
import pandas as pd
import zipfile
import datetime as dt
import os

DEBUG_MODE = True

premium_route_tag = pd.read_csv("premium_routes.csv",dialect=str,index_col=False)

#premium_route_slug_list = (premium_route_tag.iloc[:, 0]).tolist()
#premium_route_id_list = (premium_route_tag.iloc[:, 1]).tolist()

# specifies the entry and output folders
entry_path = "../region/WAS/gtfs"
ct = str(dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
output_path = os.path.join(os.getcwd(), "run-" + ct)
os.mkdir(output_path)

dated_entries = os.listdir(entry_path)
index1 = 0

# Iterate through all dated entries
for curr_dated_entry in dated_entries:

    # Make target output folder, this will look like: "../ted-data/runs/20XX-XX-XX-LIMITED"
    dated_output_path = os.path.join(output_path, curr_dated_entry + "-limited")
    os.mkdir(dated_output_path)

    zip_entries = os.listdir(os.path.join(entry_path, curr_dated_entry))
    index2 = 0

    # Iterate through .zip entries
    for curr_zip_entry in zip_entries:

        # Find entry zip folder
        curr_zip_dir = os.path.join(entry_path, curr_dated_entry, curr_zip_entry)

        if DEBUG_MODE:
            print("Currently parsing: " + curr_dated_entry + ": " + curr_zip_entry)

        try:
            remove_routes_from_gtfs(curr_zip_dir, dated_output_path, premium_route_id_list)
        except zipfile.BadZipFile:
            if DEBUG_MODE:
                print(curr_zip_entry, "is not a zipfile, skipping...")

    if DEBUG_MODE:
        print("\nFinished parsing: " + curr_dated_entry + "\n")

if DEBUG_MODE:
    print("done!")
