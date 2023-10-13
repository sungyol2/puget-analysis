import pandas as pd
import numpy as np
import os

entry_path = "../region/WAS/gtfs"
names = []

dated_entries = os.listdir(entry_path)

for x in dated_entries:

    curr_dated_entry = x
    zip_entries = os.listdir(os.path.join(entry_path, curr_dated_entry))

    for y in zip_entries:
        curr_zip_entry = y
        if not y.startswith('._'):
            curr_zip_entry = curr_zip_entry.removesuffix('.zip')
            names.append(curr_zip_entry)

unique_names = pd.DataFrame()
unique_names['route_slug'] = np.unique(names)
unique_names.to_csv("C:/Users/kaihe/Desktop/unique_names.csv",index=False)
