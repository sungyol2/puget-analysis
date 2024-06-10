import subprocess

import pandas as pd

from ted.config import MAPBOX_API_KEY

USERNAME = "wklumpen"

sources = pd.read_csv("../data/mapbox/sources.csv")

for idx, source in sources.iterrows():
    source_id = source["source_url"].split("/")[-1]
    print("Deleting", source_id)

    delete_source_string = f"tilesets delete-source {USERNAME} {source_id} --force --token {MAPBOX_API_KEY}".split(
        " "
    )
    print()
    print(" ".join(delete_source_string))
    subprocess.run(delete_source_string)
    print()
