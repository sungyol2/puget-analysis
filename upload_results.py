import datetime
import json
import os
import subprocess

from ted.config import MAPBOX_API_KEY

REGION = "WAS"
UPLOAD_FOLDER = "/home/willem/Documents/Project/TED/data/upload"
USERNAME = "wklumpen"

for upload in os.listdir(os.path.join(UPLOAD_FOLDER, "geojson")):
    if upload.startswith(REGION):
        print("Creating Mapbox Tileset for", upload)
        date = upload.split("_")[1]
        tod = upload.split("_")[2].split(".")[0]
        upload_file = os.path.join(UPLOAD_FOLDER, "geojson", upload)
        source_name = f"{REGION}-{date}-{tod}"
        upload_source_string = f"tilesets upload-source {USERNAME} --token {MAPBOX_API_KEY} {source_name} {upload_file}".split(
            " "
        )
        print("  Uploading Tileset")
        subprocess.run(upload_source_string)

        # Now we create our recipe
        recipe = {
            "version": 1,
            "layers": {
                f"{source_name}": {
                    "source": f"mapbox://tileset-source/{USERNAME}/{source_name}",
                    "minzoom": 8,
                    "maxzoom": 10,
                }
            },
        }
        recipe_filepath = os.path.join(
            UPLOAD_FOLDER, "recipes", f"{source_name}-recipe.json"
        )
        with open(
            recipe_filepath,
            "w",
        ) as recipe_file:
            recipe_file.write(json.dumps(recipe, indent=4))

        create_tileset_string = f"tilesets create {USERNAME}.{source_name}-tiles --token {MAPBOX_API_KEY} --recipe {recipe_filepath}".split(
            " "
        )
        create_tileset_string.append("--name")
        create_tileset_string.append(f"{source_name}")
        print()
        print("  Creating Tileset")
        subprocess.run(create_tileset_string)

        print()
        print("  Publishing Tileset")
        publish_tileset_string = (
            f"tilesets publish {USERNAME}.{source_name}-tiles".split(" ")
        )
        subprocess.run(publish_tileset_string)

# subprocess.run(
#     f"tilesets upload-source wklumpen populated-places-source {UPLOAD_FOLDER}"
# )
