import datetime
import json
import os
import subprocess
import time

from ted.config import MAPBOX_API_KEY

REGION = "WAS"
UPLOAD_FOLDER = "/home/willem/Documents/Project/TED/data/upload/dots/mapbox"
RECIPES_FOLDER = "/home/willem/Documents/Project/TED/data/upload/recipes"
USERNAME = "wklumpen"
TEST_RUN = False

uploads = []

for upload in os.listdir(UPLOAD_FOLDER):
    if upload.startswith(REGION):
        uploads.append(upload)

uploads = list(set(uploads))

for upload in uploads:
    recipe = {"version": 1, "layers": {}}
    print("Uploading", upload)
    region = upload.split("_")[0]
    demographic = "_".join(upload.split("_")[1:]).split(".")[0]
    upload_file = os.path.join(UPLOAD_FOLDER, f"{upload}")
    tileset_name = f"{region}-{demographic}-dots"
    source_name = tileset_name

    upload_source_string = f"tilesets upload-source {USERNAME} --no-validation --replace --token {MAPBOX_API_KEY} {source_name} {upload_file}".split(
        " "
    )
    print()
    print("  Uploading Source")
    if TEST_RUN == True:
        print(" ".join(upload_source_string))
    else:
        subprocess.run(upload_source_string)

    recipe["layers"][source_name] = {
        "source": f"mapbox://tileset-source/{USERNAME}/{source_name}",
        "minzoom": 7,
        "maxzoom": 10,
        "tiles": {"layer_size": 2500},
    }

    recipe_filepath = os.path.join(RECIPES_FOLDER, f"{region}-dots-recipe.json")
    with open(
        recipe_filepath,
        "w",
    ) as recipe_file:
        recipe_file.write(json.dumps(recipe, indent=4))

    create_tileset_string = f"tilesets create {USERNAME}.{tileset_name}-tiles --token {MAPBOX_API_KEY} --recipe {recipe_filepath}".split(
        " "
    )
    create_tileset_string.append("--name")
    create_tileset_string.append(tileset_name)
    print()
    print("  Creating Tileset")
    if TEST_RUN == True:
        print(" ".join(create_tileset_string))
    else:
        subprocess.run(create_tileset_string)

    print()
    print("  Publishing Tileset")
    publish_tileset_string = f"tilesets publish {USERNAME}.{tileset_name}-tiles --token {MAPBOX_API_KEY}".split(
        " "
    )
    if TEST_RUN == True:
        print(" ".join(publish_tileset_string))
    else:
        subprocess.run(publish_tileset_string)
    print()
    print("Waiting 20 seconds...")
    time.sleep(20)
