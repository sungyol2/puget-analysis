import datetime
import json
import os
import subprocess

from ted.config import MAPBOX_API_KEY

REGION = "CHI"
UPLOAD_FOLDER = "/home/willem/Documents/Project/TED/data/upload"
USERNAME = "wklumpen"
TEST_RUN = False

uploads = []

for upload in os.listdir(os.path.join(UPLOAD_FOLDER, "test")):
    if upload.startswith(REGION):
        uploads.append(upload)

recipe = {
    "version": 1,
    "layers": {},
}

for upload in uploads:
    date = upload.split("_")[1]
    tod = upload.split("_")[2].split(".")[0]
    upload_file = os.path.join(UPLOAD_FOLDER, "geojson", upload)
    source_name = f"{REGION}-{date}-{tod}-test2"
    upload_source_string = f"tilesets upload-source {USERNAME} --token {MAPBOX_API_KEY} {source_name} {upload_file}".split(
        " "
    )
    print()
    print("  Uploading Source", upload)
    if TEST_RUN == True:
        print(" ".join(upload_source_string))
    else:
        subprocess.run(upload_source_string)

    recipe["layers"][source_name] = {
        "source": f"mapbox://tileset-source/{USERNAME}/{source_name}",
        "minzoom": 10,
        "maxzoom": 16,
    }


recipe_filepath = os.path.join(UPLOAD_FOLDER, "recipes", f"{REGION}-recipe.json")
with open(
    recipe_filepath,
    "w",
) as recipe_file:
    recipe_file.write(json.dumps(recipe, indent=4))

create_tileset_string = f"tilesets create {USERNAME}.{REGION}-tiles --token {MAPBOX_API_KEY} --recipe {recipe_filepath}".split(
    " "
)
create_tileset_string.append("--name")
create_tileset_string.append(f"{REGION}-ted2.0")
print()
print("  Creating Tileset")
if TEST_RUN == True:
    print(" ".join(create_tileset_string))
else:
    subprocess.run(create_tileset_string)

print()
print("  Publishing Tileset")
publish_tileset_string = (
    f"tilesets publish {USERNAME}.{REGION}-tiles --token {MAPBOX_API_KEY}".split(" ")
)
if TEST_RUN == True:
    print(" ".join(publish_tileset_string))
else:
    subprocess.run(publish_tileset_string)
print()

# subprocess.run(
#     f"tilesets upload-source wklumpen populated-places-source {UPLOAD_FOLDER}"
# )
