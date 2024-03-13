import pandas as pd
import geopandas as gpd
import altair as alt

region_key = "SFO"
print("Running Quality Control on Auto Travel Times")
for tod in ["WEDAM", "WEDPM", "SATAM"]:
    print(tod)
    MATRIX_FILE = f"/home/willem/Documents/Project/TED/data/region/{region_key}/auto/{tod}.parquet"
    bgs = gpd.read_file(
        f"/home/willem/Documents/Project/TED/data/region/{region_key}/{region_key}.gpkg",
        layer="bg_areas",
    )
    CENTRAL_BGS = {
        "CHI": "170318390004",
        "WAS": "110010101002",
        "LA": "060372073062",
        "PHL": "421010005002",
        "BOS": "250250203041",
        "SFO": "060750117003",
        "NYC": "360610101001",
    }
    mx = pd.read_parquet(MATRIX_FILE)
    mx = mx[mx.to_id == CENTRAL_BGS[region_key]]
    mxbg = pd.merge(mx, bgs, left_on="from_id", right_on="BG20")[
        ["BG20", "travel_time", "geometry"]
    ]
    mxbg = gpd.GeoDataFrame(mxbg, geometry=mxbg.geometry)
    mxbg = mxbg.to_crs(epsg=4326)
    chart = (
        alt.Chart(mxbg)
        .mark_geoshape(opacity=1)
        .encode(alt.Color("travel_time:Q", title="TT (min)").scale(scheme="viridis"))
        .project(type="mercator")
    )

    chart = (
        chart.properties(
            title=f"Auto Travel Times to Central {region_key} at {tod}",
            width=1800,
            height=1800,
        )
        .configure_legend(labelFontSize=20, titleFontSize=32, orient="top")
        .configure_title(fontSize=50)
    )

    chart.save(
        f"/home/willem/Documents/Project/TED/data/region/{region_key}/qc/auto/{region_key}_{tod}.png"
    )
