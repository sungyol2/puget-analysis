import pandas as pd
import geopandas as gpd
import altair as alt

REGION = "SFO"
RUN_CATALOG_PATH = "/home/willem/Documents/Project/TED/data/run_catalog.csv"
CENTRAL_BGS = {
    "CHI": "170318390004",
    "WAS": "110010101002",
    "LA": "060372073062",
    "PHL": "421010005002",
    "BOS": "250250203041",
    "SFO": "060750117003",
    "NYC": "360610101001",
}

print("Loading Block Groups")
bgs = gpd.read_file(
    f"/home/willem/Documents/Project/TED/data/region/{REGION}/{REGION}.gpkg",
    layer="bg_areas",
)

run_catalog = pd.read_csv(RUN_CATALOG_PATH)
run_catalog = run_catalog[run_catalog.week_of == "2020-08-17"]

for idx, run in run_catalog.iterrows():
    week_of = run["week_of"]
    for tod in ["SATAM", "WEDAM", "WEDPM"]:
        print("Travel time maps for:", tod, week_of)
        print("  Transit")
        for matrix_type in ["full", "limited"]:
            print("    Matrix type:", matrix_type)
            matrix_file = f"/home/willem/Documents/Project/TED/data/results/{week_of}-{REGION}/{REGION}/{tod}/{matrix_type}_matrix.parquet"
            mx = pd.read_parquet(matrix_file)
            mx = mx[mx.to_id == CENTRAL_BGS[REGION]]
            mxbg = pd.merge(mx, bgs, left_on="from_id", right_on="BG20")[
                ["BG20", "travel_time", "geometry"]
            ]
            mxbg = gpd.GeoDataFrame(mxbg, geometry=mxbg.geometry)
            mxbg = mxbg.to_crs(epsg=4326)
            chart = (
                alt.Chart(mxbg)
                .mark_geoshape(opacity=1)
                .encode(
                    alt.Color("travel_time:Q", title="TT (min)")
                    .scale(scheme="viridis")
                    .legend(orient="top")
                )
                .project(type="mercator")
            )

            chart = (
                chart.properties(
                    title=f"{matrix_type.capitalize()} {week_of} Transit Travel Times to Central {REGION} at {tod}",
                    width=1800,
                    height=1800,
                )
                .configure_legend(
                    labelFontSize=20, titleFontSize=32, orient="top", titleLimit=400
                )
                .configure_title(fontSize=50)
            )
            chart.save(
                f"/home/willem/Documents/Project/TED/data/region/{REGION}/qc/travel_times/transit_{REGION}_{week_of}_{tod}_{matrix_type}.png"
            )
            print("Chart Saved")
