import pandas as pd
import geopandas as gpd
import altair as alt

region_key = "NYC"
week_of = "2023-09-25"
fares_2023 = False
CENTRAL_BGS = {
    "CHI": "170318390004",
    "WAS": "110010101002",
    "LA": "060372073062",
    "PHL": "421010005002",
}

print("Loading Block Groups")
bgs = gpd.read_file(
    f"/home/willem/Documents/Project/TED/data/region/{region_key}/{region_key}.gpkg",
    layer="bg_areas",
)
print("Building Maps")
for tod in ["SATAM", "WEDAM", "WEDPM"]:
    print("Travel time maps for:", tod, week_of)
    print("  Transit")
    for matrix_type in ["full", "limited"]:
        print("    Matrix type:", matrix_type)
        matrix_file = f"/home/willem/Documents/Project/TED/data/results/{week_of}-{region_key}/{region_key}/{tod}/{matrix_type}_matrix.parquet"
        mx = pd.read_parquet(matrix_file)
        mx = mx[mx.to_id == CENTRAL_BGS[region_key]]
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
                title=f"{matrix_type.capitalize()} {week_of} Transit Travel Times to Central {region_key} at {tod}",
                width=1800,
                height=1800,
            )
            .configure_legend(
                labelFontSize=20, titleFontSize=32, orient="top", titleLimit=400
            )
            .configure_title(fontSize=50)
        )
        chart.save(
            f"/home/willem/Documents/Project/TED/data/region/{region_key}/qc/travel_times/transit_{region_key}_{week_of}_{tod}_{matrix_type}.png"
        )
        print("Chart Saved")

    if fares_2023 == True:
        fare_years = ["2020", "2023"]
    else:
        fare_years = ["2020"]
    for fare_year in fare_years:
        print("  Fare year", fare_year)
        for matrix_type in ["full", "limited"]:
            print("    Matrix type:", matrix_type)
            matrix_file = f"/home/willem/Documents/Project/TED/data/{region_key}/fare/{fare_year}/fare_matrix_{fare_year}_{matrix_type}_BG20.parquet"
            mx = pd.read_parquet(matrix_file)
            mx = mx[mx.to_id == CENTRAL_BGS[region_key]]
            mxbg = pd.merge(mx, bgs, left_on="from_id", right_on="BG20")[
                ["BG20", "travel_time", "geometry"]
            ]
            mxbg = gpd.GeoDataFrame(mxbg, geometry=mxbg.geometry)
            mxbg = mxbg.to_crs(epsg=4326)
            chart = (
                alt.Chart(mxbg)
                .mark_geoshape(opacity=1)
                .encode(
                    alt.Color("fare_cost:Q", title="Fare (cents)")
                    .scale(scheme="viridis")
                    .legend(orient="top")
                )
                .project(type="mercator")
            )

            chart = (
                chart.properties(
                    title=f"{matrix_type.capitalize()} Fares to Central {region_key}",
                    width=1800,
                    height=1800,
                )
                .configure_legend(
                    labelFontSize=20, titleFontSize=32, orient="top", titleLimit=400
                )
                .configure_title(fontSize=50)
            )
            chart.save(
                f"/home/willem/Documents/Project/TED/data/region/{region_key}/qc/travel_times/transit_{region_key}_{week_of}_{tod}_{matrix_type}.png"
            )
            print("Chart Saved")
