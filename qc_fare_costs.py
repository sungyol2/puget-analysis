import pandas as pd
import geopandas as gpd
import altair as alt

region_key = "LA"
fares_2023 = False
INFINITE_FARE = 9999
CENTRAL_BGS = {
    "CHI": "170318390004",
    "WAS": "110010101002",
    "LA": "060372073062",
    "PHL": "421010005002",
    "NYC": "360610101001",
}

print("Loading Block Groups")
bgs = gpd.read_file(
    f"/home/willem/Documents/Project/TED/data/region/{region_key}/{region_key}.gpkg",
    layer="bg_areas",
)
print("Building Maps")
if fares_2023 == True:
    fare_years = ["2020", "2023"]
else:
    fare_years = ["2020"]
for fare_year in fare_years:
    print("  Fare year", fare_year)
    for matrix_type in ["full", "limited"]:
        print("    Matrix type:", matrix_type)
        matrix_file = f"/home/willem/Documents/Project/TED/data/region/{region_key}/fare/{fare_year}/fare_matrix_{fare_year}_{matrix_type}_BG20.parquet"
        mx = pd.read_parquet(matrix_file)
        mx = mx[mx.BG20_to == CENTRAL_BGS[region_key]]
        mx = mx[mx.fare_cost < INFINITE_FARE]
        mxbg = pd.merge(mx, bgs, left_on="BG20_from", right_on="BG20")[
            ["BG20", "fare_cost", "geometry"]
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
                labelFontSize=20,
                titleFontSize=32,
                orient="top",
                titleLimit=400,
                symbolLimit=400,
            )
            .configure_title(fontSize=50)
        )
        chart.save(
            f"/home/willem/Documents/Project/TED/data/region/{region_key}/qc/fares/fares_{region_key}_{matrix_type}_{fare_year}.png"
        )
        print("Chart Saved")
