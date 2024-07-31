import pandas as pd
import os

base_dir = "data"
years = [2015, 2016, 2017, 2018, 2019]
all_data = []

for year in years:
    casualty_file = os.path.join(
        base_dir, f"road-crash-data-{year}", f"{year}_DATA_SA_Casualty.csv"
    )
    crash_file = os.path.join(
        base_dir, f"road-crash-data-{year}", f"{year}_DATA_SA_Crash.csv"
    )
    units_file = os.path.join(
        base_dir, f"road-crash-data-{year}", f"{year}_DATA_SA_Units.csv"
    )

    casualty_df = pd.read_csv(casualty_file)
    crash_df = pd.read_csv(crash_file)
    units_df = pd.read_csv(units_file)

    merged_df = pd.merge(crash_df, units_df, on="REPORT_ID", how="left")
    merged_df = pd.merge(
        merged_df,
        casualty_df,
        left_on=["REPORT_ID", "Unit No"],
        right_on=["REPORT_ID", "UND_UNIT_NUMBER"],
        how="left",
    )
    all_data.append(merged_df)

final_df = pd.concat(all_data, ignore_index=True)
final_df.to_csv("combined_road_crash_data_2015_2019.csv", index=False)
