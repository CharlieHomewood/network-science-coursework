from pathlib import Path
from statsbombpy import sb
from tqdm import tqdm
import polars as pl

def reformat_datetime_columns(df):

    for col in df.columns:
        if df[col].dtype == pl.Object:
            try:
                df = df.with_columns([
                    pl.col(col).str.strptime(pl.Datetime, fmt="%Y-%m")
                ])
            except Exception:
                pass
    
    return df

project_root = Path(__file__).resolve().parent.parent

competition_id = 37 # WSL Competition ID
season_with_match_ids_list = []


# Identify season_ids for the WSL competition.
competitions_df = sb.competitions()
season_ids = competitions_df[competitions_df["competition_id"] == competition_id]["season_id"].to_list()


# Collect matches data
matches_dfs = []
for season_id in season_ids:
    df = sb.matches(competition_id=competition_id, season_id=season_id)
    matches_dfs.append(df)

# Initialise list of dictionaries for each season.
for season_id in season_ids:

    first_item_key = list(sb.matches(competition_id=competition_id, season_id=season_id, fmt="dict").keys())[0]

    season_with_match_ids_list.append({
        "season_id": season_id,
        "season_name": sb.matches(competition_id=competition_id, season_id=season_id, fmt="dict")[first_item_key]["season"]["season_name"],
        "match_ids": list(sb.matches(competition_id=competition_id, season_id=season_id, fmt="dict").keys())
    })


# Create list of events dataframes.
events_dfs = []
for season in season_with_match_ids_list:
    for match_id in tqdm(season["match_ids"], desc=f"Processing Matching for the {season["season_name"]} WSL Season"):
        df = sb.events(match_id=match_id)
        df["season_id"] = season["season_id"]
        df["season_name"] = season["season_name"]
        events_dfs.append(df)


# Concatenate the list of dataframes and reformat datetime columns.
matches_df = pl.concat([pl.from_pandas(df) for df in matches_dfs], rechunk=True)
matches_df = reformat_datetime_columns(matches_df)

events_df = pl.concat([pl.from_pandas(df) for df in events_dfs], rechunk=True)
events_df = reformat_datetime_columns(events_df)


# Write to Parquet
matches_df.write_parquet(
    f"{project_root}/data/matches_data.parquet",
    compression="zstd"
)

events_df.write_parquet(
    f"{project_root}/data/events_data.parquet",
    compression="zstd"
)