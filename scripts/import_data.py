from pathlib import Path
from statsbombpy import sb
from tqdm import tqdm
import polars as pl

project_root = Path(__file__).resolve().parent.parent

competition_id = 37 # WSL Competition ID
season_with_match_ids_list = []


# Identify season_ids for the WSL competition.
competitions_df = sb.competitions()
season_ids = competitions_df[competitions_df["competition_id"] == competition_id]["season_id"].to_list()


# Initialise list of dictionaries for each season.
for season_id in season_ids:

    first_item_key = list(sb.matches(competition_id=competition_id, season_id=season_id, fmt="dict").keys())[0]

    season_with_match_ids_list.append({
        "season_id": season_id,
        "season_name": sb.matches(competition_id=competition_id, season_id=season_id, fmt="dict")[first_item_key]["season"]["season_name"],
        "match_ids": list(sb.matches(competition_id=competition_id, season_id=season_id, fmt="dict").keys())
    })


# Create list of events dataframes.
dfs = []
for season in season_with_match_ids_list:
    for match_id in tqdm(season["match_ids"], desc=f"Processing Matching for the {season["season_name"]} WSL Season"):
        df = sb.events(match_id=match_id)
        df["season_id"] = season["season_id"]
        df["season_name"] = season["season_name"]
        dfs.append(df)


# Concatenate the list of dataframes and save it as a parquet file.
pl_dfs = [pl.from_pandas(df) for df in dfs]

final_df = pl.concat(pl_dfs, rechunk=True)

for col in final_df.columns:
    if final_df[col].dtype == pl.Object:
        try:
            final_df = final_df.with_columns([
                pl.col(col).str.strptime(pl.Datetime, fmt="%Y-%m")
            ])
        except Exception:
            pass

# Write to Parquet
final_df.write_parquet(
    f"{project_root}/data/events_data.parquet",
    compression="zstd"
)