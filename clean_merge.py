import pandas as pd
import sqlite3
import numpy as np

conn_stats = sqlite3.connect("premier_league_stats.db")
conn_table = sqlite3.connect("premier_league_tabele_24_25.db")

df_stats = pd.read_sql("SELECT * FROM match_stats", conn_stats)
df_table = pd.read_sql("SELECT * FROM premier_league", conn_table)

for col in ["possession_home", "possession_away"]:
    df_stats[col] = df_stats[col].str.replace("%", "").astype(float)

numeric_cols = [col for col in df_stats.columns if any(word in col for word in ["score", "shots", "xg", "cards", "chances", "corners"])]
df_stats[numeric_cols] = df_stats[numeric_cols].apply(pd.to_numeric, errors="coerce")

home_agg = df_stats.groupby("home_team").mean(numeric_only=True).reset_index()
home_agg = home_agg.rename(columns=lambda x: "home_" + x if x != "home_team" else "team_name")

away_agg = df_stats.groupby("away_team").mean(numeric_only=True).reset_index()
away_agg = away_agg.rename(columns=lambda x: "away_" + x if x != "away_team" else "team_name")

team_stats = pd.merge(home_agg, away_agg, on="team_name", how="outer")

df_table["team_name"] = df_table["team_name"].str.replace("FC", "").str.strip()
team_stats["team_name"] = team_stats["team_name"].str.replace("FC", "").str.strip()

final_df = df_table.merge(team_stats, on="team_name", how="left")

final_df.to_csv("final_team_data_2024_2025.csv", index=False)
print("Dane zostały oczyszczone i zapisane w 'final_team_data_2024_2025.csv'")
