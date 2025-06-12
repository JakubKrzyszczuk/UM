import sqlite3
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import joblib

conn = sqlite3.connect("../data/merged_all_seasons.db")
df = pd.read_sql("SELECT * FROM all_merged_data", conn)
conn.close()

cols_needed = [
    "xg_home", "total_shots_home", "corners_home", "yellow_cards_home",
    "home_market_value_avg_mil_eur", "home_avg_age",
    "goal_difference_home", "points_home", "rank_home"
]
df = df.dropna(subset=cols_needed)

team_season_df = df.groupby(["team_name_home", "points_home"]).agg({
    "xg_home": "mean",
    "total_shots_home": "mean",
    "corners_home": "mean",
    "yellow_cards_home": "mean",
    "home_market_value_avg_mil_eur": "mean",
    "home_avg_age": "mean",
    "goal_difference_home": "max",
    "rank_home": "max"  
}).reset_index()

X = team_season_df.drop(columns=["rank_home", "team_name_home"])
y = team_season_df["rank_home"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=40)

model = RandomForestRegressor(n_estimators=100, random_state=40)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"\n Liczba drużyn w zbiorze: {len(team_season_df)}")
print(f" MAE (błąd średni): {mae:.2f}")
print(f" R² score: {r2:.2f}")

# 8. Zapisanie modelu
joblib.dump(model, "../models/team_rank_predictor.pkl")
print(" Model zapisany jako 'team_rank_predictor.pkl'")
