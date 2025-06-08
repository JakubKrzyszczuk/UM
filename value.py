import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def parse_market_value(value_str):
    value_str = value_str.replace("€", "").strip()

    if "mld" in value_str:
        value_str = value_str.replace("mld", "").replace(",", ".").strip()
        return float(value_str) * 1000
    elif "mln" in value_str:
        value_str = value_str.replace("mln", "").replace(",", ".").strip()
        return float(value_str)
    else:
        return 0.0

url = "https://www.transfermarkt.pl/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=2021"
headers = {"User-Agent": "Mozilla/5.0"}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, "html.parser")

table = soup.find("table", class_="items")
rows = table.find("tbody").find_all("tr", recursive=False)

data = []

for row in rows:
    try:
        cols = row.find_all("td", recursive=False)
        if len(cols) < 7:
            continue

        team_name_tag = cols[1].find_all("a")
        team_name = team_name_tag[0].text.strip() if team_name_tag else "UNKNOWN"

        avg_age = float(cols[3].text.strip().replace(",", "."))
        market_value_avg = parse_market_value(cols[5].text)
        market_value_total = parse_market_value(cols[6].text)

        data.append({
            "team": team_name,
            "avg_age": avg_age,
            "market_value_avg_mil_eur": market_value_avg,
            "market_value_total_mil_eur": market_value_total
        })

    except Exception as e:
        print(f"Błąd przy przetwarzaniu wiersza: {e}")
        continue

df = pd.DataFrame(data)

db_conn = sqlite3.connect("team_market_values_2021_2022.db")
df.to_sql("team_market_values", db_conn, if_exists="replace", index=False)
db_conn.close()

