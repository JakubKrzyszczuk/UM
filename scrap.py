from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import time

options = Options()
options.add_argument('--headless')
driver_path = 'C:/chromedriver/chromedriver-win64/chromedriver.exe'
driver = webdriver.Chrome(service=Service(driver_path), options=options)

db = sqlite3.connect("premier_league_stats_21_22.db")
cursor = db.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS match_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        home_team TEXT,
        away_team TEXT,
        home_score INTEGER,
        away_score INTEGER,
        xg_home REAL,
        xg_away REAL,
        possession_home TEXT,
        possession_away TEXT,
        total_shots_home INTEGER,
        total_shots_away INTEGER,
        shots_on_target_home INTEGER,
        shots_on_target_away INTEGER,
        big_chances_home INTEGER,
        big_chances_away INTEGER,
        corners_home INTEGER,
        corners_away INTEGER,
        yellow_cards_home INTEGER,
        yellow_cards_away INTEGER
    )
''')
db.commit()

driver.get("https://www.flashscore.pl/pilka-nozna/anglia/premier-league-2021-2022/wyniki/")
time.sleep(1.5)

for _ in range(20):
    try:
        show_more = driver.find_element(By.CLASS_NAME, "event__more")
        if show_more.is_displayed():
            driver.execute_script("arguments[0].click();", show_more)
            time.sleep(0.8)
        else:
            break
    except:
        break

data = []
matches = driver.find_elements(By.CLASS_NAME, "event__match")
for match in matches:
    try:
        home = match.find_element(By.CSS_SELECTOR, ".event__homeParticipant [data-testid='wcl-scores-simpleText-01']").text
        away = match.find_element(By.CSS_SELECTOR, ".event__awayParticipant [data-testid='wcl-scores-simpleText-01']").text
        home_score = match.find_element(By.CLASS_NAME, "event__score--home").text
        away_score = match.find_element(By.CLASS_NAME, "event__score--away").text
        link = match.find_element(By.CLASS_NAME, "eventRowLink").get_attribute("href")
        match_id = link.split("/")[-3]
        data.append((home, away, int(home_score), int(away_score), match_id))
    except:
        continue

for home, away, h_score, a_score, match_id in data:
    try:
        stats_url = f"https://www.flashscore.pl/mecz/pilka-nozna/{match_id}/#/szczegoly-meczu/statystyki-meczu/0"
        driver.get(stats_url)
        time.sleep(1.5)

        try:
            WebDriverWait(driver, 2.5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "wcl-category_7qsgP"))
            )
        except:
            continue

        stats = {}
        desired = [
            "Oczekiwane gole (xG)",
            "Posiadanie piłki",
            "Strzały łącznie",
            "Strzały na bramkę",
            "Wielkie szanse",
            "Rzuty rożne",
            "Żółte kartki"
        ]

        blocks = driver.find_elements(By.CSS_SELECTOR, "[data-testid='wcl-statistics']")
        for b in blocks:
            try:
                cat = b.find_element(By.CLASS_NAME, "wcl-category_7qsgP").text.strip()
                if cat in desired:
                    home_val = b.find_element(By.CLASS_NAME, "wcl-homeValue_-iJBW").text.strip()
                    away_val = b.find_element(By.CLASS_NAME, "wcl-awayValue_rQvxs").text.strip()
                    stats[cat] = (home_val, away_val)
            except:
                continue

        try:
            xg_home, xg_away = map(float, stats.get("Oczekiwane gole (xG)", ("0", "0")))
            possession_home, possession_away = stats.get("Posiadanie piłki", ("0%", "0%"))
            total_shots_home, total_shots_away = map(int, stats.get("Strzały łącznie", ("0", "0")))
            shots_on_target_home, shots_on_target_away = map(int, stats.get("Strzały na bramkę", ("0", "0")))
            big_chances_home, big_chances_away = map(int, stats.get("Wielkie szanse", ("0", "0")))
            corners_home, corners_away = map(int, stats.get("Rzuty rożne", ("0", "0")))
            yellow_cards_home, yellow_cards_away = map(int, stats.get("Żółte kartki", ("0", "0")))

            print(f"INSERT: {home} vs {away} | Score: {h_score}-{a_score} | "
                  f"xG: {xg_home}-{xg_away} | Possession: {possession_home}-{possession_away} | "
                  f"Shots: {total_shots_home}-{total_shots_away} | On Target: {shots_on_target_home}-{shots_on_target_away} | "
                  f"Big Chances: {big_chances_home}-{big_chances_away} | "
                  f"Corners: {corners_home}-{corners_away} | "
                  f"Yellows: {yellow_cards_home}-{yellow_cards_away}")
        except:
            continue

        cursor.execute('''
            INSERT INTO match_stats (
                home_team, away_team, home_score, away_score,
                xg_home, xg_away,
                possession_home, possession_away,
                total_shots_home, total_shots_away,
                shots_on_target_home, shots_on_target_away,
                big_chances_home, big_chances_away,
                corners_home, corners_away,
                yellow_cards_home, yellow_cards_away
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            home, away, h_score, a_score,
            xg_home, xg_away,
            possession_home, possession_away,
            total_shots_home, total_shots_away,
            shots_on_target_home, shots_on_target_away,
            big_chances_home, big_chances_away,
            corners_home, corners_away,
            yellow_cards_home, yellow_cards_away
        ))
        db.commit()

    except:
        continue

driver.quit()
db.close()