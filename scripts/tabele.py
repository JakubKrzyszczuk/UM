from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import sqlite3
import time

options = Options()
options.add_argument('--headless')
driver_path = 'C:/chromedriver/chromedriver-win64/chromedriver.exe'
driver = webdriver.Chrome(service=Service(driver_path), options=options)

url = "https://www.flashscore.pl/pilka-nozna/anglia/premier-league-2021-2022/tabela/#/6kJqdMr2/table/overall"
driver.get(url)

time.sleep(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')

driver.quit()

table = soup.find('div', {'class': 'ui-table__body'})

rows = []
for row in table.find_all('div', {'class': 'ui-table__row'}):
    rank = row.find('div', {'class': 'tableCellRank'}).text.strip()
    team = row.find('a', {'class': 'tableCellParticipant__name'}).text.strip()
    matches = row.find_all('span', {'class': 'table__cell--value'})[0].text.strip()
    wins = row.find_all('span', {'class': 'table__cell--value'})[1].text.strip()
    draws = row.find_all('span', {'class': 'table__cell--value'})[2].text.strip()
    losses = row.find_all('span', {'class': 'table__cell--value'})[3].text.strip()
    goals = row.find_all('span', {'class': 'table__cell--value'})[4].text.strip()
    goal_difference = row.find_all('span', {'class': 'table__cell--value'})[5].text.strip()
    points = row.find_all('span', {'class': 'table__cell--value'})[6].text.strip()

    rows.append([rank, team, matches, wins, draws, losses, goals, goal_difference, points])

db = sqlite3.connect("premier_league_tabele_21_22.db")
cursor = db.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS premier_league (
    rank INTEGER,
    team_name TEXT,
    matches_played INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    goals_for INTEGER,
    goal_difference INTEGER,
    points INTEGER
)
''')

for row in rows:
    cursor.execute('''
    INSERT INTO premier_league (rank, team_name, matches_played, wins, draws, losses, goals_for, goal_difference, points)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))

db.commit()

db.close()

print("Dane zostały zapisane do bazy danych.")
