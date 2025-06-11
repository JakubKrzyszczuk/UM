import sqlite3
import pandas as pd

db_path = "../data/merged_all_seasons.db"

print(f"\n==================  BAZA: {db_path} ==================")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]
print(f"📦 Zawartość bazy danych: {tables}")

for table in tables:
    print(f"\n🔸 Tabela: {table}")

    cursor.execute(f"PRAGMA table_info({table});")
    for col in cursor.fetchall():
        print(f"   - {col[1]} ({col[2]})")

    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    print("\n📄 Wszystkie dane:")
    print(df.to_string(index=False))  # pełna tabela

conn.close()
