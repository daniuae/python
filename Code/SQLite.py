import sqlite3
conn = sqlite3.connect("your_database.db")  # Creates the file if it doesn't exist
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS movie(title, year, score)")
conn.commit()
cur.execute("INSERT INTO movie VALUES ('Title', 2025, 9.5)")
conn.commit()
cur.execute("SELECT * FROM movie")
rows = cur.fetchall()
print(rows)
conn.close()
