
import mysql.connector
import csv



conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='on_the_movie'
)
cursor = conn.cursor()
csv_file_path = 'movies_corretto.csv'
with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) 
    for row in reader:
        if len(row) != 4:
            continue
        movie_id, title, genres, year = row
        query = """
        INSERT INTO film (MovieID, Title, Genres, year)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (int(movie_id), title, genres, int(year)))

        
conn.commit()
cursor.close()
conn.close()
print("Importazione completata con successo!")


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="on_the_movie"
)
cursor = conn.cursor()
csv_file = "users_edit.csv"
with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader) 
    for row in reader:
        try:
            cursor.execute("""
                INSERT INTO Utenti (id, gender, eta, cap_utente, lavoro_utente)
                VALUES (%s, %s, %s, %s, %s)
            """, row)
        except mysql.connector.Error as e:
            print(f"Errore su riga {row}: {e}")
conn.commit()
cursor.close()
conn.close()
print("Dati inseriti correttamente.")



conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="on_the_movie"
)
cursor = conn.cursor()
csv_file = "ratings_edit.csv"

with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)  # Salta l'intestazione
    for row in reader:
        try:
            user_id = int(row[0])
            movie_id = int(row[1])
            rating = int(row[2])
            timestamp = int(row[3])
            cursor.execute("""
                INSERT INTO ratings (userId, movieId, rating, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (user_id, movie_id, rating, timestamp))
        except mysql.connector.Error as e:
            print(f"Errore su riga {row}: {e}")
        except ValueError:
            print(f"Valori non validi in riga: {row}")
# Commit e chiusura
conn.commit()
cursor.close()
conn.close()
print("Dati inseriti correttamente.")


