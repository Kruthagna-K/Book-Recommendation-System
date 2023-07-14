import pandas as pd
import psycopg2

# Replace the following values with your actual database credentials
db_host = "localhost"
db_port = "5432"
db_name = "Books"
db_user = "postgres"
db_password = "root"

# Replace the following value with the path to your CSV file
csv_file_path = "C:\\Users\\Suhas Jain\\Downloads\\Book-Recommender-System-master\\Book-Recommender-System-master\\book_recommender\\data\\full_book.csv"

# Read the CSV file into a Pandas DataFrame
csv_data = pd.read_csv(csv_file_path)

# Connect to the database
conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
cur = conn.cursor()

# Iterate through the rows of the CSV data and insert them into the gr_books table
for index, row in csv_data.iterrows():
    gr_id = row["r_index"]
    book_id = row["book_id"]
    isbn10 = row["isbn"]
    
    try:
        cur.execute("INSERT INTO public.gr_books (gr_id, book_id, isbn10) VALUES (%s, %s, %s);", (gr_id, book_id, isbn10))
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        print(f"Skipping duplicate gr_id: {gr_id}")
        continue

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
