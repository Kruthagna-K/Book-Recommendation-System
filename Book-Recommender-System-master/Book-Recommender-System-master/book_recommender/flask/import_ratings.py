import os, csv
from flask import Flask, session
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from flask_sqlalchemy import SQLAlchemy

# database engine object from SQLAlchemy that manages connections to the database
engine = create_engine("postgresql://postgres:root@localHost:5432/Books")

# create a 'scoped session' that ensures different users' interactions with the
# database are kept separate
db = scoped_session(sessionmaker(bind=engine))

file = open("C:\\Users\\Suhas Jain\\Downloads\\Book-Recommender-System-master\\Book-Recommender-System-master\\ratings.csv")

reader = csv.reader(file)

# skip the first row of the CSV file (column names)
next(reader)

for row in reader:
    col_id = int(row[0])
    user_id = int(row[1])
    rating = int(row[2])
    book_id = int(row[3])
    username = row[4]
    isbn10 = row[5]
    
    statement = text("INSERT INTO ratings (col_id, user_id, rating, book_id, username, isbn10) VALUES (:col_id, :user_id, :rating, :book_id, :username, :isbn10)")
    db.execute(statement, {"col_id": col_id, "user_id": user_id, "rating": rating, "book_id": book_id, "username": username, "isbn10": isbn10})
    print(f"Added rating with ISBN: {book_id} and title: {username} to database.")
    db.commit()
