from flask import Flask, jsonify, render_template, request, redirect, session, url_for, flash
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash
import xmltodict
import urllib.request as urllib2
from urllib.parse import quote
import json
import os
from dotenv import load_dotenv
import schedule
import time
import subprocess
import sys

load_dotenv()
app = Flask(__name__)

CORS(app)
ENV = 'dev'

LOCAL_DB_URL = "postgresql://postgres:root@localHost:5432/Books"
REMOTE_DB_URL = os.getenv("REMOTE_DB_URL")
SECRET_KEY = "supersecretkey"

# Setting database configs
if ENV == 'dev':
    app.debug = True
    app.config['SQLALCHEMY_DATABASE_URI'] = LOCAL_DB_URL
else:
    app.debug = False
    app.config['SQLALCHEMY_DATABASE_URI'] = REMOTE_DB_URL

#app.config['SQL_ALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.config['SECRET_KEY'] = SECRET_KEY

db = SQLAlchemy(app)

#####################################################################

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        existing_user = User.query.filter_by(username=username).first()
        if existing_user:
            flash('Username already exists. Please choose a different one.', 'danger')
            return redirect(url_for('signup'))

        password_hash = generate_password_hash(password, method='sha256')
        new_user = User(username=username, password=password, password_hash=password_hash)
        db.session.add(new_user)
        db.session.commit()
        flash('Account created successfully. You can now log in.', 'success')
        return redirect(url_for('login'))
    return render_template('signup.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password_hash, password):
            flash('Login successful. Welcome, {}!'.format(username), 'success')
            # Assuming you have a 'home' route for logged-in users
            return redirect(url_for('home'))
        else:
            flash('Invalid username or password. Please try again.', 'danger')
    return render_template('login.html')

#####################################################################

# custom functions
def customid():
    idquery = db.session.query(Ratings).order_by(Ratings.col_id.desc()).first()
    last_id = int(idquery.col_id)
    next_id = int(last_id) + 1
    return next_id

def user_id(username):
    if db.session.query(Ratings).filter(Ratings.username == username).count() == 0:
        idquery = db.session.query(Ratings).order_by(Ratings.user_id.desc()).first()
        if idquery:
            last_id = int(idquery.user_id)
            next_id = int(last_id) + 1
            print(next_id)
            return next_id
        else:
            # Handle the case when there are no records in the Ratings table
            return 1
    else:
        idquery = db.session.query(Ratings).filter(Ratings.username == username).first()
        if idquery:
            return idquery.user_id
        else:
            # Handle the case when the username is not found in the Ratings table
            return None


def idcounter():
    iding = db.session.query(GrBook).order_by(GrBook.gr_id.desc()).first()
    last_id = int(iding.gr_id)
    next_id = int(last_id) + 1
    return next_id


def parse_xml(request_xml):
    content_dict = xmltodict.parse(request_xml)
    return jsonify(content_dict)


# Building user model
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(200))
    password = db.Column(db.String(200))
    password_hash = db.Column(db.String(200))

    def __init__(self, username, password, password_hash):
        self.username = username
        self.password = password
        self.password_hash = password_hash

# Building ratings model
class Ratings(db.Model):
    __tablename__ = 'ratings'
    col_id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    rating = db.Column(db.Integer)
    book_id = db.Column(db.Integer)
    username = db.Column(db.String(200))
    isbn10 = db.Column(db.String(200))

    def __init__(self, col_id, user_id, rating, book_id, username, isbn10):
        self.col_id = col_id
        self.user_id = user_id
        self.rating = rating
        self.book_id = book_id
        self.username = username
        self.isbn10 = isbn10


# Building the new recommendations model
class NewRecs(db.Model):
    __tablename__ = 'new_recs'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    book_id = db.Column(db.Integer)
    prediction = db.Column(db.Float)

    def __init__(self, userid, book_id, prediction):
        self.user_id = user_id
        self.book_id = book_id
        self.prediction = prediction


# Building the lookup between book_id and gr_book_id
class GrBook(db.Model):
    __tablename__ = 'gr_books'
    gr_id = db.Column(db.Integer, primary_key=True)
    book_id = db.Column(db.Integer, nullable=False)
    isbn10 = db.Column(db.String(200), nullable=False)


    def __init__(self, gr_id, book_id, isbn10):
        self.gr_id = gr_id
        self.book_id = book_id
        self.isbn10 = isbn10


def login_user(user):
    session['username'] = user.username
    session['user_id'] = user.id


################################
# Building routes for the site #
################################

# Home page
@app.route('/')
def index():
    return render_template("base.html")


# registration page
@app.route('/register', methods=["GET", "POST"])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password_hash = generate_password_hash(password)

        if username == '' or password == '':
            return render_template('register.html', message='Please include a name and/or password.')

        if db.session.query(User).filter(User.username == username).count() == 0:
            user = User(username, password, password_hash)
            db.session.add(user)
            db.session.commit()
            
            # Log the user in after successful registration
            login_user(user)
            return redirect(url_for('get_profile'))
        else:
            return render_template('register.html', message='Sorry, this username is already taken.')

    else:
        return render_template('register.html')




# sign-in page
@app.route('/sign-in', methods=["GET", "POST"])
def sign_in():
    if request.method == 'POST':
        username_entered = request.form['username']
        password_entered = request.form['password']
        user = db.session.query(User).filter(User.username == username_entered).first()
        if user is not None and check_password_hash(user.password_hash, password_entered):
            login_user(user)
            return redirect(url_for('get_profile'))
        return render_template('signin.html',
                               message="Sorry, either your username does not exist or your password does not match.")
    else:
        return render_template('signin.html')


# sign-out page
@app.route('/sign-out', methods=["GET", "POST"])
def sign_out():
    if request.method == 'POST':
        session.pop('username', None)
        return redirect(url_for('sign_in'))
    else:
        return render_template('signout.html')


# loads the user profile
@app.route('/profile', methods=['GET', 'POST'])
def get_profile():
    if request.method == 'GET':
        userid = user_id(session.get('username'))
        username = session.get('username')
        ratings = db.session.query(Ratings).filter(Ratings.user_id == userid).all()

        ratings_list = []
        for i in ratings:
            gr_bookid = db.session.query(GrBook).filter(GrBook.gr_id == i.book_id).first().book_id
            ratings_list.append([gr_bookid, i.rating])

        bk = []
        for i in ratings_list:
            response_string = 'https://www.goodreads.com/book/show?id=' + str(i[0]) + '&key=Ev590L5ibeayXEVKycXbAw'
            xml = urllib2.urlopen(response_string)
            data = xml.read()
            xml.close()
            data = xmltodict.parse(data)
            gr_data = json.dumps(data)
            goodreads_fnl = json.loads(gr_data)
            gr = goodreads_fnl['GoodreadsResponse']['book']
            bk.append(dict(id=gr['id'], book_title=gr['title'], image_url=gr['image_url'], rating=i[1]))

        book = dict(work=bk)
        return render_template('profile.html', recs=book, username=username)
    else:
        return 'Error in login process...'


# search books page
@app.route("/search", methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        response_string = 'https://www.goodreads.com/search/index.xml?format=xml&key=Ev590L5ibeayXEVKycXbAw&q=' + quote(
            request.form.get("title"))
        xml = urllib2.urlopen(response_string)
        data = xml.read()
        xml.close()
        data = xmltodict.parse(data)
        gr_data = json.dumps(data)
        goodreads_fnl = json.loads(gr_data)
        gr = goodreads_fnl['GoodreadsResponse']['search']['results']

        if not request.form.get("title"):
            return ("Please enter a book title below.")

        return render_template("searchResults.html", books=gr)

    else:
        username = session.get('username')
        return render_template("search.html", username=username)


# book details route
@app.route("/bookDetails/<book_id>")
def bookDetails(book_id):
    response_string = 'https://www.goodreads.com/book/show?id=' + book_id + '&key=Ev590L5ibeayXEVKycXbAw'
    xml = urllib2.urlopen(response_string)
    data = xml.read()
    xml.close()
    data = xmltodict.parse(data)
    gr_data = json.dumps(data)
    goodreads_fnl = json.loads(gr_data)
    gr = goodreads_fnl['GoodreadsResponse']['book']
    return render_template("bookDetails.html", book=gr)


# submitting new book ratings
@app.route("/new-rating", methods=['POST'])
def postnew():
    if request.method == 'POST':
        col_id = customid()
        userid = user_id(session.get('username'))
        rating = request.form['rating']
        book_id = request.form.get('bookid')
        username = session.get('username')
        isbn10 = request.form.get('isbn10')

        if db.session.query(GrBook).filter(GrBook.book_id == book_id).count() == 0:
            gr_id = idcounter()
            grdata = GrBook(gr_id, book_id, isbn10)
            db.session.add(grdata)
            db.session.commit()
        else:
            grfilter = db.session.query(GrBook).filter(GrBook.book_id == book_id).first()
            gr_id = grfilter.gr_id

        data = Ratings(col_id, userid, rating, gr_id, username, isbn10)
        db.session.add(data)
        db.session.commit()
        return render_template('success.html')


# getting book recommendations
@app.route("/recs", methods=['GET'])
def getrecs():
    if request.method == 'GET':
        userid = user_id(session.get('username'))
        recs = db.session.query(NewRecs).filter(NewRecs.user_id == userid).all()
        # print(recs)
        recs_list = []
        for i in recs:
            gr_bookid = db.session.query(Ratings).filter(Ratings.book_id == i.book_id).first().book_id
            recs_list.append(gr_bookid)
            
        bk = []
        for i in recs_list:
            response_string = 'https://www.goodreads.com/book/show?id=' + str(i) + '&key=Ev590L5ibeayXEVKycXbAw'
            xml = urllib2.urlopen(response_string)
            data = xml.read()
            xml.close()
            data = xmltodict.parse(data)
            gr_data = json.dumps(data)
            goodreads_fnl = json.loads(gr_data)
            gr = goodreads_fnl['GoodreadsResponse']['book']
            bk.append(dict(id=gr['id'], book_title=gr['title'], image_url=gr['image_url']))

        book = dict(work=bk)
        return render_template('recs.html', recs=book)
    else:
        return "No Data"



def create_database():
    db.create_all()

#running recommendation_algorithm every 30 min 
def run_recommendation_algorithm():
    print("Running recommendation_algorithm.py...")
    sys.stdout.flush()
    subprocess.call(["python", "C:\\Users\\Suhas Jain\\Downloads\\Book-Recommender-System-master\\Book-Recommender-System-master\\recommendation_algorithm.py"])

# Schedule the recommendation_algorithm.py to run every 30 minutes
schedule.every(30).minutes.do(run_recommendation_algorithm)

def run_scheduled_tasks():
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    with app.app_context():
        create_database()

    from threading import Thread

    # Start a new thread for running scheduled tasks
    task_thread = Thread(target=run_scheduled_tasks)
    task_thread.start()
    app.run(debug=True,use_reloader=False)

