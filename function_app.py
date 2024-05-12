import azure.functions as func
import logging
from dotenv import load_dotenv
import os
import mysql.connector
from datetime import datetime as dt
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

def connect_to_database():
    cnx = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )

    return cnx

@app.route(route="create_movie")
def create_movie(req: func.HttpRequest) -> func.HttpResponse:
    cnx = connect_to_database()

    cursor = cnx.cursor(dictionary=True)

    request_body = req.get_json()

    title = request_body['title']
    release_year = request_body['release_year']
    genre = request_body['genre']
    description = request_body['description']
    director = request_body['director']
    actors = request_body['actors']

    logging.info(title)


    cursor.execute("INSERT INTO movie VALUES(%s, %s, %s, %s, %s, %s, NULL)", (title, release_year, genre, description, director, actors, ))

    cnx.commit()

    cursor.execute("SELECT * FROM movie WHERE title=%s", (title, ))
    new_movie = cursor.fetchone()

    cursor.close()
    cnx.close()

    return json.dumps(new_movie)


@app.route(route="create_review", auth_level=func.AuthLevel.FUNCTION)
def create_review(req: func.HttpRequest) -> func.HttpResponse:
    cnx = connect_to_database()

    cursor = cnx.cursor(dictionary=True)

    request_body = req.get_json()

    title = request_body['title']
    opinion = request_body['opinion']
    rating = request_body['rating']
    datetime = dt.strptime(str(request_body['datetime']), '%d-%m-%Y-%H:%M')
    author = request_body['author']

    logging.info(title)


    cursor.execute("INSERT INTO review VALUES(default, %s, %s, %s, %s, %s)", (title, opinion, int(rating), datetime, author, ))

    cnx.commit()

    cursor.execute("SELECT title, opinion, rating, `datetime`, author FROM review WHERE title=%s AND datetime=%s AND author=%s", (title, datetime, author, ))
    new_rating = cursor.fetchone()

    new_rating['datetime'] = str(new_rating['datetime'])

    cursor.close()
    cnx.close()

    return json.dumps(new_rating)

def get_movie_reviews(cursor, title):
    cursor.execute("SELECT opinion, rating, `datetime`, author FROM review WHERE title=%s", (title, ))
    reviews = cursor.fetchall()

    for review in reviews:
        review['datetime'] = str(review['datetime'])

    return reviews

def get_movie_with_reviews(cursor, title):
    cursor.execute("SELECT * FROM movie WHERE title=%s", (title, ))
    movie = cursor.fetchone()
    movie['reviews'] = get_movie_reviews(cursor, title)

    return json.dumps(movie)

def get_all_movies_with_reviews(cursor):
    cursor.execute("SELECT * FROM movie")
    movies = cursor.fetchall()

    for movie in movies:
        movie['reviews'] = get_movie_reviews(cursor, movie['title'])

    return json.dumps(movies)

@app.route(route="search_movie", auth_level=func.AuthLevel.FUNCTION)
def search_movie(req: func.HttpRequest) -> func.HttpResponse:
    cnx = connect_to_database()

    cursor = cnx.cursor(dictionary=True)

    title = req.params.get('title')

    if title:
        result = get_movie_with_reviews(cursor, title)
    else:
        result = get_all_movies_with_reviews(cursor)

    cursor.close()
    cnx.close()

    return result        

@app.timer_trigger(schedule="0 30 11 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def calculate_average_rating(myTimer: func.TimerRequest) -> None:
    cnx = connect_to_database()

    cursor = cnx.cursor()

    cursor.execute("SELECT title FROM movie")

    movie_titles = [movie_tuple[0] for movie_tuple in cursor.fetchall()]

    for title in movie_titles:
        cursor.execute("SELECT rating FROM review WHERE title=%s", (title, ))
        ratings = [rating_tuple[0] for rating_tuple in cursor.fetchall()]

        average_rating = round(sum(ratings) / len(ratings), 1)

        cursor.execute("UPDATE movie SET average_rating=%s WHERE title=%s", (average_rating, title, ))
        cnx.commit()

    cursor.close()
    cnx.close() 