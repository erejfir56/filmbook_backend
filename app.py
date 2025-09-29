from flask import Flask, jsonify, request
from flask_mysqldb import MySQL
from flask_cors import CORS
from dotenv import load_dotenv
import os

# Used an enviornment to told MYSQL DB info and password
load_dotenv()

app = Flask(__name__)
CORS(app)

app.config['MYSQL_HOST'] = os.getenv("MYSQL_HOST")
app.config['MYSQL_USER'] = os.getenv("MYSQL_USER")
app.config['MYSQL_PASSWORD'] = os.getenv("MYSQL_PASSWORD")
app.config['MYSQL_DB'] = 'sakila'

mysql = MySQL(app)

#TOP FIVE MOVIES -------------------------------------------------------------
@app.route("/topFiveMovies", methods=["GET"])
def topFiveMovies():
    cur = mysql.connection.cursor()
    # SQL query so get the films attributes as well as pull the top five movies based on rental number 
    query = """
    SELECT f.title, f.rating, f.length, f.release_year, f.description, f.language_id, f.replacement_cost, f.rental_rate, f.special_features,
           COUNT(r.rental_id) AS rentals
    FROM film f
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    GROUP BY f.film_id
    ORDER BY rentals DESC, f.title ASC
    LIMIT 5;
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    return jsonify([
    {
        "title": row[0],
        "rating": row[1],
        "length": row[2],
        "release_year": row[3],
        "description": row[4],
        "language_id" : row[5],
        "replacement_cost": row[6],
        "rental_rate": row[7],
        "special_features": row[8],
        "rentals": row[9]
    }
    for row in rows
])

#-----------------------------------------------------------------------------

#TOP FIVE ACTORS -------------------------------------------------------------
@app.route("/topFiveActors", methods=["GET"])
def topFiveActors():
    cur = mysql.connection.cursor()
    # SQl query to get the actors id, first + last name and film count to display top five actors in the most movies. 
    query = """
    SELECT a.actor_id, a.first_name, a.last_name, COUNT(fa.film_id) AS film_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    GROUP BY a.actor_id
    ORDER BY film_count DESC, a.last_name ASC
    LIMIT 5;
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    return jsonify([
    {
        "actor_id": row[0],
        "first_name": row[1],
        "last_name": row[2],
        "film_count": row[3]
    }
    for row in rows
])


#ACTOR DETAILS + TOP FIVE FILMS -------------------------------------------------------------
@app.route("/actor/<int:actor_id>", methods=["GET"])
def actorDetails(actor_id):
    cur = mysql.connection.cursor()

    #SQL query to get the actors details 
    actor = """
    SELECT actor_id, first_name, last_name
    FROM actor
    WHERE actor_id = %s;
    """
    cur.execute(actor, (actor_id,))
    actor_row = cur.fetchone()

    if not actor_row:
        cur.close()
        return jsonify({"error": "Actor not found"}), 404

    # Get films details based on the actor, they're top five most rented films
    films = """
    SELECT f.title, COUNT(r.rental_id) AS rentals
    FROM film f
    JOIN film_actor fa ON f.film_id = fa.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    WHERE fa.actor_id = %s
    GROUP BY f.film_id
    ORDER BY rentals DESC, f.title ASC
    LIMIT 5;
    """
    cur.execute(films, (actor_id,))
    film_rows = cur.fetchall()

    cur.close()

    return jsonify({
        "actor": {
            "actor_id": actor_row[0],
            "first_name": actor_row[1],
            "last_name": actor_row[2]
        },
        "top_films": [ 
            {"title": row[0], "rentals": row[1]} for row in film_rows
        ]
    })

# FILMS TABLE -------------------------------------------------------------
@app.route("/filmsTable", methods=["GET"])
def filmsTable():
    cur = mysql.connection.cursor()

    # Makes 20 films show on each page and an offset so past pages and info don't repeat
    perPage = 20
    page = int(request.args.get("page", 1))
    offset = (page - 1) * perPage

    query = """
    SELECT f.film_id, f.title, c.name AS category
    FROM film f
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    ORDER BY f.film_id ASC
    LIMIT %s OFFSET %s;
    """

    # Query through and get the films for each page
    cur.execute(query, (perPage, offset))
    rows = cur.fetchall()
    # Gets all films number but because it returns as a tuple we take the first (0) index.
    cur.execute("SELECT COUNT(*) FROM film;")
    total_films = cur.fetchone()[0]


    return jsonify({
    "current_page": page,
    "films_per_page": perPage,
    "total": total_films,
    "films": [
        {"film_id": row[0], "title": row[1], "category": row[2]}
        for row in rows
    ] 
    })


if __name__ == "__main__":
    app.run(debug=True)