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
    search = request.args.get("search", "").strip()

    # Make search if else so that way if we are searching we are showing those results, else we show normal as before just pagination
    if search:
        query = """
        SELECT DISTINCT f.film_id, f.title, c.name AS category 
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.title LIKE %s
           OR c.name LIKE %s
           OR CONCAT(a.first_name, ' ', a.last_name) LIKE %s
        ORDER BY f.film_id ASC
        LIMIT %s OFFSET %s;
        """
        cur.execute(query, (f"%{search}%", f"%{search}%", f"%{search}%", perPage, offset))
        rows = cur.fetchall()

        cur.execute("""
        SELECT COUNT(DISTINCT f.film_id)
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN film_actor fa ON f.film_id = fa.film_id
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
        WHERE f.title LIKE %s
           OR c.name LIKE %s
           OR CONCAT(a.first_name, ' ', a.last_name) LIKE %s;
        """, (f"%{search}%", f"%{search}%", f"%{search}%"))
        total_films = cur.fetchone()[0]
    else:

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

# FILMS TABLE DETAILS -------------------------------------------------------------
# TO DO: Comments and look into rental count and 
@app.route("/film/<int:film_id>", methods=["GET"])
def filmDetails(film_id):
    cur = mysql.connection.cursor()
    
    cur.execute("""
        SELECT f.film_id, f.title, f.description, f.release_year, f.length, 
               f.rating, f.rental_rate, f.replacement_cost,
               l.name AS language, c.name AS category
        FROM film f
        JOIN language l ON f.language_id = l.language_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        WHERE f.film_id = %s
    """, (film_id,))
    
    # Getting ID for film to get actors
    film_row = cur.fetchone()
    cur.execute("""
        SELECT a.actor_id, a.first_name, a.last_name
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        WHERE fa.film_id = %s
        ORDER BY a.last_name ASC, a.first_name ASC
    """, (film_id,))
    
    actor_rows = cur.fetchall()
    
    return jsonify({
        "film": {
            "film_id": film_row[0],
            "title": film_row[1],
            "description": film_row[2],
            "release_year": film_row[3],
            "length": film_row[4],
            "rating": film_row[5],
            "rental_rate": float(film_row[6]),
            "replacement_cost": float(film_row[7]),
            "language": film_row[8],
            "category": film_row[9]
        },
        "actors": [
            {"actor_id": r[0], "first_name": r[1], "last_name": r[2]} 
            for r in actor_rows
        ]
    })

# CUSTOMERS TABLE -------------------------------------------------------------
@app.route("/customersTable", methods=["GET"])
def customersTable():
    cur = mysql.connection.cursor()
    perPage = 20
    page = int(request.args.get("page", 1))
    offset = (page - 1) * perPage
    search = request.args.get("search", "").strip()

    if search: 
        query = """
        SELECT customer_id, store_id, first_name, last_name, email, address_id, active, create_date, last_update
        FROM customer
        WHERE first_name LIKE %s
           OR last_name LIKE %s
           OR email LIKE %s
           OR CAST(customer_id AS CHAR) LIKE %s
        ORDER BY customer_id ASC
        LIMIT %s OFFSET %s;
        """
        cur.execute(query, (f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", perPage, offset))
        rows = cur.fetchall()

        count_query = """
        SELECT COUNT(*)
        FROM customer
        WHERE first_name LIKE %s
           OR last_name LIKE %s
           OR email LIKE %s
           OR CAST(customer_id AS CHAR) LIKE %s;
        """
        cur.execute(count_query, (f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%"))
        total_customers = cur.fetchone()[0]

    else:
        cur.execute("""
            SELECT customer_id, store_id, first_name, last_name, email, address_id, active, create_date, last_update
            FROM customer
            ORDER BY customer_id ASC
            LIMIT %s OFFSET %s
        """, (perPage, offset))

        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM customer;")
        total_customers = cur.fetchone()[0]
    cur.close()

    return jsonify({
        "current_page": page,
        "customers_per_page": perPage,
        "total": total_customers,
        "customers": [
            {
                "customer_id": r[0],
                "store_id": r[1],
                "first_name": r[2],
                "last_name": r[3],
                "email": r[4],
                "address_id": r[5],
                "active": bool(r[6]),
                "create_date": str(r[7]),
                "last_update": str(r[8])
            }
            for r in rows
        ]
    })

# CUSTOMERS TABLE ADD CUSTOMER -------------------------------------------------------------
@app.route("/customers", methods=["POST"])
def addCustomer():
    data = request.json
    first_name = data.get("first_name")
    last_name = data.get("last_name")
    email = data.get("email")
    store_id = data.get("store_id", 1)
    address_id = data.get("address_id", 1)
    active = data.get("active", True)

    if not first_name or not last_name or not email:
        return jsonify({"error": "Please fill all fields"}), 400

    cur = mysql.connection.cursor()
    # We put current date and time with NOW then commit
    cur.execute("""
        INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date)
        VALUES (%s, %s, %s, %s, %s, %s, NOW()) 
    """, (store_id, first_name, last_name, email, address_id, active))
    mysql.connection.commit()

    cur.execute("SELECT LAST_INSERT_ID();")
    customer_id = cur.fetchone()[0]
    cur.close()

    return jsonify({
        "customer_id": customer_id,
        "store_id": store_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "address_id": address_id,
        "active": active,
        "create_date": None,
        "last_update": None
    })

# DELETE CUSTOMER -------------------------------------------------------------
@app.route("/customers/<int:customer_id>", methods=["DELETE"])
def deleteCustomer(customer_id):
    cur = mysql.connection.cursor()
    cur.execute("DELETE FROM customer WHERE customer_id = %s", (customer_id,))
    mysql.connection.commit()
    cur.close()
    return jsonify({"message": "Customer has been successfully deleted"})

# RENT TO CUSTOMER --------------------------------------------------------------
@app.route("/rentFilm", methods=["POST"])
def rentFilm():
    data = request.json
    customer_id = data.get("customer_id")
    film_id = data.get("film_id")

    if not customer_id:
        return jsonify({"error" : "Customer ID does not exist!"}), 400
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT i.inventory_id
        FROM inventory i
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id AND r.return_date IS NULL
        WHERE i.film_id = %s AND r.rental_id IS NULL
        LIMIT 1;
    """, (film_id,))
    
    available = cur.fetchone()

    if not available: 
        cur.close()
        return jsonify({"error": "There are no more copies available to rent"})

    inventory_id = available[0]
    cur.execute("""
        INSERT INTO rental (rental_date, inventory_id, customer_id, return_date, staff_id)
        VALUES (NOW(), %s, %s, NULL, 1);
                """, (inventory_id, customer_id))
    
    mysql.connection.commit()

    return jsonify({"message" : "Film rented"})

# RETURN FILM --------------------------------------------------------------
@app.route("/returnFilm", methods=["POST"])
def returnFilm():
    data = request.json
    customer_id = data.get("customer_id")
    film_id = data.get("film_id")

    if not customer_id:
        return jsonify({"error" : "Customer ID does not exist!"}), 400
    cur = mysql.connection.cursor()
    cur.execute(""" 
        SELECT r.rental_id
        FROM rental r
        JOIN inventory i on r.inventory_id = i.inventory_id
        WHERE r.customer_id = %s AND i.film_id = %s AND r.return_date IS NULL
        LIMIT 1;
        """, (customer_id, film_id))
    rental = cur.fetchone()

    if not rental:
        cur.close()
        return jsonify({"error" : "This customer is not renting this film at this time"}), 400

    rental_id = rental[0]

    cur.execute("""
        UPDATE rental
        SET return_date = NOW()
        WHERE rental_id = %s;
        """, (rental_id,))
    mysql.connection.commit()

    return jsonify({"message" : "Film returned"})

# CUSTOMER RENTED FILMS -------------------------------------------------------------
@app.route("/customer/<int:customer_id>/rentedFilms", methods=["GET"])
def getRentedFilms(customer_id):
    cur = mysql.connection.cursor()
    query = """
        SELECT f.film_id, f.title, f.release_year, f.rating
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.customer_id = %s AND r.return_date IS NULL
        ORDER BY r.rental_date DESC;
    """
    cur.execute(query, (customer_id,))
    rows = cur.fetchall()
    cur.close()

    return jsonify([
        {"film_id": r[0], "title": r[1], "release_year": r[2], "rating": r[3]}
        for r in rows
    ])

if __name__ == "__main__":
    app.run(debug=True)