import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

URL = "jdbc:postgresql://localhost:5432/pagila"
CONFIG = {"user": "postgres", "password": "mysecretpassword", "Driver": "org.postgresql.Driver"}

spark = SparkSession.builder.config("spark.jars", "postgresql-42.6.0.jar").appName("pagila").getOrCreate()

film_category = spark.read.jdbc(URL, "film_category", properties=CONFIG)
category = spark.read.jdbc(URL, "category", properties=CONFIG)
payment = spark.read.jdbc(URL, "payment", properties=CONFIG)
rental = spark.read.jdbc(URL, "rental", properties=CONFIG)
inventory = spark.read.jdbc(URL, "inventory", properties=CONFIG)
film_actor = spark.read.jdbc(URL, "film_actor", properties=CONFIG)
actor = spark.read.jdbc(URL, "actor", properties=CONFIG)
film = spark.read.jdbc(URL, "film", properties=CONFIG)
customer = spark.read.jdbc(URL, "customer", properties=CONFIG)
address = spark.read.jdbc(URL, "address", properties=CONFIG)
city = spark.read.jdbc(URL, "city", properties=CONFIG)


# Вывести количество фильмов в каждой категории, отсортировать по убыванию.
films_in_category = (
    film_category.drop("last_update")
    .groupBy("category_id")
    .agg(f.count("category_id").alias("films_in_category"))
    .join(category, film_category["category_id"] == category["category_id"])
    .select(category["category_id"], category["name"], f.col("films_in_category"))
    .orderBy("films_in_category", ascending=False)
)


# Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
actor_films_rented = (
    payment.join(rental, rental["rental_id"] == payment["rental_id"])
    .join(inventory, rental["inventory_id"] == inventory["inventory_id"])
    .join(film_actor, film_actor["film_id"] == inventory["film_id"])
    .groupBy(film_actor["actor_id"])
    .agg(f.count(film_actor["film_id"]).alias("films_rented"))
    .orderBy("films_rented", ascending=True)
    .limit(10)
    .join(actor, actor["actor_id"] == film_actor["actor_id"])
    .select([actor["actor_id"], actor["first_name"], actor["last_name"], f.col("films_rented")])
)


# Вывести категорию фильмов, на которую потратили больше всего денег.
best_category = (
    payment.join(rental, rental["rental_id"] == payment["rental_id"])
    .join(inventory, rental["inventory_id"] == inventory["inventory_id"])
    .join(film_category, inventory["film_id"] == film_category["film_id"])
    .groupBy(film_category["category_id"])
    .agg(f.sum(payment["amount"]).alias("amount_spent"))
    .orderBy("amount_spent", ascending=False)
    .limit(1)
    .join(category, film_category["category_id"] == category["category_id"])
    .select([category["category_id"], category["name"], f.col("amount_spent")])
)


# Вывести названия фильмов, которых нет в inventory.
not_in_inventory_films = film.join(inventory, film["film_id"] == inventory["film_id"], how="anti").select(film["title"])


# Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
# Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
counted_roles = (
    category.where(category["name"] == "Children")
    .join(film_category, film_category["category_id"] == category["category_id"])
    .join(film_actor, film_category["film_id"] == film_actor["film_id"])
    .groupBy(film_actor["actor_id"])
    .agg(f.count(film_actor["film_id"]).alias("roles_in_films"))
    .join(actor, actor["actor_id"] == film_actor["actor_id"])
    .select([actor["actor_id"], actor["first_name"], actor["last_name"], f.col("roles_in_films")])
)

roles_max_counts = counted_roles.select(counted_roles["roles_in_films"]).distinct().orderBy(counted_roles["roles_in_films"], ascending=False).limit(3)

children_category_actors = (
    counted_roles.join(roles_max_counts, roles_max_counts["roles_in_films"] == counted_roles["roles_in_films"])
    .drop(roles_max_counts["roles_in_films"])
    .orderBy([counted_roles["roles_in_films"], counted_roles["actor_id"]], ascending=False)
)


# Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
# Отсортировать по количеству неактивных клиентов по убыванию.
customers_in_cities = (
    customer.join(address, address["address_id"] == customer["address_id"])
    .join(city, city["city_id"] == address["city_id"])
    .groupBy(city["city_id"])
    .agg(f.count(f.when(customer["active"] == 1, 1)).alias("active"), f.count(f.when(customer["active"] == 0, 1)).alias("not_active"))
    .orderBy("not_active", ascending=False)
)


# Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
# и которые начинаются на букву “a”.
# Тоже самое сделать для городов в которых есть символ “-”.
rental_data = (
    rental.where(rental["return_date"].isNotNull())
    .join(customer, rental["customer_id"] == customer["customer_id"])
    .join(address, customer["address_id"] == address["address_id"])
    .join(city, address["city_id"] == city["city_id"])
    .where(city["city"].contains("-"))
    .join(inventory, rental["inventory_id"] == inventory["inventory_id"])
    .join(film_category, inventory["film_id"] == film_category["film_id"])
    .join(category, film_category["category_id"] == category["category_id"])
    .where(f.lower(category["name"]).startswith("a"))
    .select(city["city_id"], city["city"], category["category_id"], category["name"], rental["return_date"], rental["rental_date"])
)

rental_time_window = Window.partitionBy([rental_data["city_id"], rental_data["category_id"]])

rental_time = rental_data.withColumn("rent_time", f.sum(rental_data["return_date"] - rental_data["rental_date"]).over(rental_time_window))

max_rental_time_window = Window.partitionBy(rental_time["city_id"])

max_rental_time = (
    rental_time.withColumn("max_rental_time", f.max(rental_time["rent_time"]).over(max_rental_time_window))
    .where(f.col("max_rental_time") == rental_time["rent_time"])
    .select(rental_time["city_id"], rental_time["city"], rental_time["category_id"], rental_time["name"], f.col("max_rental_time"))
    .distinct()
    .withColumnRenamed("max_rental_time", "rental_time")
    .orderBy(rental_time["city_id"], rental_time["rent_time"])
)
