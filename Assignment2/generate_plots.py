from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count,col,round
from datetime import date
from helpers.seaborn_helper import generate_horizontal_barplot
import os

spark = SparkSession.builder.master('local').appName("DvDRental").getOrCreate()

TRANSFORMED_DATA_FOLDER = os.path.join('transformed_data',str(date.today()))

fact_rental_df = spark.read.parquet(os.path.join(TRANSFORMED_DATA_FOLDER,'fact_rental.parquet'))
dim_film_df = spark.read.parquet(os.path.join(TRANSFORMED_DATA_FOLDER,'dim_film.parquet'))

# Queries
# 1. Top 10 most rented films
# 2. Revenue generated by Genre
# 

top_10_most_rented_movies = fact_rental_df.select('rental_id','film_id').join(
    dim_film_df.select('film_id','title'),
    'film_id',
    'inner'
).groupBy('title').agg(count('title').alias('rentals')).sort(col('rentals').desc()).limit(10).collect()

# Identify the top 10% of rentals contributing the most to total revenue 
# (consider rental cost, late fees, and membership tiers). 
# Analyze rental duration, movie genre, day of the week, and promotion offers to find patterns for boosting profitable rentals.

# print(fact_rental_df.columns)
# print(dim_film_df.show(5))

top_5_movies_most_revenue = fact_rental_df.select('film_id','amount').join(
    dim_film_df.select('film_id','title'),
    'film_id',
    'inner'
).drop('film_id').groupBy('title').agg(round(sum('amount'),2).alias('revenue')).sort(col('revenue').desc()).limit(5).collect()

top_10_most_rented_movies_df = spark.createDataFrame(top_10_most_rented_movies)
top_5_movies_most_revenue_df = spark.createDataFrame(top_5_movies_most_revenue)

# print(top_10_most_rented_movies_df.show())

generate_horizontal_barplot(
    data=top_10_most_rented_movies_df,
    x='rentals',
    y='title',
    x_label='Rentals',
    y_label='Movie Titles',
    title='Top 10 Most Rented Movies'
)

generate_horizontal_barplot(
    data=top_5_movies_most_revenue_df,
    x='revenue',
    y='title',
    x_label='Revenue in Thousands USD',
    y_label='Movie Titles',
    title='Top 5 Most Revenue Generating Movies'
)

