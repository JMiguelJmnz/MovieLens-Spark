from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import os, shutil

new_file_name = "top_movies.csv"

# Setting up paths
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
DATA_PATH = PROJECT_ROOT / "data" / "ml-latest-small"
output_folder = (PROJECT_ROOT / "result" / "top_movies")

# Data paths
movies_file = DATA_PATH / "movies.csv"
ratings_file = DATA_PATH / "ratings.csv"

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("MovieLens Analysis") \
    .getOrCreate()

# Load datasets
movies_df = spark.read.csv(str(movies_file), header=True, inferSchema=True)
ratings_df = spark.read.csv(str(ratings_file), header=True, inferSchema=True)

# Show schemas
print("Movies schema:")
movies_df.printSchema()

print("Ratings schema:")
ratings_df.printSchema()

# Show a few rows
print("Sample movies:")
movies_df.show(5, truncate = False)

print("Sample ratings:")
ratings_df.show(5, truncate = False)

# 10 most-rated movies
ratings_count = ratings_df.groupBy("movieId").count()
joined_df = ratings_count.join(movies_df, on="movieId")
sorted_df = joined_df.orderBy("count", ascending=False)
print("10 most-rated movies")
sorted_df.select("title", "count").show(10)


# Top rating movies
top_ratings = ratings_df.groupBy("movieId").agg(F.max("rating").alias("max_rating"))
top_movies = top_ratings.join(movies_df, on="movieId")
sorted_ratings = top_movies.orderBy("max_rating", ascending=False)
print("10 TOP rated movies")
sorted_ratings.select("title", "max_rating").show(10)

# Top rating movies removing low count rated movies
movie_stats = ratings_df.groupBy("movieId").agg(
    F.count("rating").alias("num_ratings"),
    F.avg("rating").alias("avg_rating")
)
popular_movies = movie_stats.filter(F.col("num_ratings") >= 50)
joined_movies = popular_movies.join(movies_df, on="movieId")
top_popular_movies = joined_movies.orderBy("avg_rating", ascending=False)
print("Top 10 movies with at least 50 ratings:")
top_popular_movies.select("title", "num_ratings", "avg_rating").show(10)

# Saving the results as CSV 
top_popular_movies.select("title", "num_ratings", "avg_rating") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(output_folder))  # Save CSV to the folder

# After Spark writes the files, rename the part file to a single CSV file
csv_files = [f for f in os.listdir(output_folder) if f.startswith("part-")]
if csv_files:
    part_file = csv_files[0]
    part_file_path = os.path.join(output_folder, part_file)
    new_file_name = "top_movies.csv"
    new_file_path = os.path.join(output_folder, new_file_name)
    
    # Move and rename the file
    shutil.move(part_file_path, new_file_path)

    print(f"File saved and renamed to: {new_file_path}")

spark.stop()