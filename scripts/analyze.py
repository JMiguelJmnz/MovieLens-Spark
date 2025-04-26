from pyspark.sql import SparkSession
from pathlib import Path

# Setting up paths
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
DATA_PATH = PROJECT_ROOT / "data" / "ml-latest-small"

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

# 10 most-rated movies
ratings_count = ratings_df.groupBy("movieId").count()
joined_df = ratings_count.join(movies_df, on="movieID")
sorted_df = joined_df.orderBy("count", ascending=False)
print("10 most-rated movies")
sorted_df.select("title", "count").show(10)

# Top rating movies
top_ratings = ratings_df.groupBy("movieID")["rating"].max()
top_movies = top_ratings.join(movies_df, on="movieID")
sorted_ratings = top_movies.orderBy("rating", ascending=False)
print("10 TOP rated movies")
sorted_ratings.select("title", "rating").show(10)

# Show a few rows
print("Sample movies:")
movies_df.show(5, truncate = False)

print("Sample ratings:")
ratings_df.show(5, truncate = False)

spark.stop()