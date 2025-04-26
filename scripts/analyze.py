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

# Shor a few rows
print("Sample movies:")
movies_df.show(5, truncate = False)

print("Sample ratings:")
ratings_df.show(5, truncate = False)

spark.stop()