# Hadoop
 In this project, you will analyze real-time data from Twitter using Apache Spark Streaming. The idea is to ingest tweets in real-time, perform basic analytics, and visualize the data.
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize SparkSession
spark = SparkSession.builder.appName("MovieRatings").getOrCreate()

# Load MovieLens dataset
ratings_file_path = "path_to_movielens_data.csv"
ratings_df = spark.read.option("header", "true").csv(ratings_file_path)

# Show the first few rows of the dataset
ratings_df.show()

# Convert ratings to numeric values for analysis
ratings_df = ratings_df.withColumn("rating", ratings_df["rating"].cast("float"))

# Calculate the average rating for each movie
average_ratings = ratings_df.groupBy("movieId").agg(avg("rating").alias("avg_rating"))

# Show the top 10 movies with the highest average ratings
top_rated_movies = average_ratings.orderBy("avg_rating", ascending=False)
top_rated_movies.show(10)

# Save results to CSV
top_rated_movies.write.option("header", "true").csv("output_top_rated_movies.csv")

spark.stop()
