"""
ETL-Query script
"""

from lib.extract import extract
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

extract("https://raw.githubusercontent.com/laxmimerit/All-CSV-ML-Data-Files-Download/master/IMDB-Movie-Data.csv"\
        ,"/workspaces/week10-miniProj-zg105/movie_data.csv")

spark = SparkSession.builder.appName("IMDBAnalysis").getOrCreate()
# Define the schema based on your column names and data types
schema = StructType([
    StructField("Rank", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Genre", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Director", StringType(), True),
    StructField("Actors", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Runtime (Minutes)", IntegerType(), True),
    StructField("Rating", DoubleType(), True),
    StructField("Votes", IntegerType(), True),
    StructField("Revenue (Millions)", DoubleType(), True),
    StructField("Metascore", DoubleType(), True)
])

# Read the CSV file with the defined schema
df = spark.read.csv("movie_data.csv", header=True, schema=schema)
genre_avg_rating = df.groupBy("Genre").agg(
    col("Genre"),
    avg(col("Rating")).alias("AvgRating")
)
genre_avg_rating.show()
df.createOrReplaceTempView("movie_data")
df.show()
spark.stop()
