import findspark
findspark.init()

import psycopg2
from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.appName("PySparkPostgreSQL").getOrCreate()

# specify the database connection parameters
db_host = "localhost"
db_port = "5432"
db_name = "Books"
db_user = "postgres"
db_password = "root"

# load the PostgreSQL driver and create a connection object
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)

# create a cursor object
cur = conn.cursor()

# execute SQL query to read data from PostgreSQL table
cur.execute("SELECT * FROM ratings")

# convert result set to PySpark DataFrame
columns = [desc[0] for desc in cur.description]
data = cur.fetchall()
remote_table = spark.createDataFrame(data, columns)

# show the data
remote_table.show()

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

ratings_schema = StructType([
  StructField("col_id", IntegerType()),
  StructField("user_id", IntegerType()),
  StructField("rating", DoubleType()),
  StructField("book_id", IntegerType()),
  StructField("username", StringType()),
  StructField("isbn10", StringType())
])

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

remote_table.show()

(training, validation, test) = remote_table.randomSplit([0.6, 0.2, 0.2])

# caching data to cut down on cross-validation time later
training.cache()
validation.cache()
test.cache()

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

als_dt = ALS(maxIter=5, regParam=0.25, userCol="user_id", itemCol="book_id", ratingCol="rating", coldStartStrategy="drop", nonnegative = True, implicitPrefs = False) 

def tune_ALS(training, validation, maxIter, regParams, ranks, als_dt):
    min_error = float('inf')
    best_rank = -1
    best_regularization = 0
    best_model = None
    for rank in ranks:
        for reg in regParams:
            # get ALS model
            als = als_dt.setMaxIter(maxIter).setRank(rank).setRegParam(reg)
            # train ALS model
            model = als.fit(training)
            # evaluate the model by computing the RMSE on the validation data
            predictions = model.transform(validation)
            evaluator = RegressionEvaluator(metricName="rmse",
                                            labelCol="rating",
                                            predictionCol="prediction")
            rmse = evaluator.evaluate(predictions)
            print('{} latent factors and regularization = {}: '
                  'validation RMSE is {}'.format(rank, reg, rmse))
            if rmse < min_error:
                min_error = rmse
                best_rank = rank
                best_regularization = reg
                best_model = model
    print('\nThe best model has {} latent factors and '
          'regularization = {}'.format(best_rank, best_regularization))
    return best_model

tune_ALS(training, validation, 10, [.15, .20, .25], [10, 20, 50, 70], als_dt)

als = ALS(maxIter=10, regParam=0.20, userCol="user_id", itemCol="book_id", ratingCol="rating", coldStartStrategy="drop", nonnegative = True, implicitPrefs = False).setRank(50)
model = als.fit(training)
predictions = model.transform(test)
predictions.show(500)

# Generate n recommendations for all users
ALS_recommendations = model.recommendForAllUsers(numItems = 10)

# Temporary table
ALS_recommendations.registerTempTable("ALS_recs_temp")
clean_recs = spark.sql("""SELECT user_id,
                            bookIds_and_ratings.book_id AS book_id,
                            bookIds_and_ratings.rating AS prediction
                        FROM ALS_recs_temp LATERAL VIEW explode(recommendations) exploded_table AS bookIds_and_ratings""")

clean_recs.join(remote_table, ["user_id", "book_id"], "left").filter(remote_table.rating.isNull()).show()

clean_recs_filtered = clean_recs.select("user_id", "book_id", "prediction")

new_books = (clean_recs_filtered.join(remote_table, ["user_id", "book_id"], "left").filter(remote_table.rating.isNull()))

new_books_fnl = new_books.select('user_id', 'book_id', 'prediction')

new_books_users = new_books_fnl.filter(new_books_fnl['user_id'] > 53424)

new_books_use = new_books_users.select('user_id', 'book_id', 'prediction')


#new_books_use.write.option("truncate", "true").jdbc(url=url, table='new_recs', mode='overwrite', properties=properties)
# specify the database connection parameters

import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

# Replace the following values with your actual database credentials
db_host = "localhost"
db_port = "5432"
db_name = "Books"
db_user = "postgres"
db_password = "root"

# Create a connection to the PostgreSQL database
engine = create_engine("postgresql://postgres:root@localHost:5432/Books")

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("new_books_fnl_to_postgres") \
    .getOrCreate()

# Convert the PySpark DataFrame to a Pandas DataFrame
new_books_fnl_pd = new_books_fnl.toPandas()

# Write the Pandas DataFrame to the PostgreSQL table without the 'id' column
new_books_fnl_pd[['user_id', 'book_id', 'prediction']].to_sql(
    name="new_recs", con=engine, schema="public", if_exists="append", index=False
)

# Dispose the database connection
engine.dispose()

# Stop the Spark session
spark.stop()


