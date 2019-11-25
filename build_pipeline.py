from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when, lit, col


if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .config("spark.driver.extraClassPath", "Analysis/postgresql-42.2.8.jar")\
        .getOrCreate()


data_file = 'data/2013-07 - Citi Bike trip data.csv'
sdfData = scSpark.read.option("inferSchema", True).csv(data_file, header=True, sep=",").toDF("trip_duration", "start_time","stop_time",\
                                                                 "start_station_id", "start_station_name",\
                                                                 "start_station_latitude", "start_station_longitude",\
                                                                 "end_station_id", "end_station_name",\
                                                                 "end_station_latitude", "end_station_longitude",\
                                                                 "bike_id", "user_type", "birth_year", "gender").cache()
print('Total Records = {}'.format(sdfData.count()))
sdfData.show()


print("Analyzing gender information")
gender = sdfData.groupBy('gender').count()
print(gender.show())

# change 0 to null
sdfData = sdfData.withColumn("gender", when(col('gender') != "0", col("gender")))
gender = sdfData.groupBy('gender').count()
print(gender.show())

# check any negative duration
print(sdfData.filter(sdfData.trip_duration < 0).count())

sdfData.show()

"""
print("Analyzing usertype information")
print(sdfData.groupBy('user_type').count().show())


print("Max duration for person on this day")
output = scSpark.sql('SELECT gender, usertype, MAX(tripduration) as max_duration from trips GROUP BY gender, usertype')
output.show()


print("Register to temp table trips")
sdfData.registerTempTable("trips")


output = scSpark.sql('SELECT * FROM trips LIMIT 10')
output.show()
output.write.format('jdbc').options(
    url="jdbc:postgresql://localhost:5432/nyc-citibike-data",
    driver='org.postgresql.Driver',
    dbtable='trips_raw',
    user='nampham').mode('append').save()
"""