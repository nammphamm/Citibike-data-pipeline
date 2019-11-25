# Build a simple data pipeline with Spark
This project is designed to build a simple data pipeline to ingest Citibike data. Citibike trip
data is archived every month on Citibke [website](https://www.citibikenyc.com/system-data). The
project is inspired by Todd Scheineider's [repo](https://github.com/toddwschneider/nyc-citibike-data).

In the original repo, Todd leverages the uses of Unix command to download, extract data and copy to 
local Postgres database. In this project, we aim to use Spark to leverage its capability to ingest,
transform and load data. Spark will especially be more useful when dealing with a larger amount of data. 

# Requirements
```
Spark
Postgres
Python
```

### Step 1
Run `download_raw_data.sh` to download files from Citibike website and extract it to local `data` folder

### Step 2
Run `initialize_database.sh` to create schema for Postgres database to store the trips data

### Step 3
Run `build_pipeline.py` to ingest the csv data into our local Postgres database. Within this python script, it's helpful 
to walk over some of the codes to make some modification as needed.

Our first step is to initialize the Spark session. I set the file path and then called .read.csv to read the CSV file. 
The parameters are self-explanatory. The spark postgres driver can be downloaded so that we can interface with Postgres later.
The .cache() caches the returned resultset hence increase the performance. 
```
if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .config("spark.driver.extraClassPath", "Analysis/postgresql-42.2.8.jar")\
        .getOrCreate()
``` 

Next we want Spark to load the csv files that we downloaded from step 1. In order to do that, we specify
the location of the files. All of the files follow similar pattern, so we can simply use the asterisk to 
read all files that ends with data with the type csv
```
data_file = 'data/*data.csv'
```

Next, we will use the Spark session to read in the csv files. We set the option to infer schema automatically,
so that it can interpret the data type. I also changed the column name here so that it matches the database schema we 
created in step 2.

In the next step, we can perform some basic transformation to ensure data quality.
For example, we can see the breakdown of the gender variable

```
print("Analyzing gender information")
gender = sdfData.groupBy('gender').count()
print(gender.show())

+------+------+
|gender| count|
+------+------+
|     1|511479|
|     2|157006|
|     0|174931|
+------+------+
```

Here we can see that there are three variables for gender, is the value 0 supposed to be missing, if that's 
the case, should it be N/A instead?
```
# change 0 to null
sdfData = sdfData.withColumn("gender", when(col('gender') != "0", col("gender")))

+------+------+
|gender| count|
+------+------+
|  null|174931|
|     1|511479|
|     2|157006|
+------+------+
```

We can also check if there is any negative duration in our data
```
print(sdfData.filter(sdfData.trip_duration < 0).count())

0
```

Another cool thing we can do is to apply sql queries on DataFrame
First, we need to register DataFrame as table

```
print("Register to temp table trips")
sdfData.registerTempTable("trips")
```
We then can use Spark context to query directly from data

```
output = scSpark.sql('SELECT * FROM trips LIMIT 10')
output.show()
```

Finally, we will want to have some ways to load this data into our Postgres database. Using ODBC driver,
we can use Spark's `write` module to transfer data to our Postgres

```
output.write.format('jdbc').options(
    url="jdbc:postgresql://localhost:5432/nyc-citibike-data",
    driver='org.postgresql.Driver',
    dbtable='trips_raw',
    user='user').mode('append').save()
```
