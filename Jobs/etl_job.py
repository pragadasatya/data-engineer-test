from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

def main():
  """
  Main ETL script definition.

    :return: None
  """
  # start Spark application and get Spark session, logger and config
  spark, log, config = start_spark(
      app_name='my_etl_job',
      files=['configs/etl_config.json'])
  
  # log that main ETL job is starting
  log.warn('etl_job is up-and-running')
  
  # To execute end to end ETL pipeline

  # To extract countries data with schema definition
  df_countries = extract_countries_data(spark)
  
  # To tranform the countries data wih Id generation and data cleaning 
  df_countries_trasformed = transform_countries_data(df_countries)
  
  # To save transformed countries data into csv file
  load_countries_data(df_countries_trasformed)
  print('transformed countries with Ids loaded')
  
  # To extract olympics data
  df_olympics = extract_olympics_data(spark)

  # To extract iso country codes (3 leter codes) to normalize with olympics data 
  df_country_codes= extract_country_three_letter_codes(spark)

  # To normalize olympics data reference to country id by mapping 3 letter codes in both data sets
  df_olympics_normalized = normalize_olympics_with_country_id_reference(
            spark,df_countries_trasformed,df_country_codes,df_olympics)
  
  #To save the normalized olymics data into csv file
  load_normalized_olympics_data(df_olympics_normalized)
  print('normalized olymics data  loaded')

  # To get the denormalized or report data to disply country,region and population wise olympics data
  df_denormalized_data = denormalize_countries_olympics_data(
                    spark,df_countries_trasformed,df_olympics_normalized)
  

  # To save the denormalized data into csv file
  load_denormalized_data(df_denormalized_data)
  print('denormalized data loaded')



  # log the success and terminate Spark application
  log.warn('test_etl_job is finished')
  spark.stop()
  return None
   

def extract_countries_data(spark):
  Schema = StructType([
    StructField("Country", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Population", IntegerType(), True),
    StructField("AreaPerSqMi", IntegerType(), True),
    StructField("PopDensityPerSqMi", StringType(), True),
    StructField("CoastlineOfCoastOrAreaRatio", StringType(), True),
    StructField("NetMigration", StringType(), True),
    StructField("InfantMortality", StringType(), True),
    StructField("GDPPerCapita", StringType(), True),
    StructField("LiteracyPercent", StringType(), True),
    StructField("PhonesPerThousand", StringType(), True),
    StructField("ArablePercent", StringType(), True),
    StructField("CropsPercent", StringType(), True),
    StructField("OtherPercent", StringType(), True),
    StructField("Climate", StringType(), True),
    StructField("Birthrate", StringType(), True),
    StructField("Deathrate", StringType(), True),
    StructField("Agriculture", StringType(), True),
    StructField("Industry", StringType(), True),
    StructField("Service", StringType(), True)
])
  df = spark.read.csv("datasets/countries/*.csv", schema=Schema, header = True)
  return df


def transform_countries_data(df):
  # to exclude fields
  excluded_fields = ["Country", "Region", "Population", "AreaPerSqMi"]
  # replace comma values with dot operator and cast them to double
  for field in df.schema.fields:
    if isinstance(field.dataType, StringType) and field.name not in excluded_fields:
      df = df.withColumn(
            field.name,
            regexp_replace(field.name, ",", ".").cast("double")
        )
  df_DedupeCountries = df.dropDuplicates()
  windowSpec = Window.orderBy(monotonically_increasing_id())
  df_with_countryIds = df_DedupeCountries.withColumn("country_id", row_number().over(windowSpec))
  return df_with_countryIds

def load_countries_data(df_countries_trasformed):
  df_countries_trasformed.write.option("header", True).csv("datasets/output/countries_with_ids", mode="overwrite")
  return None

def extract_olympics_data(spark):
  # Olympics
  Schema = StructType([
      StructField("NOC", StringType(), True),
      StructField("Gold", IntegerType(), True),
      StructField("Silver", IntegerType(), True),
      StructField("Bronze", IntegerType(), True),
      StructField("Total", IntegerType(), True)
  ])
  df = spark.read.csv("datasets/olympics/*.csv", schema=Schema, header = True)
  return df

def extract_country_three_letter_codes(spark):
  df = spark.read.csv("datasets/countries/iso-country-codes.csv", inferSchema=True, header = True)
  df = df.withColumnRenamed("Alpha-3-code", "3_Letter_Code")
  return df

def normalize_olympics_with_country_id_reference(spark,df_countries,df_country_codes,df_olympics):
  df_countries.createOrReplaceTempView("vw_countries_Ids")
  df_country_codes.createOrReplaceTempView("vw_country_codes_3_letters")
  df_olympics.createOrReplaceTempView("vw_Olympics")

  df_olympics_with_country_Ids_reference = spark.sql("""
      select c.country_id,o.*
      from vw_countries_Ids  c 
      inner join vw_country_codes_3_letters cc
      on RTRIM(c.Country) = RTRIM(cc.Country)
      inner join  vw_Olympics o
      on  RTRIM(o.NOC) = RTRIM(cc.3_Letter_Code)
    """)
  return df_olympics_with_country_Ids_reference

def load_normalized_olympics_data(df_normalized_olympics):
  df_normalized_olympics.write.option("header", True).csv("datasets/output/olympics_with_country_Ids_ref", mode="overwrite")
  return None

def denormalize_countries_olympics_data(spark,df_tranformed_countries,df_normalized_olympics):

  df_tranformed_countries.createOrReplaceTempView("transformed_countries")
  df_normalized_olympics.createOrReplaceTempView("normalized_olympics")
  
  #Denormalized data
  df_denormalized_data = spark.sql("""
  select 
      tc.country_id,tc.Country,no.NOC,tc.Region,tc.Population,
      SUM(no.Gold) as GOLD,
      SUM(no.Silver) as Silver,
      SUM(no.Bronze) as Bronze,
      SUM(no.Total) as Total
  from
  transformed_countries tc
  inner join normalized_olympics no
  ON tc.country_id = no.country_id
  group by tc.country_id,tc.Country,no.NOC,tc.Region,tc.Population
  """)
  return df_denormalized_data

def load_denormalized_data(df_Denormalized):
  df_Denormalized.write.option("header", True).csv("datasets/output/Denormalized_countries_Olympics", mode="overwrite")
  return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
