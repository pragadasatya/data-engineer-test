# data-engineer-test
This is for prospective data engineers interviewing at Holman.

# Directions:
1. Fork this repo to your personal Github account.
2. Complete the below data exercises in the best way you see fit. Your project should be shareable with a link prior to your 2nd round interview. Here are some potential implementation options:

- Develop within a PySpark Project: https://github.com/AlexIoannides/pyspark-example-project 
- Use a Jupyter notebook: https://jupyter.org/ or https://colab.google/ 
- Implement a local database: 
    - https://www.prisma.io/dataguide/postgresql/setting-up-a-local-postgresql-database
    - https://www.prisma.io/dataguide/mysql/setting-up-a-local-mysql-database 

## Olympics
Build a pipeline that turns multiple .csv files into a singular data object.
1. Reference the data within `./datasets/olympics`. 
2. Combine all files in this directory into a single consummable artifact, such as a database table, a parquet file, or a .csv file. Dataframes are appreciated, but the final object must be query-able in a programmatic fashion. 
3. The above task should produce code that includes a schema defintion, transformation logic, and data upserts.

## Countries
Build a pipeline that turns a single .csv files into a singular data object.
1. Reference the data within `./datasets/countries`. 
2. Using the `countries of the world.csv`, create a database table, a parquet file, or a .csv file. Dataframes are appreciated, but the final object must be query-able in a programmatic fashion.
3. The above task should produce code that includes a schema defintion, transformation logic, and data upserts.

## Combing Olympics and Countries
Transform one or both of the olympics/countries tables to facilitate a join across these tables. 

1. Normalize the data by applying a foreign key(s) to one/both tables. The key should be unique to represent 1 country, and ensure no cartesian joins occur. (We are aware that no true key exists, and an artificial key(s) will need to be produced)
2. Transform the 2 data objects via denormalization, and deliver 1 consummable artifact in the form of a table, parquet, or something query-able.
