
from pyspark.sql import SparkSession 
  
>>> # Create Spark Session 
>>> sparkSession = SparkSession.builder.appName('rawdatacleaning').getOrCreate()                                                                                                           
24/04/06 07:01:21 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
>>> df_pyspark = sparkSession.read.csv( 
...     'Employee_Table.csv', 
...     header=True, 
...     inferSchema=True
>>> 
>>> df_pyspark = sparkSession.read.csv( 
...     'raw.csv', 
...     header=True, 
...     inferSchema=True
... ) 
>>> df_pyspark.printSchema()
root
 |-- First Name: string (nullable = true)
 |-- Last Name: string (nullable = true)
 |-- Age: integer (nullable = true)
 |-- Department: string (nullable = true)
 |-- Joining Year: string (nullable = true)

>>> # Dropping Entire rows containing Null  
>>> null_dropped=df_pyspark.na.drop() 
>>> null_dropped.show()
+----------+---------+---+----------+------------+
|First Name|Last Name|Age|Department|Joining Year|
+----------+---------+---+----------+------------+
|    Barack|    Trump| 40|        IT|        2012|
+----------+---------+---+----------+------------+

>>> # Dropping Rows where Joining Year is missing 
>>> non_null_year = df_pyspark.na.drop(subset=['Joining Year']) 
>>> non_null_year.show()
+----------+---------+---+----------+------------+
|First Name|Last Name|Age|Department|Joining Year|
+----------+---------+---+----------+------------+
|    Donald|    Biden| 30|      NULL|            |
|      NULL|     NULL| 25|      NULL|        2015|
|    Barack|    Trump| 40|        IT|        2012|
|    George|    Pence| 32|      NULL|        2020|
+----------+---------+---+----------+------------+

>>> # Fill Null values inside Department column with the word 'Generalist' 
>>> df_pyspark.na.fill('Generalist',subset=['Department']).show()
+----------+---------+---+----------------+------------+
|First Name|Last Name|Age|      Department|Joining Year|
+----------+---------+---+----------------+------------+
|    Donald|    Biden| 30|      Generalist|            |
|      NULL|     NULL| 25|      Generalist|        2015|
|    Barack|    Trump| 40|              IT|        2012|
|    George|    Pence| 32|      Generalist|        2020|
|      Mike|     NULL| 25|      Consulting|        NULL|
|      Mitt|     Bush|200|Information Tech|        NULL|
+----------+---------+---+----------------+------------+

>>> # Remove Outlier -- We assume 59 to be maximum working age in the company 
>>> df_pyspark = df_pyspark.filter('Age<60') 
>>> df_pyspark.show()
+----------+---------+---+----------+------------+
|First Name|Last Name|Age|Department|Joining Year|
+----------+---------+---+----------+------------+
|    Donald|    Biden| 30|      NULL|            |
|      NULL|     NULL| 25|      NULL|        2015|
|    Barack|    Trump| 40|        IT|        2012|
|    George|    Pence| 32|      NULL|        2020|
|      Mike|     NULL| 25|Consulting|        NULL|
+----------+---------+---+----------+------------+