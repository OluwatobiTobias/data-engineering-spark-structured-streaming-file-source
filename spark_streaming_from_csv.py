from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import sys

table_schema = StructType([StructField('s/no', IntegerType(), False),
                        StructField('date_employed', StringType(), False),
                        StructField('f_name', StringType(), False),
                        StructField('l_name', StringType(), False),
                        StructField('sex', StringType(), False),
                        StructField('data_profession', StringType(), False),
                        StructField('job_level', IntegerType(), False),
                        StructField('salary(in $)', IntegerType(), False),
                        StructField('country', StringType(), False),
                        StructField('email', StringType(), False),])

source_csv_path = 'source_csv/'

try:
    spark = SparkSession.builder.appName('streaming').master('local[*]').enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')


    try: 
        """READSTREAM BLOCK"""

        csv_source_stream = spark \
        .readStream \
        .format('csv') \
        .schema(table_schema) \
        .option('header', True) \
        .load(path=source_csv_path)
        
    except Exception as err:
        print(f'Error reading from csv_source_stream : \n', sys.exc_info())

    else:
        """TRANSFORMATION & WRITESTREAM BLOCK"""

        print('\n----- Is spark streaming: ', csv_source_stream.isStreaming, '\n')   
        csv_source_stream.printSchema()

        try:
            transformation_stream = csv_source_stream.createOrReplaceTempView('agg_table')
            agg_sql_query = spark.sql("""SELECT
                            t.data_profession,	t.sex, count(*) as sex_in_occupation
                            from agg_table t
                            group by t.data_profession, t.sex
                            order by t.data_profession;""")

        except Exception as err:
            print('Error in transformation:\n', err, sys.exc_info())

        else:
            socket_sink_stream = csv_source_stream.writeStream.queryName('streaming')\
            .format('console')\
            .outputMode('append')\
            .start(truncate=False)
            socket_sink_stream.awaitTermination(100)
            #.trigger(processingTime='1 second')\ "I removed this but it is used to set intervals between streams, since we ain't streaming much adding it neccesarily won't break our code, just make our terminal a little dirty"
            #.option('checkpointLocation', '<dir_path>') "This is used to keeps the logs that read the correct set of data files so that the exactly-once guarantee is maintained (i.e., no duplicate data or partial files are read)."

            agg_results = agg_sql_query.writeStream.queryName('aggregations')\
            .format('console')\
            .outputMode('complete')\
            .start(truncate=False)  
            agg_results.awaitTermination(30)

        finally:
            pass


    finally:
        pass

except Exception as err:
    print(f'\Error creating SparkSession: \n\n', err, sys.exc_info())


else:
    print('All went good.')
    print('\nStreaming Over.\n')


finally:
    spark.stop()
    print('SparkSession stopped!')
