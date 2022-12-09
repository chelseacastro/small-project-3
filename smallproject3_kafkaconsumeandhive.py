from kafka import KafkaConsumer
import json
from time import sleep
from pyspark.sql import SparkSession

scSpark = SparkSession.builder.appName("Spark Hive").config("spark.sql.warehouse.dir").config("hive.metastore.uris", "thrift://{IP_Master_Hadoop}:9083").enableHiveSupport().getOrCreate()


consumer = KafkaConsumer('weather-report_sp3',
                         bootstrap_servers=['10.148.0.13:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for event in consumer:
    event_data = event.value
    data_input = event_data

    # print(event_data)

    insert_data = "('" + \
                  str(data_input['longitude']) + "','" + \
                  str(data_input['latitude']) + "','" + \
                  str(data_input['city_name']) + "','" + \
                  str(data_input['weather_status']) + "','" + \
                  str(data_input['weather_description']) + "','" + \
                  str(data_input['temperature']) + "','" + \
                  str(data_input['temperature_min']) + "','" + \
                  str(data_input['temperature_max']) + "','" + \
                  str(data_input['humidity']) + "','" + \
                  str(data_input['date']) + "','" + \
                  str(data_input['time']) + "','" + \
                  str(data_input['created']) + \
                  "')"

    print(insert_data)

    sql_command = "INSERT INTO weather_report values" + insert_data
    scSpark.sql(sql_command)
    sleep(5)
    # exit(259200)

scSpark.sql("SELECT * FROM weather_report").toPandas()
# scSpark.sql("SELECT * FROM weather_report ORDER BY temperature DESC").toPandas()
# scSpark.sql("SELECT * FROM weather_report WHERE weather_status='Clear'").toPandas()
