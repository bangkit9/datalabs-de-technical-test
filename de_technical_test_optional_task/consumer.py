from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import logging

logging.getLogger("py4j").setLevel(logging.ERROR)
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def start_streaming_aggregator(spark, topic_name, bootstrap_servers):
    
    schema = StructType([
        StructField("date_time", StringType()),
        StructField("city_id", IntegerType()),
        StructField("city_name", StringType()),
        StructField("device_name", StringType()),
        StructField("P_watt", IntegerType()),
        StructField("V_volt", IntegerType()),
        StructField("I_amphere", FloatType())
    ])

    stream_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "latest")
        .load())

    parsed_stream_df = stream_df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", expr("to_timestamp(date_time)"))

    
    aggregated_df = (parsed_stream_df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "1 minute")
        )
        .agg(
            count("*").alias("sensor_reading_count")
        )
    )
    
    final_df = aggregated_df.select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        col("sensor_reading_count")
    ).orderBy(col("window_start").desc())

    query = (final_df.writeStream
        .outputMode("complete") 
        .format("console")
        .option("truncate", False) 
        .trigger(processingTime="1 minute")
        .start())

    log.info("Menunggu agregasi data sensor per menit... (Hasil akan muncul di sini)")
    query.awaitTermination()


if __name__ == "__main__":
    KAFKA_SPARK_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

    try:
        spark_conn = (SparkSession.builder
            .appName("SensorStreamAggregator")
            .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
            .getOrCreate())
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        log.info("Koneksi Spark Session berhasil dibuat.")

    except Exception as e:
        log.error("Gagal membuat Spark Session.")
        log.error("Pastikan Anda memiliki Java (JDK) terinstal dengan benar.")
        log.error(f"Error: {e}")
        exit()

    topic_name = 'sensor_topic'
    bootstrap_servers = 'localhost:9092'

    try:
        log.info("Memulai listener streaming...")
        start_streaming_aggregator(spark_conn, topic_name, bootstrap_servers)
    except Exception as e:
        log.error(f"Streaming gagal: {e}")