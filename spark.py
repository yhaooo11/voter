from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


if __name__ == '__main__':
    # init spark session
    spark = (SparkSession.builder.appName('RealtimeVoting')
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') # spark kafka integration
             .config('spark.jars', '/Users/yifanhao/projects/realtime-voting/postgresql-42.7.1.jar') # postgres driver
             .config('spark.sql.adaptive.enable', 'false') # disable adaptive query execution
             .getOrCreate())
    
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

    votes_df = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', 'votes_topic')
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), vote_schema).alias('data'))
                .select('data.*'))

    # data preprocessing: typecasting and watermarking
    votes_df = votes_df.withColumn('voting_time', col('voting_time').cast(TimestampType())).withColumn('vote', col('vote').cast(IntegerType()))
    # discard any event that is processed later than 1 minute after voting time
    enriched_votes_df = votes_df.withWatermark('voting_time', '1 minute')

    # The resulting votes_per_candidate DataFrame will have a schema with columns 'candidate_id', 'candidate_name', 'party', 'photo_url', and 'total_votes'.
    votes_per_candidate = enriched_votes_df.groupBy('candidate_id', 'candidate_name', 'party', 'photo_url').agg(_sum('vote').alias('total_votes'))

    turnout_by_location = enriched_votes_df.groupBy('address.state').count().alias('total_votes')

    total_votes = enriched_votes_df.agg(_sum('vote').alias('total_votes'))

    total_votes_to_kafka = (total_votes.selectExpr('to_json(struct(*)) AS value')
                            .writeStream
                            .format('kafka')
                            .option('kafka.bootstrap.servers', 'localhost:9092')
                            .option('topic', 'aggregated_total_votes')
                            .option('checkpointLocation', '/Users/yifanhao/projects/realtime-voting/checkpoints/checkpoint1')
                            .outputMode('update')
                            .start())

    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) AS value') # The result is a DataFrame with a single column named 'value' containing JSON-encoded strings.
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_votes_per_candidate')
                                    .option('checkpointLocation', '/Users/yifanhao/projects/realtime-voting/checkpoints/checkpoint2')
                                    .outputMode('update')
                                    .start())
    
    turnout_by_location_to_kafka = (turnout_by_location.selectExpr('to_json(struct(*)) AS value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_turnout_by_location')
                                    .option('checkpointLocation', '/Users/yifanhao/projects/realtime-voting/checkpoints/checkpoint3')
                                    .outputMode('update')
                                    .start())
    
    # blocks current thread until streaming ends
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()
    total_votes_to_kafka.awaitTermination()


'''
In Spark Structured Streaming, the defined operations such as watermarking, aggregation, and other transformations are part of a query plan. 
When you express these operations in your Spark Structured Streaming code, Spark internally builds a query plan based on the transformations you've specified. 
This plan represents the logical flow of data processing.

Here's a simplified overview of how Spark Structured Streaming works with these operations when new data comes in:

1. **Query Plan:**
   - When you define your Spark Structured Streaming code, you're essentially specifying a series of DataFrame transformations that form a logical query plan. 
   This plan includes operations like reading from a source, applying transformations, and writing to sinks.

2. **Incremental Processing:**
   - Spark Structured Streaming operates on the principle of incremental processing. Instead of processing the entire dataset each time, it processes only the new data that has arrived since the last batch or micro-batch.

3. **Watermarking:**
   - Watermarking is a technique used in streaming processing to handle late-arriving data. When you specify a watermark in Spark Structured Streaming (e.g., `withWatermark('voting_time', '1 minute')`), 
   you're telling Spark to maintain state for a certain duration beyond the event time. Events that arrive after this watermark are considered late and may be discarded.

4. **Aggregation:**
   - Aggregation operations, such as `groupBy` and `agg`, are part of the logical query plan. When new data arrives, Spark incrementally updates the aggregates based on the grouping keys. 
   This incremental processing is more efficient than recomputing aggregates for the entire dataset.

5. **Checkpointing:**
   - Checkpointing plays a crucial role in ensuring fault tolerance and maintaining state. Periodically, Spark Structured Streaming checkpoints the current state of the streaming query. 
   This includes information about the progress of the query, the offsets of processed data, and metadata about transformations.

6. **Triggering:**
   - Spark Structured Streaming operates in micro-batches or continuous mode, depending on the trigger specified (e.g., `.trigger(processingTime='5 seconds')`). 
   The trigger determines how often the streaming job processes data and updates the query state.

7. **Data Source Ingestion:**
   - The streaming source, in this case, Kafka, continuously provides new data. Spark Structured Streaming ingests this data based on the trigger interval.

8. **Result Sink:**
   - The results of the query, after applying all transformations and aggregations, are written to the specified sink, in this case, Kafka topics ('aggregated_votes_per_candidate' and 'aggregated_turnout_by_location').

By combining these elements, Spark Structured Streaming creates a continuous data processing pipeline that incrementally updates state, handles late-arriving data through watermarking, 
and ensures fault tolerance through periodic checkpointing. This design enables Spark to efficiently process and update results as new data arrives in the streaming source.
'''