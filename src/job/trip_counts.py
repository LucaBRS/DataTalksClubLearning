from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

t_env = StreamTableEnvironment.create(env)

t_env.execute_sql("""
    CREATE TABLE green_trips (
        PULocationID INT,
        DOLocationID INT,
        trip_distance FLOAT,
        lpep_pickup_datetime VARCHAR,
        lpep_dropoff_datetime VARCHAR,
        passenger_count FLOAT,
        tip_amount FLOAT,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:9092',
        'properties.group.id' = 'flink-consumer',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

t_env.execute_sql("""
    CREATE TABLE trip_counts (
        window_start TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'trip_counts',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
""")

t_env.execute_sql("""
    INSERT INTO trip_counts
    SELECT
        TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE),
        PULocationID,
        COUNT(*) AS num_trips
    FROM green_trips
    GROUP BY
        TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
        PULocationID
""")
