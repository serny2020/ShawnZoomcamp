from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_trips_aggregated_sink(t_env):
    """
    Create a JDBC sink table for aggregated trips using a session window.

    The table is named 'processed_trips_aggregated' and contains:
      - session_start: TIMESTAMP(3) representing the start time of the session window.
      - PULocationID: INT representing the pickup location.
      - num_trips: BIGINT representing the count of trips in the session window.
    A composite primary key is set on (session_start, PULocationID) (not enforced by the engine).
    """
    table_name = 'processed_trips_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (session_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_green_trips_source_table(t_env):
    """
    Create a Kafka source table for green taxi trips data.

    The table is named 'green_trips' with the following schema:
      - lpep_pickup_datetime: TIMESTAMP(3)
      - lpep_dropoff_datetime: TIMESTAMP(3) with a watermark defined as 5 seconds behind the event time.
      - PULocationID: INT
      - DOLocationID: INT
      - passenger_count: INT
      - trip_distance: DOUBLE
      - tip_amount: DOUBLE

    The JSON format is configured to ignore parse errors.
    """
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            WATERMARK FOR lpep_dropoff_datetime AS lpep_dropoff_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_aggregation():
    """
    Aggregates green taxi trips using a session window with a 5-minute gap.

    This job:
      1. Sets up the streaming execution environment with checkpointing.
      2. Creates a table environment.
      3. Creates the Kafka source table for green taxi trips and the JDBC sink table for aggregated results.
      4. Uses a session window on lpep_dropoff_datetime with a gap of 5 minutes to aggregate trips by PULocationID.
      5. Inserts the aggregated results into the JDBC sink.
    """
    # Set up the streaming execution environment.
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Checkpoint every 10 seconds.
    env.set_parallelism(3)

    # Set up the table environment in streaming mode.
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create the source and sink tables.
        source_table = create_green_trips_source_table(t_env)   
        aggregated_table = create_trips_aggregated_sink(t_env)

        # Execute a SQL query that aggregates trips using a session window with a 5-minute gap.
        t_env.execute_sql(f"""
            INSERT INTO {aggregated_table}
            SELECT
                SESSION_START(lpep_dropoff_datetime, INTERVAL '5' MINUTE) AS session_start,
                PULocationID,
                COUNT(*) AS num_trips
            FROM {source_table}
            GROUP BY SESSION(lpep_dropoff_datetime, INTERVAL '5' MINUTE), PULocationID
            """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_aggregation()
