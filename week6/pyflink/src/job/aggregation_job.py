from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_events_aggregated_sink(t_env):
    """
    Create a JDBC sink table for aggregated events.

    This function defines and creates a table named 'processed_events_aggregated' in a PostgreSQL
    database using a DDL statement executed through the provided table environment. The table
    includes the following columns:
      - event_hour: TIMESTAMP(3) indicating the start time of the event window.
      - test_data: INTEGER representing some test-specific data.
      - num_hits: BIGINT representing the count of events.
    A composite primary key is set on (event_hour, test_data) (not enforced by the engine).

    The WITH clause specifies connection details for the JDBC sink, including the JDBC URL,
    table name, username, password, and driver information.

    Parameters:
        t_env: The table environment (e.g., StreamTableEnvironment) used to execute SQL statements.

    Returns:
        table_name (str): The name of the sink table that was created.
    """
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            test_data INT,
            num_hits BIGINT,
            PRIMARY KEY (event_hour, test_data) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    # Execute the DDL to create the sink table.
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    """
    Create a Kafka source table for event data.

    This function defines and creates a table named 'events' that sources data from a Kafka topic
    named 'test-topic'. The table schema includes:
      - test_data: INTEGER representing test-specific data.
      - event_timestamp: BIGINT storing the raw timestamp (in milliseconds).
      - event_watermark: A computed column converting event_timestamp to a TIMESTAMP with local time zone
        using TO_TIMESTAMP_LTZ, with a precision of 3. It is used to generate watermarks.
    
    A watermark is also defined for the event_watermark column to allow handling of out-of-order
    events. The watermark is computed as the event_watermark value minus an interval of 1 second.

    The WITH clause includes Kafka connection properties such as:
      - 'connector': Specifies the use of the Kafka connector.
      - 'properties.bootstrap.servers': Kafka broker addresses.
      - 'topic': The Kafka topic to read from.
      - 'scan.startup.mode': Configured to start reading from the earliest offset.
      - 'properties.auto.offset.reset': Also set to 'earliest' for offset management.
      - 'format': Specifies the data format as JSON.

    Parameters:
        t_env: The table environment (e.g., StreamTableEnvironment) used to execute SQL statements.

    Returns:
        table_name (str): The name of the source table that was created.
    """
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(event_timestamp, 3),
            WATERMARK for event_watermark as event_watermark - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'test-topic',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    # Execute the DDL to create the source table.
    t_env.execute_sql(source_ddl)
    return table_name



def log_aggregation():
    """
    Aggregates log events from a Kafka source, performs a windowed count aggregation,
    and writes the results to a JDBC sink.

    Steps:
    1. Set up the streaming execution environment with checkpointing and parallelism.
    2. Create a table environment in streaming mode.
    3. Define a watermark strategy to manage out-of-order events.
    4. Create the Kafka source table and the JDBC sink table.
    5. Execute a SQL query that:
       - Groups the incoming events into 1-minute tumbling windows.
       - Uses the event timestamp from the third element of the event tuple.
       - Counts the number of events per window and per test_data field.
       - Inserts the aggregated results into the sink table.
    """
    # Set up the streaming execution environment.
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Enable checkpointing every 10 seconds for fault tolerance.
    env.set_parallelism(3)  # Set the level of parallelism to 3.

    # Set up the table environment in streaming mode.
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Define a watermark strategy to handle out-of-order events.
    # The strategy allows events to arrive up to 5 seconds late.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            # Lambda function to assign timestamps:
            #   event: the data record (e.g., a tuple),
            #   timestamp: the previously assigned (or default) timestamp.
            # Here, we extract the event time from the third element of the tuple.
            lambda event, timestamp: event[2]
        )
    )
    try:
        # Create a Kafka source table for incoming events.
        source_table = create_events_source_kafka(t_env)
        # Create a sink table (e.g., JDBC sink) for the aggregated results.
        aggregated_table = create_events_aggregated_sink(t_env)

        # Execute a SQL query to aggregate events into 1-minute tumbling windows.
        # The query selects:
        #   - The window start time (labeled as event_hour).
        #   - The test_data field.
        #   - The count of events (num_hits) in each window.
        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start as event_hour,
            test_data,
            COUNT(*) AS num_hits
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_watermark), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, test_data;
        """).wait()

    except Exception as e:
        # Print an error message if the aggregation or insertion fails.
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
