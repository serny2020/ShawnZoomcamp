from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment


def create_processed_events_sink_postgres(t_env):
    """
    Creates a PostgreSQL sink table in Flink for storing processed event data.

    This function defines a SQL DDL statement to create a JDBC sink table named 
    "processed_events" in PostgreSQL. The table stores event data processed by Flink.

    Parameters:
    -----------
    t_env : pyflink.table.TableEnvironment
        The Flink TableEnvironment instance where the sink table is registered.

    Returns:
    --------
    str
        The name of the created PostgreSQL sink table ("processed_events").

    Table Schema:
    ------------
    - test_data: INTEGER         → Processed numerical data from Kafka.
    - event_timestamp: TIMESTAMP → Event timestamp stored in PostgreSQL.

    PostgreSQL Connection Properties:
    ---------------------------------
    - 'connector' = 'jdbc'           → Uses the JDBC connector for PostgreSQL.
    - 'url' = 'jdbc:postgresql://postgres:5432/postgres' → Connects to PostgreSQL.
    - 'table-name' = 'processed_events' → Defines the target table name.
    - 'username' = 'postgres'         → PostgreSQL username.
    - 'password' = 'postgres'         → PostgreSQL password.
    - 'driver' = 'org.postgresql.Driver' → Specifies the PostgreSQL JDBC driver.

    Example:
    --------
    >>> table_name = create_processed_events_sink_postgres(t_env)
    >>> print(f"Sink table '{table_name}' created in Flink.")

    """
    # Define the name of the sink table in PostgreSQL (NOTE: this is the actual name in database)
    table_name = 'processed_events'

    # Define the SQL DDL for creating the PostgreSQL sink table
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,  -- Column to store processed event data
            event_timestamp TIMESTAMP  -- Column to store event timestamps
        ) WITH (
            'connector' = 'jdbc',  -- Specifies JDBC as the connector type
            'url' = 'jdbc:postgresql://postgres:5432/postgres',  -- PostgreSQL connection URL
            'table-name' = '{table_name}',  -- Name of the table in PostgreSQL
            'username' = 'postgres',  -- PostgreSQL username
            'password' = 'postgres',  -- PostgreSQL password
            'driver' = 'org.postgresql.Driver'  -- JDBC driver for PostgreSQL
        );
    """

    # Execute the SQL DDL to create the PostgreSQL sink table
    t_env.execute_sql(sink_ddl)

    # Return the name of the created table
    return table_name



def create_events_source_kafka(t_env):
    """
    Creates a Kafka source table in Flink using SQL DDL.

    This function registers a Kafka source table named "events" in the Flink 
    TableEnvironment (`t_env`). The table reads JSON-formatted messages 
    from the 'test-topic' topic in the Redpanda Kafka cluster.

    Parameters:
    ----------
    t_env : pyflink.table.TableEnvironment
        The Flink TableEnvironment instance where the source table is registered.

    Returns:
    -------
    str
        The name of the created table ("events").

    Table Schema:
    ------------
    - test_data: INTEGER                → Payload data from Kafka.
    - event_timestamp: BIGINT           → Event timestamp in epoch milliseconds.
    - event_watermark: TIMESTAMP_LTZ(3) → Generated event-time watermark for 
                                          event-time processing.
    - WATERMARK for event_watermark     → Defines a watermark strategy allowing 
                                          late events by 5 seconds. (sorting could happen here)

    Kafka Connection Properties:
    ----------------------------
    - 'connector' = 'kafka'              → Specifies Kafka as the data source.
    - 'properties.bootstrap.servers'     → Kafka broker address (Redpanda at redpanda-1:29092).
    - 'topic' = 'test-topic'             → Reads from 'test-topic'.
    - 'scan.startup.mode' = 'latest-offset' → Reads only real time messages.
    - 'properties.auto.offset.reset' = 'latest' → If offsets are not found, start from the latest.
    - 'format' = 'json'                  → Expects messages in JSON format.

    Example:
    --------
    >>> table_name = create_events_source_kafka(t_env)
    >>> print(f"Table '{table_name}' created in Flink.")

    """
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(event_timestamp, 3),
            WATERMARK for event_watermark as event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka', 
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'test-topic',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    """
    Processes log events from Kafka and writes them to a PostgreSQL sink.

    This function sets up a Flink streaming environment, connects to a Kafka source table, 
    and writes processed records into a PostgreSQL sink using SQL queries.

    Steps:
    ------
    1. Set up the Flink **StreamExecutionEnvironment**.
    2. Enable **checkpointing** for fault tolerance.
    3. Set up the **TableEnvironment** for Flink SQL execution.
    4. Create a **Kafka source table** in Flink.
    5. Create a **PostgreSQL sink table** for storing processed events.
    6. Execute an **INSERT INTO** SQL query to move data from Kafka to PostgreSQL.
    7. Handle exceptions if writing records fails.

    Exceptions:
    -----------
    If there is an error in reading from Kafka or writing to PostgreSQL, 
    it prints an error message without crashing the program.

    Returns:
    --------
    None
    """

    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Enable checkpointing every 10 seconds for fault tolerance
    # env.set_parallelism(1)  # Optional: Set parallelism to 1 for debugging or sequential execution

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_events_source_kafka(t_env)

        # Create PostgreSQL sink table
        postgres_sink = create_processed_events_sink_postgres(t_env)

        # Write records from Kafka to PostgreSQL
        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                test_data,
                TO_TIMESTAMP_LTZ(event_timestamp, 3) as event_timestamp
            FROM {source_table}
            """
        ).wait()  # Ensures the execution waits until the query finishes

    except Exception as e:
        # Handle exceptions if writing records to PostgreSQL fails
        print("Writing records from Kafka to JDBC failed:", str(e))



if __name__ == '__main__':
    log_processing()
