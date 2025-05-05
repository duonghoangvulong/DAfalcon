import streamlit as st
import pandas as pd
from clickhouse_driver import Client as ChClient
import json

# Load configuration from Streamlit secrets
def load_config():
    """Load configuration directly from Streamlit secrets."""
    try:
        config = {
            "BYTEHOUSE_API_KEY": st.secrets["BYTEHOUSE"]["API_KEY"],
            "BYTEHOUSE_HOST": st.secrets["BYTEHOUSE"]["HOST"],
            "BYTEHOUSE_PORT": st.secrets["BYTEHOUSE"]["PORT"],
        }
        return config
    except KeyError as e:
        raise RuntimeError(f"Missing secrets configuration: {e}")

# Create a connection to ByteHouse
def create_bytehouse_client(api_key, host, port=19000):
    """Create and return a ByteHouse client connection."""
    try:
        client = ChClient(
            host=host,
            port=port,
            user="bytehouse",
            password=api_key,
            secure=True,
        )
        return client
    except Exception as e:
        raise RuntimeError(f"Failed to create ByteHouse client: {e}")

# Test the connection
def test_connection(client):
    """Test if the ByteHouse connection is working."""
    try:
        client.execute("SELECT 1")
        return True
    except Exception as e:
        raise RuntimeError(f"ByteHouse connection failed: {e}")

# Execute a query and return results as a DataFrame
@st.cache_data(ttl=1800)
def execute_query(client, query):
    """Execute a query and return results as a DataFrame."""
    try:
        result = client.execute(query)
        if not result:
            return pd.DataFrame()
        
        # Extract column names from result metadata
        column_names = [col[0] for col in client.execute("SELECT name FROM system.columns WHERE table = 'TABLE_NAME' LIMIT 0")]
        
        # Create DataFrame
        df = pd.DataFrame(result, columns=column_names)
        return df
    except Exception as e:
        raise RuntimeError(f"Query execution failed: {e}")

# Query helper to get event names
@st.cache_data(ttl=3600)
def get_available_events(client, platform):
    """Get list of available event names from ByteHouse."""
    table_name = f"com_fc_goods_sort_matching_puzzle_triplemaster_{platform.lower()}.f_sdk_event_data"
    query = f"""
    SELECT DISTINCT event_name
    FROM {table_name}
    ORDER BY event_name
    """
    try:
        result = client.execute(query)
        return [event[0] for event in result]
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve event names for {platform}: {e}")

# Build a SQL condition for multiple time periods
def build_time_periods_condition(time_periods):
    """Build a SQL condition for multiple time periods."""
    conditions = []
    for period in time_periods:
        condition = f"""
        (created_day BETWEEN toDate('{period['start_date']}') AND toDate('{period['end_date']}')
        AND created_date >= toDateTime('{period['start_date']}')
        AND created_date <= toDateTime('{period['end_date']}'))
        """
        conditions.append(condition)
    return " OR ".join(conditions)

# Generate a query for event participation
def get_event_participation_query(event_name, time_periods, platform, min_level):
    """Generate query to get event participation metrics with multiple time periods."""
    table_prefix = f"com_fc_goods_sort_matching_puzzle_triplemaster_{platform.lower()}"
    time_periods_condition = build_time_periods_condition(time_periods)
    query = f"""
    WITH eligible_users AS (
        SELECT DISTINCT
            r.account_id,
            r.created_day
        FROM 
            {table_prefix}.f_sdk_retention_data r
        WHERE 
            ({time_periods_condition})
            AND r.level >= {min_level}
    ),
    event_participants AS (
        SELECT DISTINCT
            e.account_id,
            e.created_day
        FROM 
            {table_prefix}.f_sdk_event_data e
        WHERE 
            ({time_periods_condition})
            AND e.event_name = '{event_name}'
            AND e.level >= {min_level}
    )
    SELECT
        eu.account_id,
        eu.created_day,
        CASE 
            WHEN ep.account_id IS NOT NULL THEN 'Event Participant'
            ELSE 'Non-Participant'
        END AS participation_group
    FROM 
        eligible_users eu
    LEFT JOIN 
        event_participants ep
        ON eu.account_id = ep.account_id
        AND eu.created_day = ep.created_day
    """
    return query

# Main function to initialize the connection and provide query functionality
def initialize_connection():
    """Initialize the ByteHouse connection and return a connection object."""
    config = load_config()
    client = create_bytehouse_client(
        api_key=config["BYTEHOUSE_API_KEY"], 
        host=config["BYTEHOUSE_HOST"], 
        port=config["BYTEHOUSE_PORT"]
    )
    if test_connection(client):
        return client
    else:
        raise RuntimeError("Failed to establish a connection to ByteHouse.")

# Example usage
if __name__ == "__main__":
    try:
        client = initialize_connection()
        st.write("Connection successful!")

        # Example: Get available events for Android
        platform = "Android"
        events = get_available_events(client, platform)
        st.write(f"Available events for {platform}: {events}")

        # Example: Execute a query
        query = "SELECT 1"
        df = execute_query(client, query)
        st.write("Query result:", df)

    except Exception as e:
        st.error(f"Error: {e}")