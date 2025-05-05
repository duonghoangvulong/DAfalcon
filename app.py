import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta, date
from clickhouse_driver import Client as ChClient

# Page configuration
st.set_page_config(
    page_title="Game Event Analytics",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)



# App title and description
st.title("🎮 Game Event Analytics Dashboard")
st.markdown(f"""
This dashboard allows you to analyze game events across Android and iOS platforms.
Select event name, multiple time periods, platform, and level requirements to view key metrics.
""")

# Helper Functions
@st.cache_data(ttl=3600)
def load_config():
    """Load ByteHouse configuration from config.json file"""
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        st.error(f"Error loading configuration: {e}")
        return None

def create_bytehouse_client(api_key, host, port=19000):
    """Create and return a ByteHouse client connection"""
    try:
        client = ChClient(
            host=host,
            port=port,
            user='bytehouse',
            password=api_key,
            secure=True,
        )
        return client
    except Exception as e:
        st.error(f"Failed to create ByteHouse client: {e}")
        return None

def test_connection(client):
    """Test if the ByteHouse connection is working"""
    try:
        result = client.execute("SELECT 1")
        return True
    except Exception as e:
        st.error(f"ByteHouse connection failed: {e}")
        return False

@st.cache_data(ttl=1800)
def execute_query(_client, query, platform):
    """Execute a query and return results as a DataFrame
    
    Note: _client is prefixed with underscore to prevent Streamlit from trying to hash it
    """
    try:
        result = _client.execute(query)
        
        if not result:
            return pd.DataFrame()
            
        # Extract column names from query (simplified version)
        column_names = [f'column_{i}' for i in range(len(result[0]))]
        
        # Create DataFrame
        df = pd.DataFrame(result, columns=column_names)
        return df
    except Exception as e:
        st.error(f"Query execution failed for {platform}: {e}")
        return None

@st.cache_data(ttl=3600)
def get_available_events(_client, platform):
    """Get list of available event names from ByteHouse
    
    Note: _client is prefixed with underscore to prevent Streamlit from trying to hash it
    """
    table_name = f"com_fc_goods_sort_matching_puzzle_triplemaster_{platform.lower()}.f_sdk_event_data"
    
    query = f"""
    SELECT DISTINCT event_name
    FROM {table_name}
    ORDER BY event_name
    """
    
    try:
        result = _client.execute(query)
        return [event[0] for event in result]
    except Exception as e:
        st.error(f"Failed to retrieve event names for {platform}: {e}")
        return []

def format_datetime(date, time):
    """Format date and time into a datetime string for ByteHouse query"""
    return f"{date} {time}"

def build_time_periods_condition(time_periods):
    """Build a SQL condition for multiple time periods"""
    conditions = []
    
    for period in time_periods:
        start_date = period['start_date'].split()[0]
        end_date = period['end_date'].split()[0]
        
        condition = f"""
        (created_day BETWEEN toDate('{start_date}') AND toDate('{end_date}')
        AND created_date >= toDateTime('{period['start_date']}')
        AND created_date <= toDateTime('{period['end_date']}'))
        """
        conditions.append(condition)
    
    return " OR ".join(conditions)

def get_event_participation_query(event_name, time_periods, platform, min_level):
    """Generate query to get event participation metrics with multiple time periods"""
    table_prefix = f"com_fc_goods_sort_matching_puzzle_triplemaster_{platform.lower()}"
    
    # Build time periods condition
    time_periods_condition = build_time_periods_condition(time_periods)
    
    query = f"""
    -- Identify all eligible users with the specified minimum level from retention data
    WITH eligible_users AS (
        SELECT DISTINCT
            r.account_id,
            r.created_day
        FROM 
            {table_prefix}.f_sdk_retention_data r
        WHERE 
            ({time_periods_condition})
            AND r.level >= {min_level} -- Use configurable minimum level
    ),

    -- Identify event participants
    event_participants AS (
        SELECT DISTINCT
            e.account_id,
            e.created_day
        FROM 
            {table_prefix}.f_sdk_event_data e
        WHERE 
            ({time_periods_condition})
            AND e.event_name = '{event_name}'
            AND e.level >= {min_level} -- Use configurable minimum level
    ),

    -- Classify users as participants or non-participants
    user_participation AS (
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
    ),

    -- Get total active users per day BY GROUP as denominator for participation rate
    daily_active_users_by_group AS (
        SELECT
            created_day,
            participation_group,
            COUNT(DISTINCT account_id) AS group_total_users
        FROM 
            user_participation
        GROUP BY
            created_day,
            participation_group
    ),

    -- Calculate IAP metrics combining with in_app data
    iap_metrics AS (
        SELECT
            i.created_day,
            up.participation_group,
            
            -- Basic metrics
            COUNT(DISTINCT i.account_id) AS paying_users,
            COUNT(DISTINCT i.uuid) AS purchase_count,
            SUM(i.price_usd) AS total_revenue,
            
            -- Percentiles for revenue
            quantile(0.25)(i.price_usd) AS revenue_25th_percentile,
            quantile(0.50)(i.price_usd) AS revenue_median,
            quantile(0.75)(i.price_usd) AS revenue_75th_percentile,
            quantile(0.90)(i.price_usd) AS revenue_90th_percentile,
            
            -- First purchase conversion metrics
            SUM(IF(i.in_app_count = 1, 1, 0)) AS first_time_payers
        FROM 
            {table_prefix}.f_sdk_in_app_data i
        JOIN
            user_participation up 
            ON i.account_id = up.account_id 
            AND i.created_day = up.created_day
        WHERE
            ({time_periods_condition})
            AND i.level >= {min_level} -- Use configurable minimum level
        GROUP BY
            i.created_day,
            up.participation_group
    )

    -- Final result with ARPU and ARPPU calculations
    SELECT
        im.created_day,
        im.participation_group,
        da.group_total_users,
        im.paying_users,
        im.purchase_count,
        im.total_revenue,
        im.revenue_25th_percentile,
        im.revenue_median,
        im.revenue_75th_percentile,
        im.revenue_90th_percentile,
        im.first_time_payers,
        -- Conversion rate: first time payers divided by THAT GROUP'S total users
        im.first_time_payers / nullIf(da.group_total_users, 0) AS conversion_rate,
        -- Pay rate: paying users divided by THAT GROUP'S total users
        im.paying_users / nullIf(da.group_total_users, 0) AS pay_rate,
        -- ARPU: Total Revenue / Total Users in the group
        im.total_revenue / nullIf(da.group_total_users, 0) AS ARPU,
        -- ARPPU: Total Revenue / Paying Users
        im.total_revenue / nullIf(im.paying_users, 0) AS ARPPU
    FROM
        iap_metrics im
    JOIN
        daily_active_users_by_group da
        ON im.created_day = da.created_day
        AND im.participation_group = da.participation_group
    ORDER BY
        im.created_day,
        im.participation_group
    """
    
    return query

def get_event_engagement_query(event_name, time_periods, platform, min_level):
    """Generate query to get event engagement metrics with multiple time periods"""
    table_prefix = f"com_fc_goods_sort_matching_puzzle_triplemaster_{platform.lower()}"
    
    # Build time periods condition
    time_periods_condition = build_time_periods_condition(time_periods)
    
    query = f"""
    WITH event_engagement AS (
        SELECT
            created_day,
            COUNT(DISTINCT account_id) AS unique_users,
            COUNT(*) AS total_interactions,
            COUNT(*) / COUNT(DISTINCT account_id) AS avg_interactions_per_user,
            COUNT(DISTINCT session_id) AS unique_sessions,
            COUNT(*) / COUNT(DISTINCT session_id) AS avg_interactions_per_session
        FROM
            {table_prefix}.f_sdk_event_data
        WHERE
            ({time_periods_condition})
            AND event_name = '{event_name}'
            AND level >= {min_level} -- Use configurable minimum level
        GROUP BY
            created_day
        ORDER BY
            created_day
    )
    
    SELECT * FROM event_engagement
    """
    
    return query

def calculate_overall_participation_rate(df):
    """Calculate the overall participation rate from event data"""
    if df is None or df.empty:
        return 0
    
    try:
        # Get total participants and non-participants
        participants = df[df['participation_group'] == 'Event Participant']['group_total_users'].sum()
        total_users = df['group_total_users'].sum()
        
        # Calculate participation rate
        if total_users > 0:
            return participants / total_users
        return 0
    except:
        return 0

def calculate_overall_revenue(df):
    """Calculate the overall revenue from event data"""
    if df is None or df.empty:
        return 0
    
    try:
        # Calculate total revenue for event participants
        participant_revenue = df[df['participation_group'] == 'Event Participant']['total_revenue'].sum()
        return participant_revenue
    except:
        return 0

def format_metrics(value, format_type='number'):
    """Format metrics for display"""
    if format_type == 'currency':
        return f"${value:,.2f}"
    elif format_type == 'percentage':
        return f"{value:.2%}"
    else:
        return f"{value:,.0f}"

def format_time_periods(time_periods):
    """Format time periods for display"""
    return ", ".join([f"{period['start_date']} to {period['end_date']}" for period in time_periods])

# Sidebar - Controls
st.sidebar.header("📊 Controls")

# 1. Load configuration
config = load_config()
if not config:
    st.error("Failed to load configuration. Please check your config.json file.")
    st.stop()

# 2. Create ByteHouse clients
clients = {}
for platform in ['Android', 'iOS']:
    try:
        api_key = config['BYTEHOUSE_API_KEY']
        host = config['BYTEHOUSE_HOST']
        port = config['BYTEHOUSE_PORT']
        
        client = create_bytehouse_client(api_key, host, port)
        
        if client and test_connection(client):
            clients[platform] = client
            st.sidebar.success(f"✅ ByteHouse connection successful for {platform}!")
        else:
            st.sidebar.error(f"❌ ByteHouse connection failed for {platform}!")
    except Exception as e:
        st.sidebar.error(f"Error setting up client for {platform}: {e}")

# Stop if no clients are available
if not clients:
    st.error("No ByteHouse connections available. Please check your configuration.")
    st.stop()

# 3. Platform selection
platform = st.sidebar.selectbox(
    "Select Platform",
    options=list(clients.keys()),
    index=0
)

# 4. Get available events
available_events = get_available_events(clients[platform], platform)

if not available_events:
    st.error(f"No events found for {platform}. Please check your data.")
    st.stop()

# 5. Event selection
selected_event = st.sidebar.selectbox(
    "Select Event",
    options=available_events,
    index=0 if available_events else None
)

# 6. Multiple time periods selection
st.sidebar.subheader("Time Periods")
st.sidebar.markdown("Add one or more time periods for analysis:")

# Initialize time periods list in session state if it doesn't exist
if 'time_periods' not in st.session_state:
    st.session_state.time_periods = []

# Parse the CURRENT_DATE string to create a datetime object
try:
    today_datetime = datetime.strptime(CURRENT_DATE, "%Y-%m-%d %H:%M:%S")
    today = today_datetime.date()
except:
    today_datetime = datetime.now()
    today = today_datetime.date()

# Calculate default dates that allow wider range
default_end_date = today
default_start_date = today - timedelta(days=7)

# Maximum time range - use a very large range for flexibility (e.g., 10 years)
min_date = date(2015, 1, 1)  # Allow dates as far back as 2015
max_date = date(2030, 12, 31)  # Allow dates as far into the future as 2030

# Form for adding a new time period
with st.sidebar.form("add_time_period"):
    st.subheader("Add Time Period")
    
    new_start_date = st.date_input(
        "Start Date",
        value=default_start_date,
        min_value=min_date,
        max_value=max_date
    )
    
    new_start_time = st.time_input(
        "Start Time",
        value=datetime.strptime("00:00:00", "%H:%M:%S").time()
    )
    
    new_end_date = st.date_input(
        "End Date",
        value=default_end_date,
        min_value=min_date,
        max_value=max_date
    )
    
    new_end_time = st.time_input(
        "End Time",
        value=datetime.strptime("23:59:59", "%H:%M:%S").time()
    )
    
    # Check if the end date is before the start date
    date_error = new_end_date < new_start_date
    if date_error:
        st.error("End date must be after start date.")
    
    submit_button = st.form_submit_button("Add Time Period")
    
    if submit_button and not date_error:
        # Format the datetime strings
        start_datetime = format_datetime(new_start_date, new_start_time)
        end_datetime = format_datetime(new_end_date, new_end_time)
        
        # Add the new time period to the list
        st.session_state.time_periods.append({
            'start_date': start_datetime,
            'end_date': end_datetime
        })
        
        st.success(f"Added time period: {start_datetime} to {end_datetime}")
    elif submit_button and date_error:
        st.error("Cannot add time period: End date must be after start date.")

# Display and manage existing time periods
if st.session_state.time_periods:
    st.sidebar.subheader("Selected Time Periods")
    
    for i, period in enumerate(st.session_state.time_periods):
        col1, col2 = st.sidebar.columns([3, 1])
        with col1:
            st.write(f"{i+1}. {period['start_date']} to {period['end_date']}")
        with col2:
            if st.button(f"Remove", key=f"remove_{i}"):
                st.session_state.time_periods.pop(i)
                st.rerun()
else:
    st.sidebar.warning("No time periods added. Add at least one time period.")

# Clear all time periods button
if st.session_state.time_periods and st.sidebar.button("Clear All Time Periods"):
    st.session_state.time_periods = []
    st.rerun()

# 7. Minimum level requirement selection
min_level = st.sidebar.number_input(
    "Minimum Level Requirement",
    min_value=1,
    max_value=1000,
    value=1,
    step=1,
    help="Set the minimum level required to participate in this event"
)

# Execute button
if st.sidebar.button("Analyze Event", key="analyze_button") and st.session_state.time_periods:
    # Show loading indicator
    with st.spinner(f"Analyzing {selected_event} on {platform} across {len(st.session_state.time_periods)} time periods..."):
        # Execute queries
        participation_query = get_event_participation_query(selected_event, st.session_state.time_periods, platform, min_level)
        participation_df = execute_query(clients[platform], participation_query, platform)
        
        engagement_query = get_event_engagement_query(selected_event, st.session_state.time_periods, platform, min_level)
        engagement_df = execute_query(clients[platform], engagement_query, platform)
        
        # Fix column names for the dataframes based on the queries
        if participation_df is not None and not participation_df.empty:
            participation_df.columns = [
                'created_day', 'participation_group', 'group_total_users', 'paying_users', 
                'purchase_count', 'total_revenue', 'revenue_25th_percentile', 
                'revenue_median', 'revenue_75th_percentile', 'revenue_90th_percentile', 
                'first_time_payers', 'conversion_rate', 'pay_rate', 'ARPU', 'ARPPU'
            ]
        
        if engagement_df is not None and not engagement_df.empty:
            engagement_df.columns = [
                'created_day', 'unique_users', 'total_interactions', 
                'avg_interactions_per_user', 'unique_sessions', 'avg_interactions_per_session'
            ]
        
        # Calculate overall metrics
        overall_participation_rate = calculate_overall_participation_rate(participation_df)
        overall_revenue = calculate_overall_revenue(participation_df)
        
        # If we have engagement data, calculate total engagement
        total_engagement = 0
        if engagement_df is not None and not engagement_df.empty:
            total_engagement = engagement_df['total_interactions'].sum()
        
        # Event details
        st.subheader(f"Event Analysis: {selected_event}")
        st.markdown(f"""
        **Platform:** {platform}  
        **Time Periods:** {len(st.session_state.time_periods)} periods selected  
        **Minimum Level Required:** {min_level}  
        """)
        
        # Display time periods in a table
        st.subheader("Selected Time Periods")
        time_periods_df = pd.DataFrame([
            {"Period": i+1, "Start Date & Time": period['start_date'], "End Date & Time": period['end_date']}
            for i, period in enumerate(st.session_state.time_periods)
        ])
        st.table(time_periods_df)
        
        # Display metrics in tiles
        st.subheader("Overall Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label="Event Participation Rate",
                value=format_metrics(overall_participation_rate, 'percentage')
            )
            
        with col2:
            st.metric(
                label="Total Event Revenue",
                value=format_metrics(overall_revenue, 'currency')
            )
            
        with col3:
            st.metric(
                label="Total Event Engagements",
                value=format_metrics(total_engagement)
            )
        
        # Display charts
        st.subheader("Participation Metrics")
        
        if participation_df is not None and not participation_df.empty:
            # Reshape data for grouped bar chart
            participant_data = participation_df[participation_df['participation_group'] == 'Event Participant']
            
            # Create participation chart
            fig_participation = px.bar(
                participant_data,
                x='created_day',
                y='group_total_users',
                title="Daily Participation Count",
                labels={
                    'created_day': 'Date',
                    'group_total_users': 'Number of Participants'
                },
                color_discrete_sequence=['rgb(55, 83, 109)']
            )
            
            # Create revenue chart
            fig_revenue = px.bar(
                participant_data,
                x='created_day',
                y='total_revenue',
                title="Daily Revenue",
                labels={
                    'created_day': 'Date',
                    'total_revenue': 'Revenue (USD)'
                },
                color_discrete_sequence=['rgb(26, 118, 255)']
            )
            
            # Display charts
            st.plotly_chart(fig_participation, use_container_width=True)
            st.plotly_chart(fig_revenue, use_container_width=True)
            
            # Pay rate and ARPU comparison
            st.subheader("Monetization Metrics")
            
            # Pay rate comparison
            fig_pay_rate = px.bar(
                participation_df,
                x='created_day',
                y='pay_rate',
                color='participation_group',
                barmode='group',
                title="Pay Rate Comparison",
                labels={
                    'created_day': 'Date',
                    'pay_rate': 'Pay Rate',
                    'participation_group': 'Group'
                },
                color_discrete_map={
                    'Event Participant': 'blue',
                    'Non-Participant': 'gray'
                },
                opacity=0.8
            )
            
            # ARPU comparison
            fig_arpu = px.bar(
                participation_df,
                x='created_day',
                y='ARPU',
                color='participation_group',
                barmode='group',
                title="ARPU Comparison",
                labels={
                    'created_day': 'Date',
                    'ARPU': 'ARPU (USD)',
                    'participation_group': 'Group'
                },
                color_discrete_map={
                    'Event Participant': 'green',
                    'Non-Participant': 'lightgray'
                },
                opacity=0.8
            )
            
            # Display charts side by side
            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(fig_pay_rate, use_container_width=True)
            with col2:
                st.plotly_chart(fig_arpu, use_container_width=True)
        
        # Engagement metrics
        if engagement_df is not None and not engagement_df.empty:
            st.subheader("Engagement Metrics")
            
            # Create users and interactions chart
            fig_users = px.bar(
                engagement_df,
                x='created_day',
                y=['unique_users', 'total_interactions'],
                barmode='group',
                title="Daily Users & Interactions",
                labels={
                    'created_day': 'Date',
                    'value': 'Count',
                    'variable': 'Metric'
                },
                color_discrete_map={
                    'unique_users': 'rgb(158, 202, 225)',
                    'total_interactions': 'rgb(94, 158, 217)'
                }
            )
            
            # Create average interactions chart
            fig_avg = px.line(
                engagement_df,
                x='created_day',
                y=['avg_interactions_per_user', 'avg_interactions_per_session'],
                title="Average Interactions",
                labels={
                    'created_day': 'Date',
                    'value': 'Average',
                    'variable': 'Metric'
                },
                color_discrete_map={
                    'avg_interactions_per_user': 'rgb(231, 107, 243)',
                    'avg_interactions_per_session': 'rgb(255, 151, 255)'
                }
            )
            
            # Add markers to line chart
            fig_avg.update_traces(mode='lines+markers')
            
            # Display charts side by side
            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(fig_users, use_container_width=True)
            with col2:
                st.plotly_chart(fig_avg, use_container_width=True)
        
        # Display raw data in expandable sections
        with st.expander("Raw Participation Data"):
            if participation_df is not None and not participation_df.empty:
                st.dataframe(participation_df)
            else:
                st.warning("No participation data available.")
        
        with st.expander("Raw Engagement Data"):
            if engagement_df is not None and not engagement_df.empty:
                st.dataframe(engagement_df)
            else:
                st.warning("No engagement data available.")
        
        # Show the queries used (for debugging purposes)
        with st.expander("SQL Queries Used"):
            st.code(participation_query, language="sql")
            st.code(engagement_query, language="sql")

# Instructions
elif not st.session_state.time_periods:
    st.info("👈 Please add at least one time period before analyzing the event.")
else:
    st.info("👈 Select a platform, event, time periods, and minimum level requirement, then click 'Analyze Event' to view metrics.")

# Footer
st.markdown("---")
