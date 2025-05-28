import os
import time
import pandas as pd
import streamlit as st
from pinotdb import connect
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

auto_refresh = True # We seta uto refresh of a page to True
pinot_host = os.environ.get("PINOT_SERVER", "pinot-broker")
pinot_port = os.environ.get("PINOT_PORT", 8099)
conn = connect(pinot_host, pinot_port)
st.set_page_config(layout="wide")
st.title("Pizza Shop orders analytics 🍕")


def plot_time_series(df_ts, time_col='dateMin', value_cols=['orders', 'revenue']):
    """
    Plots time series data for orders and revenue with padding and resampling.
    :param df_ts: DataFrame with time series (must contain time_col + value_cols)
    :param time_col: Name of timestamp column
    :param value_cols: List of columns to plot (typically ['orders', 'revenue'])
    """

    # Convert to datetime
    df_ts[time_col] = pd.to_datetime(df_ts[time_col])

    # Set index
    df_ts = df_ts.set_index(time_col).sort_index()

    # Infer best granularity
    total_seconds = (df_ts.index.max() - df_ts.index.min()).total_seconds()

    if total_seconds < 60 * 10:
        freq = 'S'  # second
    elif total_seconds < 60 * 60:
        freq = 'T'  # minute
    else:
        freq = 'H'  # hour

    # Resample to regular time intervals and fill gaps with 0
    df_resampled = df_ts.resample(freq).sum().fillna(0)

    # Create Streamlit layout
    col1, col2 = st.columns(2)

    with col1:
        # Plot orders
        fig_orders = px.line(df_resampled, x=df_resampled.index, y='orders', title=f"Orders over time ({freq})")
        fig_orders.update_traces(mode="lines+markers")
        fig_orders.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_orders, use_container_width=True)


    with col2:
        # Plot revenue
        fig_revenue = px.line(df_resampled, x=df_resampled.index, y='revenue', title=f"Revenue over time ({freq})")
        fig_revenue.update_traces(mode="lines+markers", line=dict(color='green'))
        fig_revenue.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_revenue, use_container_width=True)


mapping2 = {
    "1 hour": {"period": "PT60M", "previousPeriod": "PT120M", "granularity": "minute"},
    "30 minutes": {"period": "PT30M", "previousPeriod": "PT60M", "granularity": "minute"},
    "10 minutes": {"period": "PT10M", "previousPeriod": "PT20M", "granularity": "second"},
    "5 minutes": {"period": "PT5M", "previousPeriod": "PT10M", "granularity": "second"},
    "1 minute": {"period": "PT1M", "previousPeriod": "PT2M", "granularity": "second"}
}

time_options = list(mapping2.keys()) + ["Custom"] # time labels at the beginning of the dashboard

# Initialize default sleep time if not already set in the session state
if "sleep_time" not in st.session_state:
    st.session_state.sleep_time = 5 # default refresh interval

# Initialize auto_refresh flag if not already present
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True # default to auto-refresh enabled

# Those buttons at the beginning of a page for choosing time window
with st.expander("Dashboard settings", expanded=True):

    # Radio buttons for selecting the time window of data display 
    time_ago = st.radio("Display data from the last", options=time_options, horizontal=True, index=len(time_options) - 2, key="time_ago")

    # If user selects "Custom", provide manual date and time range inputs
    if time_ago == "Custom":

        # Turn off auto-refresh in custom mode
        st.session_state.auto_refresh = False
        auto_refresh = False

        # Four columns for date and time inputs
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
        with col3:
            start_time = st.time_input("Start Time", value=datetime.strptime("08:00", "%H:%M").time())
        with col4:
            end_time = st.time_input("End Time", value=datetime.strptime("20:00", "%H:%M").time())

        # Combine date and time inputs into full datetime objects
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)

        st.info(f"**Custom range selected:** {start_datetime} to {end_datetime}")

    else:
        # Display a toggle switch for enabling/disabling auto-refresh
        auto_refresh = st.toggle('Auto-refresh', value=st.session_state.auto_refresh)
        st.session_state.auto_refresh = auto_refresh # Sync session state with user input

        # If auto-refresh is enabled, allow user to set refresh interval
        if auto_refresh:
            number = st.number_input('Refresh rate in seconds', value=st.session_state.sleep_time)
            st.session_state.sleep_time = number # Update refresh interval in session state

now = datetime.now() # Get the current date and time
dt_string = now.strftime("%B %-d, %Y at %-I:%M:%S %p") # Format the current time into a human-readable string 
if st.session_state.auto_refresh: 
    st.info(f"**Last update:** {dt_string}", icon="ℹ️") #  display the timestamp of the last update

# Create a cursor for executing SQL queries against the database connection
curs = conn.cursor() 
pinot_available = False

try: #Testing Pinot connectivity
    curs.execute("select * FROM orders where ts > ago('PT2M')")
    pinot_available = curs.description is not None
    if not pinot_available:
        st.warning("Connected to Pinot, but no orders imported", icon="⚠️")
except Exception as e:
    st.warning(f"Unable to connect to or query Apache Pinot [{pinot_host}:{pinot_port}] Exception: {e}", icon="⚠️")

# CONFIGURATION UP TO THIS POINT. Now comes the logic of the app.

# We (only) proceed when Pinot is available.
if pinot_available:
    if time_ago != "Custom": # time range is not a custom selection

        # Query to fetch count and sum of orders for the last period and previous period
        # First row of query: Orders in the most recent period
        # Second row Orders in the previous period
        # Third row Revenue in the most recent period
        # Forth row Revenue in the previous period
        query = """
            select count(*) FILTER(WHERE ts > ago(%(nearTimeAgo)s)) AS events1Min, 
                   count(*) FILTER(WHERE ts <= ago(%(nearTimeAgo)s) AND ts > ago(%(timeAgo)s)) AS events1Min2Min,
                   sum(price) FILTER(WHERE ts > ago(%(nearTimeAgo)s)) AS total1Min,
                   sum(price) FILTER(WHERE ts <= ago(%(nearTimeAgo)s) AND ts > ago(%(timeAgo)s)) AS total1Min2Min
            from orders
            where ts > ago(%(timeAgo)s)
            limit 1
        """

        # execute query
        curs.execute(query, {
            "timeAgo": mapping2[time_ago]["previousPeriod"],
            "nearTimeAgo": mapping2[time_ago]["period"]
        })

        # Load the query result into a pandas DataFrame
        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        # Create a container with a border to display the metrics
        metrics_container = st.container(border=True)
        metrics_container.subheader(f"Orders in the last {time_ago}")
        num_orders, rev, order_val = metrics_container.columns(3)

        # Display the number of recent orders and the delta compared to the previous period
        num_orders.metric(
            label="Number of orders",
            value="{:,}".format(int(df['events1Min'].values[0])),
            delta="{:,}".format(int(df['events1Min'].values[0] - df['events1Min2Min'].values[0]))
            if df['events1Min2Min'].values[0] > 0 else None
        )

        # Display total revenue and the delta compared to the previous period
        rev.metric(
            label="Revenue in €",
            value="{:,.2f}".format(df['total1Min'].values[0]),
            delta="{:,.2f}".format(df['total1Min'].values[0] - df['total1Min2Min'].values[0])
            if df['total1Min2Min'].values[0] > 0 else None
        )

        # Calculate the average order value for both periods
        average_order_value_1min = df['total1Min'].values[0] / int(df['events1Min'].values[0]) if df['events1Min'].values[0] > 0 else 0
        average_order_value_1min_2min = df['total1Min2Min'].values[0] / int(df['events1Min2Min'].values[0]) if int(df['events1Min2Min'].values[0]) > 0 else 0

        order_val.metric(
            label="Average order value in €",
            value="{:,.2f}".format(average_order_value_1min),
            delta="{:,.2f}".format(average_order_value_1min - average_order_value_1min_2min)
            if average_order_value_1min_2min > 0 else None
        )

        # Query to retrieve time-series data grouped by time intervals (minute, second, etc.)
        query_ts = """
            select ToDateTime(DATETRUNC(%(granularity)s, ts), 'yyyy-MM-dd HH:mm:ss') AS dateMin, 
                   count(*) AS orders, 
                   sum(price) AS revenue
            from orders
            where ts > ago(%(timeAgo)s)
            group by dateMin
            order by dateMin desc
            LIMIT 10000
        """

        curs.execute(query_ts, {
            "timeAgo": mapping2[time_ago]["previousPeriod"], 
            "granularity": mapping2[time_ago]["granularity"]
        })

        # Load the time-series result into a DataFrame
        df_ts = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    else:
        # Custom time range query
        start_str = start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        query = f"""
            select count(*) AS eventsCustom,
            sum(price) AS totalCustom from orders
            where ts BETWEEN '{start_str}' AND '{end_str}'
            limit 1
        """

        curs.execute(query)
        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        metrics_container = st.container(border=True)

        num_orders, rev = metrics_container.columns(2)

        num_orders.metric(
            label="Number of orders",
            value="{:,}".format(int(df['eventsCustom'].values[0]))
        )

        rev.metric(
            label="Revenue in €",
            value="{:,.2f}".format(df['totalCustom'].values[0])
        )

        query_ts = f"""
            select ToDateTime(ts, 'yyyy-MM-dd HH:mm:ss') AS dateMin, 
            count(*) AS orders, 
            sum(price) AS revenue
            from orders
            where ts BETWEEN '{start_str}' AND '{end_str}'
            group by dateMin
            order by dateMin desc
            LIMIT 10000
        """

        curs.execute(query_ts)

        df_ts = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    # Plotting section
    if df_ts.shape[0] > 1:
        plot_time_series(df_ts, time_col='dateMin', value_cols=['orders', 'revenue'])

    
    # histograms
    if not df_ts.empty:
        # Extract the orders and revenue data
        orders_data = df_ts['orders'].values
        revenue_data = df_ts['revenue'].values

        # Create histogram for orders
        fig_orders_hist = go.Figure()
        fig_orders_hist.add_trace(go.Histogram(
            x=orders_data,
            nbinsx=30,  # number of bins, adjust as needed
            marker_color='green',
            marker_line_color='black',   # border color
            marker_line_width=1.5,       # border width (pixels)
            opacity=0.65
        ))
        fig_orders_hist.update_layout(
            title=f"Histogram of orders per last {time_ago}" if time_ago != 'Custom' else f"({start_datetime.strftime('%Y-%m-%d')} to {end_datetime.strftime('%Y-%m-%d')})",
            xaxis_title="Number of Orders",
            yaxis_title="Frequency",
            bargap=0.2,
            showlegend=False,
            template="plotly_white"
        )

        # Create histogram for revenue
        fig_revenue_hist = go.Figure()
        fig_revenue_hist.add_trace(go.Histogram(
            x=revenue_data,
            nbinsx=30,
            marker_color='blue',
            marker_line_color='black',   # border color
            marker_line_width=1.5,       # border width (pixels)
            opacity=0.65
        ))
        fig_revenue_hist.update_layout(
            title=f"Histogram of revenue for the last {time_ago}" if time_ago != 'Custom' else f"({start_datetime.strftime('%Y-%m-%d')} to {end_datetime.strftime('%Y-%m-%d')})",
            xaxis_title="Revenue (€)",
            yaxis_title="Frequency",
            bargap=0.2,
            showlegend=False,
            template="plotly_white"
        )

        # Display in Streamlit side by side
        col_hist_orders, col_hist_revenue = st.columns(2)
        with col_hist_orders:
            st.plotly_chart(fig_orders_hist, use_container_width=True)
            
        with col_hist_revenue:
            st.plotly_chart(fig_revenue_hist, use_container_width=True)

    left, right = st.columns(2)

    with left:
        curs.execute("""
        SELECT ToDateTime(ts, 'HH:mm:ss:SSS') AS dateTime, status, price, userId, productsOrdered, totalQuantity
        FROM orders
        ORDER BY ts DESC
        LIMIT 10
        """)

        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
        # Potential todo: convert time to datetime for better formatting in data_editor
        with st.container(border=True):
            st.subheader("Latest orders")
            st.data_editor(
                df,
                column_config={
                    "dateTime": "Time",
                    "status": "Status",
                    "price": st.column_config.NumberColumn("Price", format="%.2f€"),
                    "userId": st.column_config.NumberColumn("User ID", format="%d"),
                    "productsOrdered": st.column_config.NumberColumn("Quantity", help="Quantity of distinct products ordered", format="%d"),
                    "totalQuantity": st.column_config.NumberColumn("Total quantity", help="Total quantity ordered", format="%d"),
                },
                disabled=True
            )

    with right:
        if not df_ts.empty:
            if time_ago == "Custom":
                st.subheader(f"Top 10 Users by Revenue ({start_datetime.strftime('%Y-%m-%d')} to {end_datetime.strftime('%Y-%m-%d')})")
                top_users_query = f"""
                    SELECT userId,
                        COUNT(*) AS orders, 
                        SUM(price) AS revenue
                    FROM orders
                    WHERE ts BETWEEN '{start_str}' AND '{end_str}'
                    GROUP BY userId
                    ORDER BY revenue DESC
                    LIMIT 10
                """
            else:
                st.subheader("Top 10 Users by Revenue")
                top_users_query = f"""
                    SELECT userId,
                        COUNT(*) AS orders, 
                        SUM(price) AS revenue
                    FROM orders
                    WHERE ts > ago('{mapping2[time_ago]['period']}')
                    GROUP BY userId
                    ORDER BY revenue DESC
                    LIMIT 10
                """

            curs.execute(top_users_query)
            df_top_users = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

            if not df_top_users.empty: 
                df_top_users["user_label"] = df_top_users.apply(
                    lambda row: f"User ID: {int(row['userId'])}", axis=1
                )

                fig_top_users = go.Figure()
                fig_top_users.add_trace(go.Bar(
                    x=df_top_users["revenue"],
                    y=df_top_users["user_label"],
                    orientation='h',
                    marker_color='LightSkyBlue',
                    marker_line_color='black',
                    marker_line_width=1.5, 
                    opacity=0.65
                ))
                fig_top_users.update_layout(
                        xaxis=dict(
                        title="Revenue (€)",
                        showgrid=True,
                        gridcolor='lightgray',
                        zeroline=True,
                        zerolinecolor='gray'
                    ),
                    yaxis=dict(
                        title="User",
                        autorange="reversed"
                    ),
                    margin=dict(l=120, r=20, t=40, b=40),
                    template="plotly_white"
                )

                with right:
                    st.plotly_chart(fig_top_users, use_container_width=True)

    lefff, riggg = st.columns(2)

    if time_ago == "Custom":
        where_clause = f"ts BETWEEN '{start_str}' AND '{end_str}'"
    else:
        where_clause = f"ts > ago('{mapping2[time_ago]['period']}')"

    with lefff:
        with st.container(border=True):
            st.subheader("Most popular categories")

            query_categories = f"""
                SELECT "product.category" AS category, 
                    distinctcount(orderId) AS orders,
                    sum("orderItem.quantity") AS quantity
                FROM order_items_enriched
                WHERE {where_clause}
                GROUP BY category
                ORDER BY count(*) DESC
                LIMIT 5
            """

            curs.execute(query_categories)

            df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
            df["quantityPerOrder"] = df["quantity"] / df["orders"]

            st.data_editor(
                df,
                column_config={
                    "category": "Category",
                    "orders": st.column_config.NumberColumn("Number of orders", format="%d"),
                    "quantity": st.column_config.NumberColumn("Total quantity ordered", format="%d"),
                    "quantityPerOrder": st.column_config.NumberColumn("Average quantity per order", format="%d"),
                },
                disabled=True
            )
 
    with riggg:

        query_products = f"""
            SELECT "product.name" AS product, 
                "product.image" AS image,
                distinctcount(orderId) AS orders,
                sum("orderItem.quantity") AS quantity
            FROM order_items_enriched
            WHERE {where_clause}
            GROUP BY product, image
            ORDER BY count(*) DESC
            LIMIT 5
        """

        curs.execute(query_products)

        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
        df["quantityPerOrder"] = df["quantity"] / df["orders"]

        with st.container(border=True):
            st.subheader("Most popular items")
            st.data_editor(
                df,
                use_container_width=True,
                column_config={
                    "product": "Product",
                    "image": st.column_config.ImageColumn(label="Image", width="medium"),
                    "orders": st.column_config.NumberColumn("Number of orders", format="%d"),
                    "quantity": st.column_config.NumberColumn("Total quantity ordered", format="%d"),
                    "quantityPerOrder": st.column_config.NumberColumn("Average quantity per order", format="%d"),
                },
                disabled=True
            )

    A, B = st.columns(2)

    with A:
        with st.container(border=True):
            st.subheader("Busiest Hours Heatmap")
            
            # Initialize session state for heatmap if not exists
            if 'heatmap_date_range' not in st.session_state:
                st.session_state.heatmap_date_range = "LAST_7_DAYS"  # Set 7 days as default
            
            # Create centered buttons with 5 columns (empty columns on sides for centering)
            col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
            
            with col2:
                if st.button("Last 7 Days", key="heatmap_7days"):
                    st.session_state.heatmap_date_range = "LAST_7_DAYS"
            with col3:
                if st.button("Last 30 Days", key="heatmap_30days"):
                    st.session_state.heatmap_date_range = "LAST_30_DAYS"
            with col4:
                if st.button("All Time", key="heatmap_alltime"):
                    st.session_state.heatmap_date_range = "ALL_TIME"

            # Determine date range based on session state
            if st.session_state.heatmap_date_range == "LAST_7_DAYS":
                start_date_heatmap = datetime.now() - timedelta(days=7)
                end_date_heatmap = datetime.now()
                where_clause_heatmap = f"ts BETWEEN '{start_date_heatmap.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' AND '{end_date_heatmap.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
                
                if (end_date_heatmap - start_date_heatmap).days < 7:
                    st.warning("Note: Selected date range is less than 7 days; some days may have no data.", icon="⚠️")
            
            elif st.session_state.heatmap_date_range == "LAST_30_DAYS":
                start_date_heatmap = datetime.now() - timedelta(days=30)
                end_date_heatmap = datetime.now()
                where_clause_heatmap = f"ts BETWEEN '{start_date_heatmap.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' AND '{end_date_heatmap.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
            
            elif st.session_state.heatmap_date_range == "ALL_TIME":
                start_date_heatmap = datetime(1970, 1, 1)
                end_date_heatmap = datetime.now()
                where_clause_heatmap = f"ts BETWEEN '{start_date_heatmap.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' AND '{end_date_heatmap.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
            
            else:  # GLOBAL_PICKER (fallback)
                if time_ago == "Custom":
                    where_clause_heatmap = f"ts BETWEEN '{start_str}' AND '{end_str}'"
                    if (end_datetime - start_datetime).days < 7:
                        st.warning("Note: Selected date range is less than 7 days; some days may have no data.", icon="⚠️")
                else:
                    where_clause_heatmap = f"ts > ago('{mapping2[time_ago]['period']}')"
                    # Clear any previous heatmap if switching to non-custom
                    st.session_state.heatmap_date_range = "LAST_7_DAYS"  # Reset to default
            
            # Only show heatmap if we're in a valid state
            if not (time_ago != "Custom" and st.session_state.heatmap_date_range == "GLOBAL_PICKER"):
                # Query for heatmap data
                heatmap_query = f"""
                    SELECT 
                        DAYOFWEEK(ts) AS weekday_num, 
                        HOUR(ts) AS hour, 
                        COUNT(*) AS order_volume,
                        SUM(price) AS total_sales
                    FROM orders
                    WHERE {where_clause_heatmap}
                    GROUP BY weekday_num, hour
                    ORDER BY weekday_num, hour
                """

                curs.execute(heatmap_query)
                df_heat = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

                if not df_heat.empty:
                    # Proper day mapping - adjust if your database uses different numbering
                    day_mapping = {
                        1: "Sunday",
                        2: "Monday", 
                        3: "Tuesday",
                        4: "Wednesday",
                        5: "Thursday",
                        6: "Friday",
                        7: "Saturday"
                    }
                    
                    # Apply the day mapping
                    df_heat["weekday"] = df_heat["weekday_num"].map(day_mapping)
                    
                    # Ensure all hours (0-23) and all days are represented
                    all_hours = list(range(24))
                    all_days = list(day_mapping.values())  # Use consistent order from mapping
                    
                    # Create complete grid with zeros for missing combinations
                    heatmap_data = []
                    for day in all_days:
                        day_data = []
                        for hour in all_hours:
                            match = df_heat[(df_heat["weekday"] == day) & (df_heat["hour"] == hour)]
                            if not match.empty:
                                # Use the first value if multiple (shouldn't happen due to GROUP BY)
                                day_data.append(float(match["total_sales"].iloc[0]))
                            else:
                                day_data.append(0)  # Explicit zero for missing combinations
                        heatmap_data.append(day_data)
                    
                    # Create heatmap figure
                    fig_heatmap = go.Figure(data=go.Heatmap(
                        z=heatmap_data,
                        x=all_hours,
                        y=all_days,
                        colorscale='Blues',
                        hoverongaps=False,
                        hovertemplate='Day: %{y}<br>Hour: %{x}<br>Sales: €%{z}<extra></extra>',
                        colorbar=dict(title='Sales (€)')
                    ))
                    
                    # Ensure proper day ordering in visualization
                    fig_heatmap.update_layout(
                        title=f"Sales by Hour and Day of Week ({st.session_state.heatmap_date_range.replace('_', ' ')})",
                        xaxis=dict(
                            title="Hour of Day",
                            tickmode='array',
                            tickvals=list(range(24)),
                            ticktext=[f"{h}:00" for h in range(24)]
                        ),
                        yaxis=dict(
                            title="Day of Week",
                            categoryorder='array',
                            categoryarray=all_days  # Use the same order as our mapping
                        ),
                        margin=dict(l=100, r=50, t=80, b=50)
                    )
                    
                    st.plotly_chart(fig_heatmap, use_container_width=True)
                else:
                    st.warning('No order activity to show for this period.', icon="⚠️")

    with B:
        with st.container(border=True):
            st.subheader("Revenue Contribution by Product Category")

            query_treemap = f"""
                SELECT product.category AS category, SUM(product.price) AS revenue
                FROM order_items_enriched
                WHERE {where_clause}
                GROUP BY product.category
            """

            curs.execute(query_treemap)
            df_treemap = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

            if not df_treemap.empty:
                fig_treemap = px.treemap(
                df_treemap,
                path=[px.Constant("all"), 'category'],  # Add a root node explicitly
                values='revenue',
                color='revenue',
                color_continuous_scale='RdBu'
                )

                fig_treemap.update_traces(root_color="white") 

                fig_treemap.update_layout(
                    paper_bgcolor='white',
                    margin=dict(t=50, l=25, r=25, b=25)
                )

                st.plotly_chart(fig_treemap, use_container_width=True)
            else:
                st.info("No revenue data available for selected time range.")

    with st.container(border=True):
        st.subheader("Sales Over Time")
        
        # Check if we should show the chart
        show_chart = True
        if time_ago != "Custom":
            show_chart = False
            st.info("Please select 'Custom' date range and choose at least 1 month period to view this chart", icon="ℹ️")
        else:
            # For custom range, check if at least 1 month is selected
            date_diff = (end_datetime - start_datetime).days
            if date_diff < 30:  # Approx 1 month
                show_chart = False
                st.warning('Please select a time period of at least 1 month to view sales trends.', icon="⚠️")
        
        if show_chart:
            # Use session state to persist granularity selection
            if 'sales_granularity' not in st.session_state:
                st.session_state.sales_granularity = "MONTHLY"
            
            # Centered buttons using columns
            cols = st.columns((2, 1, 1, 2))  # Wider spacing for better centering
            with cols[1]:
                if st.button("Monthly View", key="monthly_view_btn"):
                    st.session_state.sales_granularity = "MONTHLY"
            with cols[2]:
                if st.button("Yearly View", key="yearly_view_btn"):
                    st.session_state.sales_granularity = "YEARLY"
            
            # Query based on granularity
            if st.session_state.sales_granularity == "MONTHLY":
                sales_query = f"""
                    SELECT 
                        ToDateTime(DATETRUNC('MONTH', ts), 'yyyy-MM-dd HH:mm:ss') AS time_period,
                        SUM(price) AS total_sales
                    FROM orders
                    WHERE ts BETWEEN '{start_str}' AND '{end_str}'
                    GROUP BY DATETRUNC('MONTH', ts)
                    ORDER BY time_period
                """
                xaxis_title = "Month"
            else:  # YEARLY
                sales_query = f"""
                    SELECT 
                        ToDateTime(DATETRUNC('YEAR', ts), 'yyyy-MM-dd HH:mm:ss') AS time_period,
                        SUM(price) AS total_sales
                    FROM orders
                    WHERE ts BETWEEN '{start_str}' AND '{end_str}'
                    GROUP BY DATETRUNC('YEAR', ts)
                    ORDER BY time_period
                """
                xaxis_title = "Year"

            curs.execute(sales_query)
            df_sales = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
            
            if not df_sales.empty:
                # Convert time_period to datetime
                df_sales['time_period'] = pd.to_datetime(df_sales['time_period'])
                
                # Create complete date range
                min_date = df_sales['time_period'].min()
                max_date = df_sales['time_period'].max()
                
                if st.session_state.sales_granularity == "MONTHLY":
                    all_periods = pd.date_range(start=min_date, end=max_date, freq='MS')
                    df_all = pd.DataFrame({'time_period': all_periods})
                    df_sales = df_all.merge(df_sales, on='time_period', how='left').fillna(0)
                    df_sales['time_period_display'] = df_sales['time_period'].dt.strftime('%b %Y')
                else:
                    all_periods = pd.date_range(start=min_date, end=max_date, freq='YS')
                    df_all = pd.DataFrame({'time_period': all_periods})
                    df_sales = df_all.merge(df_sales, on='time_period', how='left').fillna(0)
                    df_sales['time_period_display'] = df_sales['time_period'].dt.strftime('%Y')
                
                # Create line chart with shaded area
                fig_sales = go.Figure()
                
                # Add shaded area under the curve
                fig_sales.add_trace(go.Scatter(
                    x=df_sales['time_period_display'],
                    y=df_sales['total_sales'],
                    fill='tozeroy',
                    fillcolor='rgba(65, 105, 225, 0.2)',
                    line=dict(color='rgba(0,0,0,0)'),
                    hoverinfo='skip',
                    showlegend=False
                ))
                
                # Add main line
                fig_sales.add_trace(go.Scatter(
                    x=df_sales['time_period_display'],
                    y=df_sales['total_sales'],
                    mode='lines+markers',
                    line=dict(color='royalblue', width=2),
                    marker=dict(size=8, color='royalblue'),
                    name='Total Sales',
                    hovertemplate='%{x}<br>Sales: €%{y:,.2f}<extra></extra>'
                ))
                
                fig_sales.update_layout(
                    title=f"Sales Over Time ({start_datetime.strftime('%Y-%m-%d')} to {end_datetime.strftime('%Y-%m-%d')})",
                    xaxis=dict(
                        title=xaxis_title,
                        type='category',
                        tickmode='array',
                        tickvals=df_sales['time_period_display'],
                        tickangle=-45   ,
                        tickfont=dict(size=10),  # Adjust font size if needed
                        nticks=len(df_sales),    # Force showing all ticks
                        autorange=True,           # Ensure full range is visible
                        fixedrange=False
                    ),
                    yaxis=dict(
                        title='Sales (€)',
                        tickprefix='€'
                    ),
                    hovermode='x unified',
                    margin=dict(l=50, r=50, t=80, b=100),
                    showlegend=False,
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                
                st.plotly_chart(fig_sales, use_container_width=True)
            else:
                st.warning('No sales data available for the selected time range.', icon="⚠️")

    curs.close()

if auto_refresh:
    time.sleep(number)
    st.experimental_rerun()