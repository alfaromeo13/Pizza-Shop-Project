import os
import time
import pandas as pd
import streamlit as st
from pinotdb import connect
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

def plot_time_series(df_ts, time_col='dateMin', time_mode="Custom"):
    # Convert to datetime
    df_ts[time_col] = pd.to_datetime(df_ts[time_col])
    df_ts = df_ts.set_index(time_col).sort_index()

    # Infer best granularity
    total_seconds = (df_ts.index.max() - df_ts.index.min()).total_seconds()
    if total_seconds < 60 * 10:
        freq = 'S'
    elif total_seconds < 60 * 60:
        freq = 'T'
    else:
        freq = 'H'

    # Resample and fill missing
    df_resampled = df_ts.resample(freq).sum().fillna(0)

    # Determine if we're in live mode (non-Custom)
    is_live = time_mode != "Custom"

    col1, col2 = st.columns(2)

    for metric, col, color in [('orders', col1, 'green'), ('revenue', col2, 'blue')]:
        with col:
            st.markdown(f"### <div style='text-align: center;'>{metric.capitalize()} Over Time</div>", unsafe_allow_html=True)

            fig = go.Figure()

            if is_live and df_resampled.shape[0] > 1:
                # Split complete vs in-progress
                latest = df_resampled.index.max()
                second_latest = df_resampled.index.sort_values()[-2]

                complete = df_resampled[df_resampled.index < latest]
                incomplete = df_resampled[df_resampled.index >= second_latest]

                fig.add_trace(go.Scatter(
                    x=complete.index,
                    y=complete[metric],
                    mode='lines',
                    line=dict(color=color, dash='solid'),
                    name=f"{metric.capitalize()} (complete)"
                ))
                fig.add_trace(go.Scatter(
                    x=incomplete.index,
                    y=incomplete[metric],
                    mode='lines',
                    line=dict(color=color, dash='dash'),
                    name=f"{metric.capitalize()} (in-progress)"
                ))
            else:
                # Just plot everything normally
                fig.add_trace(go.Scatter(
                    x=df_resampled.index,
                    y=df_resampled[metric],
                    mode='lines',
                    line=dict(color=color),
                    name=metric.capitalize()
                ))

            fig.update_layout(
                xaxis=dict(type='date'),
                margin=dict(l=0, r=0, t=40, b=0),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

auto_refresh = True # We seta uto refresh of a page to True
pinot_host = os.environ.get("PINOT_SERVER", "pinot-broker")
pinot_port = os.environ.get("PINOT_PORT", 8099)
conn = connect(pinot_host, pinot_port)
st.set_page_config(layout="wide")

st.markdown("<h1 style='text-align: center;'>Pizza Shop orders analytics 🍕</h1><br>", unsafe_allow_html=True)

mapping2 = {
    "1 hour": {"minutes": 60, "previous": 120, "granularity": "minute"},
    "30 minutes": {"minutes": 30, "previous": 60, "granularity": "minute"},
    "10 minutes": {"minutes": 10, "previous": 20, "granularity": "second"},
    "5 minutes": {"minutes": 5, "previous": 10, "granularity": "second"},
    "1 minute": {"minutes": 1, "previous": 2, "granularity": "second"}
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

# We (only) proceed when Pinot is available.
if pinot_available:
    if time_ago != "Custom":
        now = datetime.now()

        minutes = mapping2[time_ago]["minutes"]
        previous_minutes = mapping2[time_ago]["previous"]

        start_dt = now - timedelta(minutes=minutes)
        previous_start_dt = now - timedelta(minutes=previous_minutes)

        # Format for Pinot
        start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        previous_start_str = previous_start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        now_str = now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        query = f"""
            SELECT 
                COUNT(*) FILTER(WHERE ts BETWEEN '{start_str}' AND '{now_str}') AS events1Min, 
                COUNT(*) FILTER(WHERE ts BETWEEN '{previous_start_str}' AND '{start_str}') AS events1Min2Min,
                SUM(price) FILTER(WHERE ts BETWEEN '{start_str}' AND '{now_str}') AS total1Min,
                SUM(price) FILTER(WHERE ts BETWEEN '{previous_start_str}' AND '{start_str}') AS total1Min2Min
            FROM orders
            WHERE ts BETWEEN '{previous_start_str}' AND '{now_str}'
            LIMIT 1
        """

        curs.execute(query)
        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        metrics_container = st.container(border=True)
        metrics_container.subheader(f"Orders in the last {time_ago}")
        num_orders, rev, order_val = metrics_container.columns(3)

        num_orders.metric(
            label="Number of orders",
            value="{:,}".format(int(df['events1Min'].values[0]))
            if df['events1Min2Min'].values[0] > 0 else None
        )

        rev.metric(
            label="Revenue in €",
            value="{:,.2f}".format(df['total1Min'].values[0]),
            delta="{:,.2f}".format(df['total1Min'].values[0] - df['total1Min2Min'].values[0])
            if df['total1Min2Min'].values[0] > 0 else None
        )

        average_order_value_1min = df['total1Min'].values[0] / int(df['events1Min'].values[0]) if df['events1Min'].values[0] > 0 else 0
        average_order_value_1min_2min = df['total1Min2Min'].values[0] / int(df['events1Min2Min'].values[0]) if int(df['events1Min2Min'].values[0]) > 0 else 0

        order_val.metric(
            label="Average order value in €",
            value="{:,.2f}".format(average_order_value_1min),
            delta="{:,.2f}".format(average_order_value_1min - average_order_value_1min_2min)
            if average_order_value_1min_2min > 0 else None
        )

        granularity = mapping2[time_ago]["granularity"]
        query_ts = f"""
            SELECT ToDateTime(DATETRUNC('{granularity}', ts), 'yyyy-MM-dd HH:mm:ss') AS dateMin, 
                COUNT(*) AS orders, 
                SUM(price) AS revenue
            FROM orders
            WHERE ts BETWEEN '{start_str}' AND '{now_str}'
            GROUP BY dateMin
            ORDER BY dateMin ASC
            LIMIT 10000
        """

        curs.execute(query_ts)
        df_ts = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        df_ts['dateMin'] = pd.to_datetime(df_ts['dateMin'])
        df_ts = df_ts.set_index('dateMin').sort_index()

        freq = {'second': 'S', 'minute': 'T', 'hour': 'H'}.get(granularity, 'T')
        df_ts = df_ts.resample(freq).sum().fillna(0).reset_index()

        if df_ts.shape[0] > 1:
            avg_orders = df_ts['orders'][:-1].mean()
            if df_ts.iloc[-1]['orders'] < 0.1 * avg_orders:
                df_ts = df_ts.iloc[:-1]

    else:
        # Similar story but for custom time range query
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
            order by dateMin asc
            LIMIT 10000
        """

        curs.execute(query_ts)

        df_ts = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    # Plotting timeseries
    if df_ts.shape[0] > 1:
        plot_time_series(df_ts, time_col='dateMin', time_mode=time_ago)

    query_raw_orders = f"""
        SELECT ToDateTime(ts, 'yyyy-MM-dd HH:mm:ss') AS ts, userId, price
        FROM orders
        WHERE ts BETWEEN '{start_str}' AND '{now_str if time_ago != 'Custom' else end_str}'
        LIMIT 100000
    """
    curs.execute(query_raw_orders)

    df_orders = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
    df_orders['ts'] = pd.to_datetime(df_orders['ts'])

    st.markdown("<br><br>", unsafe_allow_html=True) 

    if not df_ts.empty:
        # Extract the orders and revenue data
        orders_data = df_ts['orders'].values
        revenue_data = df_ts['revenue'].values

        c, cc = st.columns(2)
        with c:
            curs.execute("""
            SELECT ToDateTime(ts, 'HH:mm:ss:SSS') AS dateTime, status, price, userId, productsOrdered, totalQuantity
            FROM orders
            ORDER BY ts DESC
            LIMIT 10
            """)

            df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

            st.markdown("### <div style='text-align: center;'>Latest Orders</div>", unsafe_allow_html=True)
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
                disabled=True,
                use_container_width=True
            )
   
        with cc:
            st.markdown("### <div style='text-align: center;'>Distribution of Orders by Value Range</div>", unsafe_allow_html=True)

            # Define revenue bins
            revenue_bins = [0, 10, 20, 30, 50, 100, float('inf')]
            bin_labels = ["0–10€", "10–20€", "20–30€", "30–50€", "50–100€", "100€+"]

            # Cut revenue values into bins
            binned = pd.cut(revenue_data, bins=revenue_bins, labels=bin_labels, right=False)

            # Count number of orders per bin
            bin_counts = binned.value_counts().sort_index()

            fig_revenue_pie = go.Figure(data=[go.Pie(
                labels=bin_counts.index,
                values=bin_counts.values,
                hole=0.4,  # donut style
                marker=dict(line=dict(color='black', width=1))
            )])

            fig_revenue_pie.update_layout(
                margin=dict(t=40, b=20),
                showlegend=True
            )

            st.plotly_chart(fig_revenue_pie, use_container_width=True)

    # First format time column into hourly buckets
    df_orders['hour'] = df_orders['ts'].dt.strftime('%Y-%m-%d %H:00')

    st.markdown("<br><br>", unsafe_allow_html=True) 

    st.markdown("""<h3 style='text-align: center; margin-bottom: 5px;'>Distribution of Order Prices</h3>""", unsafe_allow_html=True)
    
    fig_box = go.Figure()
    fig_box.add_trace(go.Box(
        x=df_orders['hour'],
        y=df_orders['price'],
        boxpoints='outliers',
        marker_color='DarkSlateBlue'
    ))
    fig_box.update_layout(
        xaxis_title='Time (Hour)',
        yaxis_title='Order Value (€)',
        template='plotly_white',
        height=600,  # Increase height for better vertical scale
        margin=dict(t=40, b=60),  # Reduce space above and below the title/chart
        yaxis=dict(
            title_font=dict(size=14),
            tickfont=dict(size=12),
            automargin=True
        ),
        xaxis=dict(
            title_font=dict(size=14),
            tickfont=dict(size=12),
            automargin=True
        )
    )

    st.plotly_chart(fig_box, use_container_width=True)

    st.markdown("<br><br>", unsafe_allow_html=True)

    left, right = st.columns(2)

    with left:

        st.markdown("### <div style='text-align: center;'>Top Users by Average Order Value (€)</div>", unsafe_allow_html=True)

        df_avg_order_value = df_orders.groupby('userId').agg(
            total_spent=('price', 'sum'),
            order_count=('price', 'count')
        ).assign(avg_order_value=lambda x: x['total_spent'] / x['order_count']).reset_index()

        df_top_avg_users = df_avg_order_value.sort_values(by='avg_order_value', ascending=False).head(10)

        fig_avg_order = go.Figure()
        fig_avg_order.add_trace(go.Bar(
            x=df_top_avg_users['avg_order_value'],
            y=[f"User {uid}" for uid in df_top_avg_users['userId']],
            orientation='h',
            marker_color='LightCoral',
            marker_line_color='black',
            marker_line_width=1.5, 
            opacity=0.65
        ))
        fig_avg_order.update_layout(
            xaxis=dict(
                title='Avg Order Value (€)',
                showgrid=True,         # Enable vertical gridlines
                gridcolor='lightgray', # Optional: grid line color
                gridwidth=1            # Optional: grid line width
            ),
            yaxis=dict(
                autorange='reversed',
                showgrid=True,         # Enable horizontal gridlines
                gridcolor='lightgray',
                gridwidth=1
            ),
            margin=dict(l=120, r=20, t=40, b=40), 
            template='plotly_white'
        )
        st.plotly_chart(fig_avg_order, use_container_width=True)

    with right:
        if not df_ts.empty:

            st.markdown("### <div style='text-align: center;'>Top 10 Users by Revenue (€)</div>", unsafe_allow_html=True)

            if time_ago == "Custom":
                top_users_start = start_datetime
                top_users_end = end_datetime + timedelta(seconds=1)
            else:
                now = datetime.now()
                minutes = mapping2[time_ago]["minutes"]
                top_users_start = now - timedelta(minutes=minutes)
                top_users_end = now

            start_str_top_users = top_users_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            end_str_top_users = top_users_end.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            top_users_query = f"""
                SELECT userId,
                    COUNT(*) AS orders, 
                    SUM(price) AS revenue
                FROM orders
                WHERE ts BETWEEN '{start_str_top_users}' AND '{end_str_top_users}'
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

                st.plotly_chart(fig_top_users, use_container_width=True)
    
    st.markdown("<br><br>", unsafe_allow_html=True)

    lefff, riggg = st.columns(2)

    if time_ago == "Custom":
        range_start = start_datetime
        range_end = end_datetime + timedelta(seconds=1)
    else:
        now = datetime.now()
        minutes = mapping2[time_ago]["minutes"]
        range_start = now - timedelta(minutes=minutes)
        range_end = now

    start_str_range = range_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    end_str_range = range_end.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    where_clause = f"ts BETWEEN '{start_str_range}' AND '{end_str_range}'"

    with lefff:

        st.markdown("### <div style='text-align: center;'>Most popular categories</div>", unsafe_allow_html=True)

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

        if not df.empty:
            df["quantityPerOrder"] = df["quantity"] / df["orders"]

            st.data_editor(
                df,
                column_config={
                    "category": "Category",
                    "orders": st.column_config.NumberColumn("Number of orders", format="%d"),
                    "quantity": st.column_config.NumberColumn("Total quantity ordered", format="%d"),
                    "quantityPerOrder": st.column_config.NumberColumn("Average quantity per order", format="%d"),
                },
                disabled=True,
                use_container_width=True
            )
        else:
            st.info("No data available for selected period.")

    with riggg:

        st.markdown("### <div style='text-align: center;'>Most popular items</div>", unsafe_allow_html=True)

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

        if not df.empty:
            df["quantityPerOrder"] = df["quantity"] / df["orders"]

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
        else:
            st.info("No data available for selected period.")
    
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    A, B = st.columns(2) 

    with A:
        
        st.markdown("### <div style='text-align: center;'>Order Activity Heatmap</div>", unsafe_allow_html=True)

        if time_ago != "Custom":
            filter_option = st.radio(
                "Select heatmap data range",
                options=["Last 7 days", "Last 30 days", "All time"],
                index=0,  # Default to "Last 7 days"
                horizontal=True
            )
        else:
            filter_option = None  # Not used when Custom is selected

        if time_ago == "Custom":
            heatmap_start = start_datetime
            heatmap_end = end_datetime + timedelta(seconds=1)
        else:
            now = datetime.now()
          
            if filter_option == "Last 7 days":
                heatmap_start = now - timedelta(days=7)
            elif filter_option == "Last 30 days":
                heatmap_start = now - timedelta(days=30)
            else:
                heatmap_start = datetime(2023, 1, 1)
            heatmap_end = now

        # Format times for SQL
        heatmap_start_str = heatmap_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        heatmap_end_str = heatmap_end.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        query_heatmap = f"""
            SELECT 
                dayOfWeek(ts) AS day, 
                hour(ts) AS hour,
                COUNT(*) AS orders
            FROM orders
            WHERE ts BETWEEN '{heatmap_start_str}' AND '{heatmap_end_str}'
            GROUP BY day, hour
            LIMIT 10000
        """

        curs.execute(query_heatmap)
        df_heatmap = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        if not df_heatmap.empty:
            day_map = {
                1: "Sunday",
                2: "Monday",
                3: "Tuesday",
                4: "Wednesday",
                5: "Thursday",
                6: "Friday",
                7: "Saturday"
            }
            
            df_heatmap['day'] = df_heatmap['day'].map(day_map)

            # Pivot to matrix format
            heatmap_matrix = df_heatmap.pivot(index='day', columns='hour', values='orders').fillna(0)

            # Ensure full hour range (0–23) is present
            all_hours = list(range(24))
            heatmap_matrix = heatmap_matrix.reindex(columns=all_hours, fill_value=0)

            # Reorder rows Monday to Sunday
            weekdays_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            heatmap_matrix = heatmap_matrix.reindex(weekdays_order)

            # Create matching tooltip text matrix
            hover_text = []
            for day in heatmap_matrix.index:
                row = []
                for hour in heatmap_matrix.columns:
                    value = heatmap_matrix.loc[day, hour]
                    count = int(value) if pd.notna(value) else 0
                    row.append(f"{day}, {hour:02}h: {count} orders")
                hover_text.append(row)

            hour_labels = [f"{h:02}" for h in heatmap_matrix.columns]

            fig_heatmap = go.Figure(data=go.Heatmap(
                z=heatmap_matrix.values,
                x=hour_labels,
                y=heatmap_matrix.index,
                text=hover_text,
                hoverinfo="text",
                colorscale='Blues'
            ))

            fig_heatmap.update_layout(
                xaxis=dict(
                    title="Hour of Day",
                    tickmode='array',
                    tickvals=list(range(24)),
                    ticktext=[f"{h:02}h" for h in range(24)],
                    tickangle=45
                ),
                yaxis_title="Day of Week",
                margin=dict(l=40, r=20, t=40, b=40)
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("No data available to render heatmap for the selected period.")

    with B:

        st.markdown("""
            <h3 style='text-align: center; margin-bottom: 5px;'>Revenue Contribution by Product Category</h3>
        """, unsafe_allow_html=True)

        query_treemap = f"""
            SELECT product.category AS category, SUM(product.price) AS revenue
            FROM order_items_enriched
            WHERE {where_clause}
            GROUP BY product.category
        """
        
        curs.execute(query_treemap)
        df_treemap = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        if not df_treemap.empty:

            df_treemap = df_treemap.sort_values(by="revenue", ascending=False)

            fig_treemap = px.treemap(
                df_treemap,
                path=[px.Constant("all"), 'category'],  # Add a root node explicitly
                values='revenue',
                color='revenue',
                color_continuous_scale='RdBu'
            )

            fig_treemap.update_traces(
                hovertemplate='<b>%{label}</b><br>Revenue: €%{value:,.2f}<extra></extra>',
                root_color="white"
            ) 

            fig_treemap.update_layout(
                paper_bgcolor='white',
                margin=dict(t=50, l=25, r=25, b=25)
            )

            st.plotly_chart(fig_treemap, use_container_width=True)
        else:
            st.info("No revenue data available for selected time range.")

    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("### <div style='text-align: center;'>Sales Over Time</div>", unsafe_allow_html=True)
      
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
        
        display_to_value = {"Monthly": "MONTHLY", "Yearly": "YEARLY"}
        value_to_display = {v: k for k, v in display_to_value.items()}

        # Get current display value based on session state
        current_display = value_to_display.get(st.session_state.get("sales_granularity", "MONTHLY"), "Monthly")

        # Render radio with friendly labels
        selected_display = st.radio(
            "Select Granularity",
            options=["Monthly", "Yearly"],
            index=["Monthly", "Yearly"].index(current_display),
            key="granularity_radio",
            horizontal=True
        )

        # Save selected internal value back to session state
        st.session_state.sales_granularity = display_to_value[selected_display]


        # Query based on granularity
        if st.session_state.sales_granularity == "MONTHLY":
            sales_query = f"""
                SELECT 
                    ToDateTime(DATETRUNC('MONTH', ts), 'yyyy-MM-dd HH:mm:ss') AS time_period,
                    SUM(price) AS total_sales
                FROM orders
                WHERE ts BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY DATETRUNC('MONTH', ts)
                ORDER BY time_period limit 100
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