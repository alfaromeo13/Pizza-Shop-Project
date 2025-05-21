import streamlit as st
import pandas as pd
from pinotdb import connect
from datetime import datetime, timedelta
import os
import time
import plotly.graph_objects as go
import plotly.express as px
from pandas.api.types import is_datetime64_any_dtype as is_datetime

pinot_host = os.environ.get("PINOT_SERVER", "pinot-broker")
pinot_port = os.environ.get("PINOT_PORT", 8099)

auto_refresh = True

conn = connect(pinot_host, pinot_port)

st.set_page_config(layout="wide")
st.title("Pizza Shop orders analytics 🍕")

mapping2 = {
    "1 hour": {"period": "PT60M", "previousPeriod": "PT120M", "granularity": "minute"},
    "30 minutes": {"period": "PT30M", "previousPeriod": "PT60M", "granularity": "minute"},
    "10 minutes": {"period": "PT10M", "previousPeriod": "PT20M", "granularity": "second"},
    "5 minutes": {"period": "PT5M", "previousPeriod": "PT10M", "granularity": "second"},
    "1 minute": {"period": "PT1M", "previousPeriod": "PT2M", "granularity": "second"},
}

time_options = list(mapping2.keys()) + ["Custom"]

if "sleep_time" not in st.session_state:
    st.session_state.sleep_time = 5

if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True

with st.expander("Dashboard settings", expanded=True):
    time_ago = st.radio("Display data from the last", options=time_options,
                        horizontal=True, index=len(time_options) - 2, key="time_ago")

    if time_ago == "Custom":
        st.session_state.auto_refresh = False
        auto_refresh = False

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=7))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
        with col3:
            start_time = st.time_input("Start Time", value=datetime.strptime("08:00", "%H:%M").time())
        with col4:
            end_time = st.time_input("End Time", value=datetime.strptime("20:00", "%H:%M").time())

        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
        st.info(f"**Custom range selected:** {start_datetime} to {end_datetime}")
    else:
        if not st.session_state.auto_refresh:
            st.session_state.auto_refresh = True

        auto_refresh = st.toggle('Auto-refresh', value=st.session_state.auto_refresh)
        st.session_state.auto_refresh = auto_refresh

        if auto_refresh:
            number = st.number_input('Refresh rate in seconds', value=st.session_state.sleep_time)
            st.session_state.sleep_time = number

now = datetime.now()
dt_string = now.strftime("%B %-d, %Y at %-I:%M:%S %p")
if st.session_state.auto_refresh:
    st.info(f"**Last update:** {dt_string}", icon="ℹ️")

curs = conn.cursor()

pinot_available = False
try:
    curs.execute("select * FROM orders where ts > ago('PT2M')")
    pinot_available = curs.description is not None
    if not pinot_available:
        st.warning("Connected to Pinot, but no orders imported", icon="⚠️")
except Exception as e:
    st.warning(f"Unable to connect to or query Apache Pinot [{pinot_host}:{pinot_port}] Exception: {e}", icon="⚠️")

if pinot_available:
    if time_ago != "Custom":
        query = """
            select count(*) FILTER(WHERE ts > ago(%(nearTimeAgo)s)) AS events1Min,
                   count(*) FILTER(WHERE ts <= ago(%(nearTimeAgo)s) AND ts > ago(%(timeAgo)s)) AS events1Min2Min,
                   sum(price) FILTER(WHERE ts > ago(%(nearTimeAgo)s)) AS total1Min,
                   sum(price) FILTER(WHERE ts <= ago(%(nearTimeAgo)s) AND ts > ago(%(timeAgo)s)) AS total1Min2Min
            from orders
            where ts > ago(%(timeAgo)s)
            limit 1
        """

        curs.execute(query, {
            "timeAgo": mapping2[time_ago]["previousPeriod"],
            "nearTimeAgo": mapping2[time_ago]["period"]
        })

        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        metrics_container = st.container(border=True)
        metrics_container.subheader(f"Orders in the last {time_ago}")

        num_orders, rev, order_val = metrics_container.columns(3)

        num_orders.metric(
            label="Number of orders",
            value="{:,}".format(int(df['events1Min'].values[0])),
            delta="{:,}".format(int(df['events1Min'].values[0] - df['events1Min2Min'].values[0]))
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

        curs.execute(query_ts, {"timeAgo": mapping2[time_ago]["period"], "granularity": mapping2[time_ago]["granularity"]})
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
        df_ts_melt = pd.melt(df_ts, id_vars=['dateMin'], value_vars=['revenue', 'orders'])

        col1, col2 = st.columns(2)

        with col1:
            orders = df_ts_melt[df_ts_melt.variable == "orders"]
            latest_date = orders.dateMin.max()
            latest_date_but_one = orders.sort_values(by=["dateMin"], ascending=False).iloc[[1]].dateMin.values[0]

            revenue_complete = orders[orders.dateMin < latest_date]
            revenue_incomplete = orders[orders.dateMin >= latest_date_but_one]

            fig = go.FigureWidget(data=[
                go.Scatter(x=revenue_complete.dateMin, y=revenue_complete.value, mode='lines', line={'dash': 'solid', 'color': 'green'}),
                go.Scatter(x=revenue_incomplete.dateMin, y=revenue_incomplete.value, mode='lines', line={'dash': 'dash', 'color': 'green'}),
            ])
            fig.update_layout(
                showlegend=False, 
                title=f"Orders per {mapping2[time_ago]['granularity'] if time_ago != 'Custom' else 'timestamp'}",
                              margin=dict(l=0, r=0, t=40, b=0))
            fig.update_yaxes(range=[0, df_ts["orders"].max() * 1.1])
            with st.container(border=True):
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            revenue = df_ts_melt[df_ts_melt.variable == "revenue"]
            latest_date = revenue.dateMin.max()
            latest_date_but_one = revenue.sort_values(by=["dateMin"], ascending=False).iloc[[1]].dateMin.values[0]

            revenue_complete = revenue[revenue.dateMin < latest_date]
            revenue_incomplete = revenue[revenue.dateMin >= latest_date_but_one]

            fig = go.FigureWidget(data=[
                go.Scatter(x=revenue_complete.dateMin, y=revenue_complete.value, mode='lines', line={'dash': 'solid', 'color': 'blue'}),
                go.Scatter(x=revenue_incomplete.dateMin, y=revenue_incomplete.value, mode='lines', line={'dash': 'dash', 'color': 'blue'}),
            ])
            fig.update_layout(showlegend=False, title=f"Revenue per {mapping2[time_ago]['granularity'] if time_ago != 'Custom' else 'timestamp'}",
                              margin=dict(l=0, r=0, t=40, b=0))
            fig.update_yaxes(range=[0, df_ts["revenue"].max() * 1.1])
            with st.container(border=True):
                st.plotly_chart(fig, use_container_width=True)

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
            title=f"Histogram of Orders per {mapping2[time_ago]['granularity'] if time_ago != 'Custom' else 'timestamp'}",
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
            
            title=f"Revenue per {mapping2[time_ago]['granularity'] if time_ago != 'Custom' else 'From {start_datetime} to {end_datetime}'}",
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
                    title=f"Top 10 Users by Revenue ({mapping2[time_ago]['granularity'] if time_ago != 'Custom' else 'Custom Range'})",
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
        # --- HEATMAP: Hourly Order Activity by Day of Week ---
        if time_ago == "Custom":
            heatmap_query = f"""
                SELECT 
                    DAYOFWEEK(ts) AS weekday, 
                    HOUR(ts) AS hour, 
                    COUNT(*) AS order_count
                FROM orders
                WHERE ts BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY weekday, hour
            """
        else:
            heatmap_query = f"""
                SELECT 
                    DAYOFWEEK(ts) AS weekday, 
                    HOUR(ts) AS hour, 
                    COUNT(*) AS order_count
                FROM orders
                WHERE ts > ago('{mapping2[time_ago]['period']}')
                GROUP BY weekday, hour
            """

        curs.execute(heatmap_query)
        df_heat = pd.DataFrame(curs, columns=[item[0] for item in curs.description])  # df_heat always defined

        if df_heat.empty:
            st.info("No order activity to show for this period.")
        else:
            day_labels = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
            df_heat["weekday"] = df_heat["weekday"].apply(lambda x: day_labels[int(x) - 1])

            heatmap_df = df_heat.pivot(index="weekday", columns="hour", values="order_count").fillna(0)

            fig_heatmap = px.imshow(
                heatmap_df,
                labels=dict(x="Hour of Day", y="Day of Week", color="Orders"),
                x=heatmap_df.columns,
                y=heatmap_df.index,
                color_continuous_scale="Blues"
            )
            fig_heatmap.update_layout(title="Order Activity Heatmap")
            st.plotly_chart(fig_heatmap, use_container_width=True)
    with B:
        # --- BAR PLOT: Monthly Revenue Trend ---
        if time_ago == "Custom":
            monthly_query = f"""
                SELECT 
                    DATETRUNC('MONTH', ts) AS month,
                    SUM(price) AS monthly_revenue
                FROM orders
                WHERE ts BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY DATETRUNC('MONTH', ts)
                ORDER BY month
            """
        else:
            monthly_query = f"""
                SELECT 
                    DATETRUNC('MONTH', ts) AS month,
                    SUM(price) AS monthly_revenue
                FROM orders
                WHERE ts > ago('{mapping2[time_ago]['period']}')
                GROUP BY DATETRUNC('MONTH', ts)
                ORDER BY month
            """

        curs.execute(monthly_query)
        df_monthly = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

        if not df_monthly.empty:
            fig_monthly = px.bar(
                df_monthly,
                x="month",
                y="monthly_revenue",
                title="Monthly Revenue Trend",
                labels={"monthly_revenue": "Revenue (€)", "month": "Month"},
                color_discrete_sequence=["#4C78A8"]
            )
            fig_monthly.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_monthly, use_container_width=True)


    curs.close()

if auto_refresh:
    time.sleep(number)
    st.experimental_rerun()