"""
COVID-19 Analytics Dashboard
Built with Streamlit
"""

import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

# ========================================
# PAGE CONFIG
# ========================================

st.set_page_config(
    page_title="COVID-19 Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ========================================
# DATABASE CONNECTION
# ========================================


# @st.cache_resource
# def get_db_connection():
#     """Get database connection"""
#     return psycopg2.connect(
#         host=os.getenv("POSTGRES_HOST"),
#         port=os.getenv("POSTGRES_PORT"),
#         database=os.getenv("POSTGRES_DB"),
#         user=os.getenv("POSTGRES_USER"),
#         password=os.getenv("POSTGRES_PASSWORD"),
#     )
@st.cache_resource
def get_engine():
    """Create SQLAlchemy engine"""
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )

def query_db(sql, params=None):
    """Execute query and return DataFrame"""
    engine = get_engine()
    try:
        df = pd.read_sql_query(sql, engine, params=params)
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()


# ========================================
# STYLING
# ========================================

st.markdown(
    """
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""",
    unsafe_allow_html=True,
)

# ========================================
# SIDEBAR
# ========================================

st.sidebar.title("🎛️ Dashboard Controls")

# View selector
view_type = st.sidebar.selectbox(
    "📊 Select View",
    [
        "Overview",
        "National Daily Metrics",
        "State Metrics",
        "County Hotspots",
        # "Weekly Summary",
    ],
)

# Date range
st.sidebar.subheader("📅 Date Range")
# days_back = st.sidebar.slider("Days to show", 7, 90, 30, 7)
start_date, end_date = st.sidebar.date_input(
    "Select date range",
    value=(date(2020, 1, 1), date(2022, 6, 30)),
    min_value=date(2020, 1, 1),
    max_value=date(2022, 6, 30),
)

# Visualization type (for some views)
if view_type in ["National Daily Metrics", "State Metrics"]:
    viz_type = st.sidebar.radio(
        "📈 Visualization Type", ["Line Chart", "Bar Chart", "Area Chart", "Both"]
    )
else:
    viz_type = None

# Refresh
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info(
    f"""
**Settings**
- View: {view_type}
- Range: {start_date} to {end_date}
- Updated: {datetime.now().strftime('%H:%M')}
"""
)

# ========================================
# MAIN HEADER
# ========================================

st.markdown(
    """
    <h1 style='text-align: center; color: #1f77b4; margin-bottom: 2rem;'>
        📊 COVID-19 Analytics Dashboard
    </h1>
""",
    unsafe_allow_html=True,
)

# ========================================
# OVERVIEW
# ========================================

if view_type == "Overview":
    st.header("🎯 Dashboard Overview")

    # Get latest stats from all tables
    col1, col2, col3, col4 = st.columns(4)

    # Daily metrics - latest
    daily_latest = query_db(
        """
        SELECT new_cases_total, new_deaths_total, data_quality_score
        FROM analytics.daily_national_metrics
        ORDER BY metric_date DESC LIMIT 1
    """
    )

    # State metrics - count
    state_count = query_db(
        """
        SELECT COUNT(DISTINCT state_name) as count
        FROM analytics.state_metrics
        WHERE metric_date = (SELECT MAX(metric_date) FROM analytics.state_metrics)
    """
    )

    # County metrics - count
    county_count = query_db(
        """
        SELECT COUNT(*) as count
        FROM analytics.county_metrics
        WHERE metric_date = (SELECT MAX(metric_date) FROM analytics.county_metrics)
    """
    )

    # # Weekly summary - latest
    # weekly_latest = query_db(
    #     """
    #     SELECT total_new_cases, cases_wow_change
    #     FROM analytics.weekly_summary
    #     ORDER BY week_start_date DESC LIMIT 1
    # """
    # )

    with col1:
        if not daily_latest.empty:
            st.metric(
                "Latest Daily Cases",
                f"{daily_latest['new_cases_total'].iloc[0]:,}",
                f"Quality: {daily_latest['data_quality_score'].iloc[0]:.1%}",
            )

    with col2:
        if not daily_latest.empty:
            st.metric(
                "Latest Daily Deaths",
                f"{daily_latest['new_deaths_total'].iloc[0]:,}",
                "National Total",
            )

    with col3:
        if not state_count.empty:
            st.metric(
                "States Reporting", f"{state_count['count'].iloc[0]}", "Active States"
            )

    with col4:
        if not county_count.empty:
            st.metric(
                "Top Counties Tracked", f"{county_count['count'].iloc[0]}", "Hotspots"
            )

    st.markdown("---")

    # Quick visualizations from each table
    col1, col2 = st.columns(2)

    # with col1:
    #     st.subheader("📈 Last 30 Days - National Trend")
    #     trend_data = query_db(
    #         """
    #         SELECT metric_date, new_cases_total, cases_7day_avg
    #         FROM analytics.daily_national_metrics
    #         WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
    #         ORDER BY metric_date
    #     """
    #     )

    #     if not trend_data.empty:
    #         fig = go.Figure()
    #         fig.add_trace(
    #             go.Scatter(
    #                 x=trend_data["metric_date"],
    #                 y=trend_data["cases_7day_avg"],
    #                 name="7-Day Average",
    #                 line=dict(color="#1f77b4", width=3),
    #                 fill="tozeroy",
    #             )
    #         )
    #         fig.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
    #         st.plotly_chart(fig, use_container_width=True)

    with col1:
        st.subheader("🏅 Top 10 States (Latest)")
        top_states = query_db(
            """
            SELECT state_name, new_cases, cases_rank
            FROM analytics.state_metrics
            WHERE metric_date = (SELECT MAX(metric_date) FROM analytics.state_metrics)
            ORDER BY cases_rank
            LIMIT 10
        """
        )

        if not top_states.empty:
            fig = px.bar(
                top_states,
                x="new_cases",
                y="state_name",
                orientation="h",
                color="new_cases",
                color_continuous_scale="Reds",
            )
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=0, b=0),
                yaxis={"categoryorder": "total ascending"},
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🏅 Top 10 Counties (Latest)")
        top_counties = query_db(
            """
            SELECT county_name, new_cases, cases_7day_avg
            FROM analytics.county_metrics
            WHERE metric_date = (SELECT MAX(metric_date) FROM analytics.county_metrics)
            ORDER BY cases_7day_avg DESC
            LIMIT 10
        """
        )

        if not top_counties.empty:
            fig = px.bar(
                top_counties,
                x="cases_7day_avg",
                y="county_name",
                orientation="h",
                color="cases_7day_avg",
                color_continuous_scale="Blues",
            )
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=0, b=0),
                yaxis={"categoryorder": "total ascending"},
            )
            st.plotly_chart(fig, use_container_width=True)

    # Table summaries
    st.markdown("---")
    st.subheader("📋 Available Data Tables")

    tables_info = [
        {
            "Table": "daily_national_metrics",
            "Description": "Daily national-level aggregates",
            "Latest Date": (
                query_db(
                    "SELECT MAX(metric_date) FROM analytics.daily_national_metrics"
                ).iloc[0, 0]
                if not query_db(
                    "SELECT MAX(metric_date) FROM analytics.daily_national_metrics"
                ).empty
                else "N/A"
            ),
            "Row Count": (
                query_db("SELECT COUNT(*) FROM analytics.daily_national_metrics").iloc[
                    0, 0
                ]
                if not query_db(
                    "SELECT COUNT(*) FROM analytics.daily_national_metrics"
                ).empty
                else 0
            ),
        },
        {
            "Table": "state_metrics",
            "Description": "State-level daily metrics with rankings",
            "Latest Date": (
                query_db("SELECT MAX(metric_date) FROM analytics.state_metrics").iloc[
                    0, 0
                ]
                if not query_db(
                    "SELECT MAX(metric_date) FROM analytics.state_metrics"
                ).empty
                else "N/A"
            ),
            "Row Count": (
                query_db("SELECT COUNT(*) FROM analytics.state_metrics").iloc[0, 0]
                if not query_db("SELECT COUNT(*) FROM analytics.state_metrics").empty
                else 0
            ),
        },
        {
            "Table": "county_metrics",
            "Description": "Top 100 counties per day with trends",
            "Latest Date": (
                query_db("SELECT MAX(metric_date) FROM analytics.county_metrics").iloc[
                    0, 0
                ]
                if not query_db(
                    "SELECT MAX(metric_date) FROM analytics.county_metrics"
                ).empty
                else "N/A"
            ),
            "Row Count": (
                query_db("SELECT COUNT(*) FROM analytics.county_metrics").iloc[0, 0]
                if not query_db("SELECT COUNT(*) FROM analytics.county_metrics").empty
                else 0
            ),
        },
        # {
        #     "Table": "weekly_summary",
        #     "Description": "Weekly executive summaries",
        #     "Latest Date": (
        #         query_db(
        #             "SELECT MAX(week_start_date) FROM analytics.weekly_summary"
        #         ).iloc[0, 0]
        #         if not query_db(
        #             "SELECT MAX(week_start_date) FROM analytics.weekly_summary"
        #         ).empty
        #         else "N/A"
        #     ),
        #     "Row Count": (
        #         query_db("SELECT COUNT(*) FROM analytics.weekly_summary").iloc[0, 0]
        #         if not query_db("SELECT COUNT(*) FROM analytics.weekly_summary").empty
        #         else 0
        #     ),
        # },
    ]

    st.dataframe(pd.DataFrame(tables_info), use_container_width=True, hide_index=True)

# ========================================
# NATIONAL DAILY METRICS
# ========================================

elif view_type == "National Daily Metrics":
    st.header("📊 National Daily Metrics")

    # Query data
    query = f"""
        SELECT 
            metric_date,
            new_cases_total,
            new_deaths_total,
            cumulative_cases,
            cumulative_deaths,
            cases_7day_avg,
            deaths_7day_avg,
            cases_growth_rate,
            avg_case_fatality_rate,
            data_quality_score,
            counties_reporting,
            states_reporting
        FROM analytics.daily_national_metrics
        WHERE metric_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY metric_date
    """
    df = query_db(query)

    if df.empty:
        st.warning("No data available for selected date range")
    else:
        # KPIs
        latest = df.iloc[-1]
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric("Latest Cases", f"{latest['new_cases_total']:,}")
        with col2:
            st.metric("Latest Deaths", f"{latest['new_deaths_total']:,}")
        with col3:
            st.metric("Growth Rate", f"{latest['cases_growth_rate']:.1f}%")
        with col4:
            st.metric("Avg CFR", f"{latest['avg_case_fatality_rate']:.2f}%")
        with col5:
            st.metric("Quality Score", f"{latest['data_quality_score']:.1%}")

        st.markdown("---")

        # Main visualization - Cases
        st.subheader("📈 Daily Cases with 7-Day Average")

        if viz_type in ["Line Chart", "Both"]:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df["metric_date"],
                    y=df["new_cases_total"],
                    name="Daily Cases",
                    mode="lines",
                    line=dict(color="lightblue", width=1),
                    opacity=0.5,
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df["metric_date"],
                    y=df["cases_7day_avg"],
                    name="7-Day Average",
                    mode="lines",
                    line=dict(color="blue", width=3),
                )
            )
            fig.update_layout(height=400, hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)

        if viz_type in ["Bar Chart", "Both"]:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=df["metric_date"],
                    y=df["new_cases_total"],
                    name="Daily Cases",
                    marker_color="skyblue",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df["metric_date"],
                    y=df["cases_7day_avg"],
                    name="7-Day Average",
                    mode="lines",
                    line=dict(color="darkblue", width=3),
                )
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        if viz_type == "Area Chart":
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df["metric_date"],
                    y=df["cases_7day_avg"],
                    name="7-Day Average",
                    fill="tozeroy",
                    line=dict(color="blue"),
                )
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        # Secondary charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("💀 Deaths Trend")
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df["metric_date"],
                    y=df["deaths_7day_avg"],
                    fill="tozeroy",
                    name="7-Day Avg Deaths",
                    line=dict(color="red"),
                )
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("📊 Growth Rate")
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=df["metric_date"],
                    y=df["cases_growth_rate"],
                    name="Growth Rate",
                    marker_color=df["cases_growth_rate"].apply(
                        lambda x: "red" if x > 10 else "orange" if x > 0 else "green"
                    ),
                )
            )
            fig.add_hline(y=0, line_dash="dash", line_color="black")
            fig.update_layout(height=300, yaxis_title="Growth Rate (%)")
            st.plotly_chart(fig, use_container_width=True)

        # Additional metrics
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 Cumulative Cases")
            fig = px.area(
                df,
                x="metric_date",
                y="cumulative_cases",
                color_discrete_sequence=["#2ca02c"],
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🔍 Data Quality & Reporting")
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Scatter(
                    x=df["metric_date"],
                    y=df["data_quality_score"],
                    name="Quality Score",
                    line=dict(color="blue"),
                ),
                secondary_y=False,
            )
            fig.add_trace(
                go.Scatter(
                    x=df["metric_date"],
                    y=df["states_reporting"],
                    name="States Reporting",
                    line=dict(color="green"),
                ),
                secondary_y=True,
            )
            fig.update_yaxes(title_text="Quality Score", secondary_y=False)
            fig.update_yaxes(title_text="States", secondary_y=True)
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        # Data table
        with st.expander("📋 View Raw Data"):
            st.dataframe(df, use_container_width=True)

# ========================================
# STATE METRICS
# ========================================

elif view_type == "State Metrics":
    st.header("🗺️ State-Level Metrics")

    # State selector
    states_list = query_db(
        """
        SELECT DISTINCT state_name 
        FROM analytics.state_metrics 
        ORDER BY state_name
    """
    )["state_name"].tolist()

    selected_states = st.multiselect(
        "Select states to compare (leave empty for top 10)", states_list, default=[]
    )

    # Query
    if selected_states:
        states_filter = "', '".join(selected_states)
        where_clause = f"AND state_name IN ('{states_filter}')"
    else:
        # Get top 10 states by latest cases
        top_states_query = """
            SELECT state_name 
            FROM analytics.state_metrics
            WHERE metric_date = (SELECT MAX(metric_date) FROM analytics.state_metrics)
            ORDER BY cases_rank
            LIMIT 10
        """
        top_states = query_db(top_states_query)["state_name"].tolist()
        states_filter = "', '".join(top_states)
        where_clause = f"AND state_name IN ('{states_filter}')"

    query = f"""
        SELECT 
            metric_date,
            state_name,
            new_cases,
            new_deaths,
            cumulative_cases,
            cumulative_deaths,
            case_fatality_rate,
            cases_7day_avg,
            cases_rank
        FROM analytics.state_metrics
        WHERE metric_date BETWEEN '{start_date}' AND '{end_date}'
            {where_clause}
        ORDER BY metric_date, cases_rank
    """
    df = query_db(query)

    if df.empty:
        st.warning("No data available")
    else:
        # Latest rankings
        st.subheader("🏅 Latest State Rankings")
        latest_date = df["metric_date"].max()
        latest_df = df[df["metric_date"] == latest_date].sort_values("cases_rank")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**Top 10 by Cases**")
            top5 = latest_df.head(10)[["state_name", "new_cases", "cases_rank"]]
            st.dataframe(top5, hide_index=True, use_container_width=True)

        with col2:
            st.markdown("**Cases by State (Latest)**")
            fig = px.bar(
                latest_df.head(10),
                x="state_name",
                y="new_cases",
                color="new_cases",
                color_continuous_scale="Reds",
            )
            fig.update_layout(height=450, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            st.markdown("**CFR Distribution**")
            fig = px.box(latest_df, y="case_fatality_rate", points="all")
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Trend comparison
        st.subheader("📈 State Comparison Over Time")

        if viz_type in ["Line Chart", "Both"]:
            fig = px.line(
                df,
                x="metric_date",
                y="cases_7day_avg",
                color="state_name",
                title="7-Day Average Cases by State",
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        if viz_type in ["Bar Chart", "Both"]:
            # Stacked bar for selected states
            pivot_df = df.pivot(
                index="metric_date", columns="state_name", values="new_cases"
            ).fillna(0)

            fig = go.Figure()
            for state in pivot_df.columns:
                fig.add_trace(go.Bar(x=pivot_df.index, y=pivot_df[state], name=state))
            fig.update_layout(
                barmode="stack", height=400, title="Daily Cases by State (Stacked)"
            )
            st.plotly_chart(fig, use_container_width=True)

        if viz_type == "Area Chart":
            # Stacked area
            pivot_df = df.pivot(
                index="metric_date", columns="state_name", values="cases_7day_avg"
            ).fillna(0)

            fig = go.Figure()
            for state in pivot_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=pivot_df.index,
                        y=pivot_df[state],
                        name=state,
                        stackgroup="one",
                    )
                )
            fig.update_layout(height=400, title="7-Day Avg (Stacked Area)")
            st.plotly_chart(fig, use_container_width=True)

        # Heatmap
        st.subheader("🔥 Cases Heatmap")
        pivot_for_heatmap = df.pivot(
            index="state_name", columns="metric_date", values="new_cases"
        )

        fig = go.Figure(
            data=go.Heatmap(
                z=pivot_for_heatmap.values,
                x=pivot_for_heatmap.columns,
                y=pivot_for_heatmap.index,
                colorscale="Reds",
            )
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        # Data table
        with st.expander("📋 View Raw Data"):
            st.dataframe(df, use_container_width=True)

# ========================================
# COUNTY HOTSPOTS
# ========================================

elif view_type == "County Hotspots":
    st.header("🚨 County Hotspots Analysis")

    # Filters
    col1, col2 = st.columns(2)

    with col1:
        trend_filter = st.selectbox(
            "Filter by Trend", ["All", "Increasing", "Decreasing", "Stable"]
        )

    with col2:
        top_n = st.slider("Number of counties to show", 10, 100, 20, 10)

    # Query
    trend_condition = ""
    if trend_filter != "All":
        trend_condition = f"AND trend_direction = '{trend_filter.lower()}'"

    query = f"""
        SELECT 
            county_name,
            state_name,
            new_cases,
            cumulative_cases,
            cumulative_deaths,
            case_fatality_rate,
            cases_7day_avg,
            trend_direction
        FROM analytics.county_metrics
        WHERE metric_date = (SELECT MAX(metric_date) FROM analytics.county_metrics)
            {trend_condition}
        ORDER BY new_cases DESC
        LIMIT {top_n}
    """
    df = query_db(query)

    if df.empty:
        st.warning("No data available")
    else:
        # Add trend emoji
        df["Trend"] = df["trend_direction"].map(
            {"increasing": "⬆️", "decreasing": "⬇️", "stable": "➡️"}
        )

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Counties", len(df))
        with col2:
            increasing = (df["trend_direction"] == "increasing").sum()
            st.metric("Increasing", increasing, delta_color="inverse")
        with col3:
            decreasing = (df["trend_direction"] == "decreasing").sum()
            st.metric("Decreasing", decreasing, delta_color="normal")
        with col4:
            avg_cfr = df["case_fatality_rate"].mean()
            st.metric("Avg CFR", f"{avg_cfr:.2f}%")

        st.markdown("---")

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📊 Top Counties by Cases")
            fig = px.bar(
                df.head(15),
                y="county_name",
                x="new_cases",
                color="trend_direction",
                orientation="h",
                color_discrete_map={
                    "increasing": "#d62728",
                    "decreasing": "#2ca02c",
                    "stable": "#ff7f0e",
                },
                hover_data=["state_name", "cases_7day_avg"],
            )
            fig.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🗺️ Geographic Distribution")
            # Pie chart by state
            state_dist = df.groupby("state_name").size().reset_index(name="count")
            fig = px.pie(
                state_dist.head(10),
                values="count",
                names="state_name",
                title="Top 10 States with Most Hotspot Counties",
            )
            fig.update_layout(height=250)
            st.plotly_chart(fig, use_container_width=True)

            # Trend distribution
            st.markdown("**Trend Distribution**")
            trend_dist = df["trend_direction"].value_counts()
            fig = px.pie(
                values=trend_dist.values,
                names=trend_dist.index,
                color=trend_dist.index,
                color_discrete_map={
                    "increasing": "#d62728",
                    "decreasing": "#2ca02c",
                    "stable": "#ff7f0e",
                },
            )
            fig.update_layout(height=250)
            st.plotly_chart(fig, use_container_width=True)

        # Scatter plot
        st.subheader("📉 Cases vs CFR")
        fig = px.scatter(
            df,
            x="new_cases",
            y="case_fatality_rate",
            size="cumulative_cases",
            color="trend_direction",
            hover_data=["county_name", "state_name"],
            color_discrete_map={
                "increasing": "#d62728",
                "decreasing": "#2ca02c",
                "stable": "#ff7f0e",
            },
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        # Detailed table
        st.subheader("📋 Detailed County Data")
        display_df = df[
            [
                "county_name",
                "state_name",
                "new_cases",
                "cases_7day_avg",
                "Trend",
                "case_fatality_rate",
                "cumulative_cases",
                "cumulative_deaths",
            ]
        ].copy()

        display_df.columns = [
            "County",
            "State",
            "New Cases",
            "7-Day Avg",
            "Trend",
            "CFR %",
            "Total Cases",
            "Total Deaths",
        ]

        st.dataframe(display_df, hide_index=True, use_container_width=True, height=400)

# ========================================
# WEEKLY SUMMARY
# ========================================

# elif view_type == "Weekly Summary":
#     st.header("📅 Weekly Executive Summary")

#     # Number of weeks to show
#     weeks_to_show = st.slider("Number of weeks", 4, 12, 8)

#     query = f"""
#         SELECT 
#             week_start_date,
#             week_end_date,
#             total_new_cases,
#             total_new_deaths,
#             cases_wow_change,
#             deaths_wow_change,
#             peak_cases_date,
#             peak_cases_count,
#             total_states_affected,
#             total_counties_affected,
#             avg_cfr
#         FROM analytics.weekly_summary
#         ORDER BY week_start_date DESC
#         LIMIT {weeks_to_show}
#     """
#     df = query_db(query)

#     if df.empty:
#         st.warning("No weekly summary data available")
#     else:
#         df = df.sort_values("week_start_date")

#         # Latest week KPIs
#         latest = df.iloc[-1]

#         col1, col2, col3, col4 = st.columns(4)

#         with col1:
#             st.metric(
#                 "Latest Week Cases",
#                 f"{latest['total_new_cases']:,}",
#                 f"{latest['cases_wow_change']:.1f}% WoW",
#                 delta_color="inverse",
#             )

#         with col2:
#             st.metric(
#                 "Latest Week Deaths",
#                 f"{latest['total_new_deaths']:,}",
#                 f"{latest['deaths_wow_change']:.1f}% WoW",
#                 delta_color="inverse",
#             )

#         with col3:
#             st.metric(
#                 "Peak Day",
#                 (
#                     latest["peak_cases_date"].strftime("%b %d")
#                     if pd.notna(latest["peak_cases_date"])
#                     else "N/A"
#                 ),
#                 (
#                     f"{latest['peak_cases_count']:,} cases"
#                     if pd.notna(latest["peak_cases_count"])
#                     else ""
#                 ),
#             )

#         with col4:
#             st.metric(
#                 "Avg CFR",
#                 f"{latest['avg_cfr']:.2f}%",
#                 f"{latest['total_states_affected']} states",
#             )

#         st.markdown("---")

#         # Weekly trends
#         col1, col2 = st.columns(2)

#         with col1:
#             st.subheader("📊 Weekly Cases Trend")
#             fig = go.Figure()

#             # Bar chart
#             fig.add_trace(
#                 go.Bar(
#                     x=df["week_start_date"],
#                     y=df["total_new_cases"],
#                     name="Weekly Cases",
#                     marker_color="#1f77b4",
#                     text=df["total_new_cases"],
#                     texttemplate="%{text:,.0f}",
#                     textposition="outside",
#                 )
#             )

#             fig.update_layout(height=400)
#             st.plotly_chart(fig, use_container_width=True)

#         with col2:
#             st.subheader("💀 Weekly Deaths Trend")
#             fig = go.Figure()

#             fig.add_trace(
#                 go.Bar(
#                     x=df["week_start_date"],
#                     y=df["total_new_deaths"],
#                     name="Weekly Deaths",
#                     marker_color="#d62728",
#                     text=df["total_new_deaths"],
#                     texttemplate="%{text:,.0f}",
#                     textposition="outside",
#                 )
#             )

#             fig.update_layout(height=400)
#             st.plotly_chart(fig, use_container_width=True)

#         # Week-over-week changes
#         st.subheader("📈 Week-over-Week Change (%)")

#         fig = go.Figure()

#         fig.add_trace(
#             go.Scatter(
#                 x=df["week_start_date"],
#                 y=df["cases_wow_change"],
#                 name="Cases WoW Change",
#                 mode="lines+markers",
#                 line=dict(color="blue", width=3),
#             )
#         )

#         fig.add_trace(
#             go.Scatter(
#                 x=df["week_start_date"],
#                 y=df["deaths_wow_change"],
#                 name="Deaths WoW Change",
#                 mode="lines+markers",
#                 line=dict(color="red", width=3),
#             )
#         )

#         fig.add_hline(y=0, line_dash="dash", line_color="black")
#         fig.update_layout(height=400, yaxis_title="% Change")
#         st.plotly_chart(fig, use_container_width=True)

#         # Coverage metrics
#         col1, col2 = st.columns(2)

#         with col1:
#             st.subheader("🗺️ States Affected")
#             fig = px.line(
#                 df, x="week_start_date", y="total_states_affected", markers=True
#             )
#             fig.update_layout(height=300)
#             st.plotly_chart(fig, use_container_width=True)

#         with col2:
#             st.subheader("📍 Counties Affected")
#             fig = px.area(
#                 df,
#                 x="week_start_date",
#                 y="total_counties_affected",
#                 color_discrete_sequence=["#2ca02c"],
#             )
#             fig.update_layout(height=300)
#             st.plotly_chart(fig, use_container_width=True)

#         # Data table
#         with st.expander("📋 View Weekly Summary Data"):
#             display_df = df.copy()
#             display_df["week_start_date"] = display_df["week_start_date"].dt.strftime(
#                 "%Y-%m-%d"
#             )
#             display_df["week_end_date"] = display_df["week_end_date"].dt.strftime(
#                 "%Y-%m-%d"
#             )
#             st.dataframe(display_df, use_container_width=True)

# ========================================
# FOOTER
# ========================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 0.9em;'>
        <p>📊 COVID-19 Analytics Dashboard | Built with Streamlit & PostgreSQL</p>
        <p>Data from: <code>analytics.daily_national_metrics</code>, 
        <code>analytics.state_metrics</code>, 
        <code>analytics.county_metrics</code></p>
    </div>
""",
    unsafe_allow_html=True,
)
