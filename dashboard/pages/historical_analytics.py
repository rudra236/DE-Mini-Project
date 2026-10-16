"""
BankFlow Sentinel - Historical Analytics Page (dashboard/pages/historical_analytics.py)
Multi-batch trendlines, stacked breakdowns, fault heatmaps, LAG metrics, and OLAP cubes.
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import empty_state
from dashboard.db import get_batch_summary_view, get_historical_trends, get_olap_rollup
from dashboard.style import style_plotly_fig


def render_page(selected_batch: str):
    st.markdown("<div class='section-header'>📈 Long-Term Analytical Trends & OLAP Intelligence</div>", unsafe_allow_html=True)

    trends = get_historical_trends()
    df_daily = trends["daily"]
    df_batch_type = trends["batch_type"]

    # 1. Row 1: Daily Discrepancies Line Chart & Stacked Bar by Type
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("<div class='section-header'>📅 Daily Discrepancy Timeline</div>", unsafe_allow_html=True)
        if not df_daily.empty:
            fig_daily = go.Figure()
            fig_daily.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["total_missing"], name="Missing", mode="lines+markers", line=dict(color="#EF4444", width=2)))
            fig_daily.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["total_mismatches"], name="Mismatches", mode="lines+markers", line=dict(color="#F59E0B", width=2)))
            fig_daily.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["total_duplicates"], name="Duplicates", mode="lines+markers", line=dict(color="#3B82F6", width=2)))
            fig_daily.add_trace(go.Scatter(x=df_daily["day"], y=df_daily["total_delayed"], name="Delayed", mode="lines+markers", line=dict(color="#8B5CF6", width=2)))
            style_plotly_fig(fig_daily, height=320, title="Fault Frequency over Days")
            st.plotly_chart(fig_daily, use_container_width=True)
        else:
            empty_state("No Daily Trend Data", "Historical daily counts will appear as batches accumulate.", icon="📊")

    with c2:
        st.markdown("<div class='section-header'>📊 Incident Breakdown by Batch</div>", unsafe_allow_html=True)
        if not df_batch_type.empty:
            fig_bar = px.bar(
                df_batch_type,
                x="batch_id",
                y="count",
                color="type",
                color_discrete_sequence=["#EF4444", "#F59E0B", "#3B82F6", "#8B5CF6", "#10B981", "#EC4899"],
                labels={"batch_id": "Batch", "count": "Incident Count", "type": "Type"}
            )
            style_plotly_fig(fig_bar, height=320, title="Stacked Incident Types")
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            empty_state("No Batch Breakdown", "No batch incident distributions available.", icon="ℹ️")

    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

    # 2. Row 2: Repeated Failure Heatmap (Batch x Type)
    st.markdown("<div class='section-header'>🔥 Repeated Fault Heatmap (Batch × Incident Type)</div>", unsafe_allow_html=True)
    if not df_batch_type.empty:
        pivot_df = df_batch_type.pivot_table(index="batch_id", columns="type", values="count", fill_value=0)
        fig_heat = px.imshow(
            pivot_df,
            labels=dict(x="Discrepancy Type", y="Batch ID", color="Count"),
            x=pivot_df.columns,
            y=pivot_df.index,
            color_continuous_scale="Viridis",
            text_auto=True,
            aspect="auto"
        )
        style_plotly_fig(fig_heat, height=300, title="Cross-Batch Fault Density")
        st.plotly_chart(fig_heat, use_container_width=True)

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # 3. Row 3: LAG Batch-over-Batch Comparison (v_batch_summary)
    st.markdown("<div class='section-header'>🔄 Batch-over-Batch Mismatch Shifts (SQL Window & LAG)</div>", unsafe_allow_html=True)
    df_summary = get_batch_summary_view()
    if not df_summary.empty:
        st.caption("Derived from database analytical view `v_batch_summary` using `LAG()` and `DENSE_RANK()`:")
        st.dataframe(df_summary, use_container_width=True)
    else:
        empty_state("Summary View Not Found", "Run db/queries_sql_features.sql to create v_batch_summary.", icon="ℹ️")

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # 4. Row 4: OLAP CUBE / ROLLUP Grouping Matrix
    st.markdown("<div class='section-header'>🧊 OLAP Multidimensional Rollup (Stage × Severity Cube)</div>", unsafe_allow_html=True)
    df_olap = get_olap_rollup()
    if not df_olap.empty:
        st.caption("Aggregated using PostgreSQL `GROUP BY CUBE(stage, severity)` with `GROUPING()` labeling:")
        st.dataframe(df_olap, use_container_width=True)
    else:
        empty_state("OLAP Aggregation", "No incident rows available to compute multidimensional cube.", icon="ℹ️")
