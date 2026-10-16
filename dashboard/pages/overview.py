"""
BankFlow Sentinel - Overview Page (dashboard/pages/overview.py)
KPI summary, pipeline stage funnel, incident breakdown, and recent alerts.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import empty_state, metric_card, severity_badge, status_chip
from dashboard.db import get_funnel_counts, get_incidents, get_overview_kpis
from dashboard.style import (
    COLOR_CARD_BG,
    COLOR_CARD_BORDER,
    style_plotly_fig,
)


def render_page(selected_batch: str):
    st.markdown("<div class='section-header'>📊 Operations Executive Overview</div>", unsafe_allow_html=True)

    # 1. Top KPI Metric Cards
    kpis = get_overview_kpis(selected_batch)
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        metric_card("Total Batches", f"{kpis['total_batches']:,}", subtitle="Ingested batches")
    with col2:
        metric_card("Txns Processed", f"{kpis['total_transactions']:,}", subtitle="Across pipeline stages")
    with col3:
        metric_card("Unmatched Items", f"{kpis['unmatched_records']:,}",
                    delta=f"{kpis['unmatched_records']:,}", is_positive_delta=False,
                    subtitle="Requires reconciliation")
    with col4:
        metric_card("Active Incidents", f"{kpis['active_incidents']}",
                    subtitle="Open or investigating")
    with col5:
        metric_card("Reconciled Health", f"{kpis['health_pct']:.1f}%",
                    subtitle="Target: >99.0%", is_positive_delta=(kpis['health_pct'] >= 95.0))

    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

    # 2. Charts Row: Funnel Chart & Incident Donut
    c1, c2 = st.columns([1.1, 0.9])

    with c1:
        st.markdown("<div class='section-header'>🔻 Pipeline Stage Funnel (Selected Batch)</div>", unsafe_allow_html=True)
        # Fetch funnel counts
        funnel_batch = selected_batch if selected_batch != "All Batches" else "B003"
        f_counts = get_funnel_counts(funnel_batch)

        stages = ["Source", "Payment", "Settlement"]
        values = [f_counts["source"], f_counts["payment"], f_counts["settlement"]]

        fig_funnel = go.Figure(go.Funnel(
            y=stages,
            x=values,
            textinfo="value+percent previous",
            texttemplate="<b>%{value:,}</b><br>(%{percentPrevious:.1%})",
            marker=dict(
                color=["#3B82F6", "#60A5FA", "#93C5FD"],
                line=dict(width=1, color="#1E293B")
            ),
            connector=dict(line=dict(color="#334155", width=1))
        ))
        style_plotly_fig(fig_funnel, height=340, title=f"Transaction Flow Dropoff ({funnel_batch})")
        st.plotly_chart(fig_funnel, use_container_width=True)

    with c2:
        st.markdown("<div class='section-header'>🍩 Incidents by Discrepancy Type</div>", unsafe_allow_html=True)
        df_inc = get_incidents(selected_batch)
        if not df_inc.empty:
            type_counts = df_inc["type"].value_counts().reset_index()
            type_counts.columns = ["type", "count"]

            palette = ["#EF4444", "#F59E0B", "#3B82F6", "#8B5CF6", "#10B981", "#EC4899"]
            fig_donut = go.Figure(data=[go.Pie(
                labels=type_counts["type"],
                values=type_counts["count"],
                hole=0.58,
                marker=dict(colors=palette, line=dict(color="#111A2E", width=2)),
                textinfo="label+percent",
                showlegend=False
            )])
            style_plotly_fig(fig_donut, height=340, title="Fault Mix Proportion")
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            empty_state("No Incidents Detected", "All transactions in this scope are fully reconciled.", icon="✅")

    # 3. Latest Incidents Table
    st.markdown("<div class='section-header'>🚨 Recent Operational Incidents</div>", unsafe_allow_html=True)
    if not df_inc.empty:
        display_inc = df_inc.head(8).copy()
        
        # Format HTML columns
        display_inc["Severity"] = display_inc["severity"].apply(severity_badge)
        display_inc["Status"] = display_inc["status"].apply(status_chip)
        display_inc["Detected (UTC)"] = pd.to_datetime(display_inc["detected_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        table_rows = []
        for _, r in display_inc.iterrows():
            row_html = (
                f"<tr>"
                f"<td style='padding: 8px 12px; font-weight: 600; color: #F8FAFC;'>{r['id']}</td>"
                f"<td style='padding: 8px 12px; color: #94A3B8;'>{r['batch_id']}</td>"
                f"<td style='padding: 8px 12px; text-transform: capitalize;'>{r['stage']}</td>"
                f"<td style='padding: 8px 12px; font-family: monospace; color: #E2E8F0;'>{r['type']}</td>"
                f"<td style='padding: 8px 12px;'>{r['Severity']}</td>"
                f"<td style='padding: 8px 12px;'>{r['Status']}</td>"
                f"<td style='padding: 8px 12px; color: #94A3B8; font-size: 0.8rem;'>{r['Detected (UTC)']}</td>"
                f"</tr>"
            )
            table_rows.append(row_html)

        table_body = "".join(table_rows)
        table_html = (
            f"<div style='background-color: {COLOR_CARD_BG}; border: 1px solid {COLOR_CARD_BORDER}; border-radius: 8px; overflow-x: auto;'>"
            f"<table style='width: 100%; border-collapse: collapse; text-align: left; font-size: 0.85rem;'>"
            f"<thead>"
            f"<tr style='border-bottom: 1px solid {COLOR_CARD_BORDER}; background: rgba(255,255,255,0.02); color: #64748B;'>"
            f"<th style='padding: 10px 12px;'>INCIDENT ID</th>"
            f"<th style='padding: 10px 12px;'>BATCH</th>"
            f"<th style='padding: 10px 12px;'>STAGE</th>"
            f"<th style='padding: 10px 12px;'>TYPE</th>"
            f"<th style='padding: 10px 12px;'>SEVERITY</th>"
            f"<th style='padding: 10px 12px;'>STATUS</th>"
            f"<th style='padding: 10px 12px;'>DETECTED AT</th>"
            f"</tr>"
            f"</thead>"
            f"<tbody>{table_body}</tbody>"
            f"</table>"
            f"</div>"
        )
        st.html(table_html)
    else:
        empty_state("No Incidents Found", "No open or logged incidents for the selected batch filter.", icon="🎉")
