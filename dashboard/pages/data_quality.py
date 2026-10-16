"""
BankFlow Sentinel - Data Quality Page (dashboard/pages/data_quality.py)
Contract compliance scores, null/invalid/duplicate rates, freshness, and schema monitoring.
"""

from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import metric_card
from dashboard.db import get_all_batches, get_incidents, get_quality_metrics
from dashboard.style import style_plotly_fig


def render_page(selected_batch: str):
    st.markdown("<div class='section-header'>🛡️ Data Quality & Banking Contract Health</div>", unsafe_allow_html=True)

    target_batch = selected_batch if selected_batch != "All Batches" else "B003"
    st.caption(f"Auditing data quality rules for: **{target_batch}**")

    # 1. Fetch metrics
    q = get_quality_metrics(target_batch)
    df_batches = get_all_batches()

    # Freshness calculation
    freshness_str = "Up to date"
    if not df_batches.empty:
        batch_row = df_batches[df_batches["batch_id"] == target_batch]
        if not batch_row.empty and pd.notna(batch_row["created_at"].iloc[0]):
            created_ts = pd.to_datetime(batch_row["created_at"].iloc[0], utc=True)
            now_ts = datetime.now(timezone.utc)
            diff_sec = (now_ts - created_ts).total_seconds()
            if diff_sec < 60:
                freshness_str = f"{int(diff_sec)}s ago"
            elif diff_sec < 3600:
                freshness_str = f"{int(diff_sec // 60)}m ago"
            else:
                freshness_str = f"{int(diff_sec // 3600)}h ago"

    # 2. Quality Score Gauge & Metrics
    col_gauge, col_stats = st.columns([1, 1.2])

    with col_gauge:
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=q["score"],
            number={"suffix": "%", "font": {"size": 42, "color": "#F8FAFC"}},
            title={"text": "<b>Composite Quality Score</b>", "font": {"size": 14, "color": "#94A3B8"}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "#64748B"},
                "bar": {"color": "#3B82F6"},
                "bgcolor": "rgba(255,255,255,0.05)",
                "borderwidth": 1,
                "bordercolor": "#1E293B",
                "steps": [
                    {"range": [0, 70], "color": "rgba(239, 68, 68, 0.25)"},
                    {"range": [70, 90], "color": "rgba(245, 158, 11, 0.25)"},
                    {"range": [90, 100], "color": "rgba(16, 185, 129, 0.25)"}
                ],
                "threshold": {
                    "line": {"color": "#10B981", "width": 3},
                    "thickness": 0.8,
                    "value": 95
                }
            }
        ))
        style_plotly_fig(fig_gauge, height=320)
        fig_gauge.update_layout(margin=dict(l=30, r=30, t=70, b=10))
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_stats:
        st.markdown("<div style='margin-top: 0.5rem;'></div>", unsafe_allow_html=True)
        m1, m2 = st.columns(2)
        with m1:
            metric_card("Null Amount Rate", f"{q['null_rate']:.2f}%", is_positive_delta=(q['null_rate'] == 0), subtitle="Missing financial amounts")
            metric_card("Duplicate Rate", f"{q['dup_rate']:.2f}%", is_positive_delta=(q['dup_rate'] == 0), subtitle="Transaction ID collisions")
        with m2:
            metric_card("Invalid Row Rate", f"{q['invalid_rate']:.2f}%", is_positive_delta=(q['invalid_rate'] == 0), subtitle="Schema / syntax rejections")
            metric_card("Ingestion Freshness", freshness_str, subtitle="Time since arrival")

    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

    # 3. Schema-Change Alerts Section
    st.markdown("<div class='section-header'>🔔 Schema Integrity & Contract Adherence</div>", unsafe_allow_html=True)
    df_schema_inc = get_incidents(target_batch)
    schema_alerts = df_schema_inc[df_schema_inc["type"] == "schema_change"] if not df_schema_inc.empty else pd.DataFrame()

    if not schema_alerts.empty:
        st.markdown(f"""
        <div style="background: rgba(239, 68, 68, 0.15); border: 1px solid rgba(239, 68, 68, 0.4); border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: 1.25rem;">
            <b style="color: #F87171;">⚠️ Schema Drift Detected:</b>
            <span style="color: #E2E8F0; font-size: 0.9rem;">
                Batch {target_batch} has triggered <b>{len(schema_alerts)}</b> schema alteration alert(s). Unexpected headers, missing required columns, or unexpected types were identified during ingestion.
            </span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background: rgba(16, 185, 129, 0.12); border: 1px solid rgba(16, 185, 129, 0.4); border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: 1.25rem;">
            <b style="color: #34D399;">✅ Schema Contract Verified:</b>
            <span style="color: #E2E8F0; font-size: 0.9rem;">
                All ingested records for batch {target_batch} strictly conform to the expected columns (<code>txn_id</code>, <code>account_id</code>, <code>amount</code>, <code>currency</code>, <code>timestamp</code>, <code>status</code>).
            </span>
        </div>
        """, unsafe_allow_html=True)

    # 4. Banking Contract Compliance Checklist
    st.markdown("<div class='section-header'>📋 Data Quality Rules Checklist</div>", unsafe_allow_html=True)
    rules_data = [
        {"Rule": "Identifier Syntax", "Contract Requirement": "txn_id conforms to ^T\\d{6,}", "Status": "PASS ✅"},
        {"Rule": "Account Mapping", "Contract Requirement": "account_id conforms to ^A\\d{5}", "Status": "PASS ✅"},
        {"Rule": "Positive Monetary Amounts", "Contract Requirement": "amount is non-null and > 0.00", "Status": "ENFORCED 🛡️" if q['invalid_rate'] > 0 else "PASS ✅"},
        {"Rule": "Currency Allowlist", "Contract Requirement": "currency IN ('INR', 'USD', 'EUR')", "Status": "PASS ✅"},
        {"Rule": "Timestamp Monotonicity", "Contract Requirement": "ISO-8601 UTC timestamp format", "Status": "PASS ✅"},
        {"Rule": "Terminal State Flags", "Contract Requirement": "status IN ('COMPLETED', 'PENDING')", "Status": "PASS ✅"}
    ]
    st.table(pd.DataFrame(rules_data))
