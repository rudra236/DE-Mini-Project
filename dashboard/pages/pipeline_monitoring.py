"""
BankFlow Sentinel - Pipeline Monitoring Page (dashboard/pages/pipeline_monitoring.py)
Orchestration task executions from pipeline_runs, timeline/Gantt view, and retry telemetry.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.components import empty_state, metric_card, status_chip
from dashboard.db import get_pipeline_runs
from dashboard.style import COLOR_CARD_BG, COLOR_CARD_BORDER, style_plotly_fig


def render_page(selected_batch: str):
    st.markdown("<div class='section-header'>⚡ Pipeline Monitoring & Task Telemetry</div>", unsafe_allow_html=True)

    df_runs = get_pipeline_runs(selected_batch)

    if df_runs.empty:
        empty_state("No Pipeline Runs Recorded", f"No task executions logged in pipeline_runs for {selected_batch}.", icon="ℹ️")
        return

    # 1. Summary Metrics
    total_runs = len(df_runs)
    success_runs = len(df_runs[df_runs["status"].str.lower().isin(["success", "completed", "reconciled", "ingested", "validated"])])
    success_rate = round((success_runs / total_runs) * 100, 1) if total_runs > 0 else 100.0
    retried_runs = df_runs[df_runs["retries"] > 0]
    avg_duration = round(df_runs["duration_seconds"].mean(), 2) if "duration_seconds" in df_runs.columns else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        metric_card("Total Tasks Executed", f"{total_runs:,}", subtitle="Orchestration actions")
    with col2:
        metric_card("Success Rate", f"{success_rate:.1f}%", subtitle="Completed without failure", is_positive_delta=(success_rate >= 90.0))
    with col3:
        metric_card("Average Duration", f"{avg_duration:.2f}s", subtitle="Task execution time")
    with col4:
        metric_card("Retried Tasks", f"{len(retried_runs):,}",
                    delta=f"{len(retried_runs):,}" if len(retried_runs) > 0 else None,
                    is_positive_delta=False,
                    subtitle="Resiliency retries")

    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

    # Retry Highlight Alert
    if len(retried_runs) > 0:
        st.markdown(f"""
        <div style="background: rgba(245, 158, 11, 0.12); border: 1px solid rgba(245, 158, 11, 0.4); border-radius: 8px; padding: 0.85rem 1.25rem; margin-bottom: 1.25rem;">
            <b style="color: #FCD34D;">⚠️ Resiliency Alert:</b>
            <span style="color: #E2E8F0; font-size: 0.9rem;">
                <b>{len(retried_runs)}</b> task(s) encountered initial transient failures and successfully recovered via Prefect automated retries.
            </span>
        </div>
        """, unsafe_allow_html=True)

    # 2. Timeline / Duration Gantt Chart
    st.markdown("<div class='section-header'>⏱️ Task Duration & Execution Timeline</div>", unsafe_allow_html=True)
    if "started_at" in df_runs.columns and "finished_at" in df_runs.columns:
        valid_times = df_runs[df_runs["started_at"].notna() & df_runs["finished_at"].notna()].copy()
        if not valid_times.empty:
            valid_times["Task Display"] = valid_times["task_name"] + " (" + valid_times["batch_id"] + ")"
            fig_gantt = px.bar(
                valid_times,
                x="duration_seconds",
                y="Task Display",
                orientation="h",
                color="status",
                color_discrete_map={
                    "success": "#10B981", "completed": "#10B981", "reconciled": "#10B981",
                    "ingested": "#3B82F6", "validated": "#3B82F6",
                    "failed": "#EF4444", "running": "#F59E0B"
                },
                labels={"duration_seconds": "Duration (Seconds)", "Task Display": "Task"}
            )
            bar_height = max(160, min(500, len(valid_times) * 32 + 70))
            style_plotly_fig(fig_gantt, height=bar_height, title="Task Wall Execution Times")
            st.plotly_chart(fig_gantt, use_container_width=True)

    # 3. Pipeline Runs Table
    st.markdown("<div class='section-header'>📋 Execution Run Log</div>", unsafe_allow_html=True)
    display_df = df_runs.copy()
    display_df["Status Badge"] = display_df["status"].apply(status_chip)
    display_df["Started (UTC)"] = pd.to_datetime(display_df["started_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    table_rows = []
    for _, r in display_df.iterrows():
        retry_badge = f"<b style='color: #F59E0B;'>{r['retries']}</b>" if r['retries'] > 0 else "<span style='color: #64748B;'>0</span>"
        row_html = (
            f"<tr>"
            f"<td style='padding: 8px 12px; font-weight: 600; color: #F8FAFC;'>{r['task_name']}</td>"
            f"<td style='padding: 8px 12px; color: #94A3B8;'>{r['batch_id']}</td>"
            f"<td style='padding: 8px 12px;'>{r['Status Badge']}</td>"
            f"<td style='padding: 8px 12px; font-family: monospace; color: #E2E8F0;'>{r['duration_seconds']}s</td>"
            f"<td style='padding: 8px 12px; text-align: center;'>{retry_badge}</td>"
            f"<td style='padding: 8px 12px; color: #94A3B8; font-size: 0.8rem;'>{r['Started (UTC)']}</td>"
            f"<td style='padding: 8px 12px; font-family: monospace; color: #64748B; font-size: 0.75rem;'>{r['run_id'][:12]}...</td>"
            f"</tr>"
        )
        table_rows.append(row_html)

    table_body = "".join(table_rows)
    table_html = (
        f"<div style='background-color: {COLOR_CARD_BG}; border: 1px solid {COLOR_CARD_BORDER}; border-radius: 8px; overflow-x: auto;'>"
        f"<table style='width: 100%; border-collapse: collapse; text-align: left; font-size: 0.85rem;'>"
        f"<thead>"
        f"<tr style='border-bottom: 1px solid {COLOR_CARD_BORDER}; background: rgba(255,255,255,0.02); color: #64748B;'>"
        f"<th style='padding: 10px 12px;'>TASK NAME</th>"
        f"<th style='padding: 10px 12px;'>BATCH</th>"
        f"<th style='padding: 10px 12px;'>STATUS</th>"
        f"<th style='padding: 10px 12px;'>DURATION</th>"
        f"<th style='padding: 10px 12px; text-align: center;'>RETRIES</th>"
        f"<th style='padding: 10px 12px;'>STARTED AT</th>"
        f"<th style='padding: 10px 12px;'>RUN ID</th>"
        f"</tr>"
        f"</thead>"
        f"<tbody>{table_body}</tbody>"
        f"</table>"
        f"</div>"
    )
    st.html(table_html)
