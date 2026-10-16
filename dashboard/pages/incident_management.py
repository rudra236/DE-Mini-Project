"""
BankFlow Sentinel - Incident Management Page (dashboard/pages/incident_management.py)
Triage portal for operational incidents, evidence inspection, and status state transitions.
"""

import json

import pandas as pd
import streamlit as st

from dashboard.components import empty_state, metric_card, severity_badge, status_chip
from dashboard.db import get_incidents, update_incident


def render_page(selected_batch: str):
    st.markdown("<div class='section-header'>🚨 Operational Incident Triage & Resolution</div>", unsafe_allow_html=True)

    # 1. Filters Row
    col_st, col_sev, col_type, col_stage = st.columns(4)
    with col_st:
        status_sel = st.selectbox("Status", ["All", "open", "investigating", "resolved"])
    with col_sev:
        sev_sel = st.selectbox("Severity", ["All", "high", "medium", "low"])
    with col_type:
        type_sel = st.selectbox("Discrepancy Type", [
            "All", "missing_record", "amount_mismatch", "duplicate", "delayed", "invalid_record", "schema_change"
        ])
    with col_stage:
        stage_sel = st.selectbox("Stage", ["All", "payment", "settlement"])

    df_inc = get_incidents(selected_batch, status_filter=status_sel, severity_filter=sev_sel)

    # Apply client-side filters for type and stage if selected
    if not df_inc.empty and type_sel != "All":
        df_inc = df_inc[df_inc["type"] == type_sel]
    if not df_inc.empty and stage_sel != "All":
        df_inc = df_inc[df_inc["stage"] == stage_sel]

    # 2. MTTR & KPI Summary
    all_inc = get_incidents(selected_batch)
    open_count = len(all_inc[all_inc["status"] == "open"])
    inv_count = len(all_inc[all_inc["status"] == "investigating"])
    res_count = len(all_inc[all_inc["status"] == "resolved"])

    # Calculate MTTR
    mttr_str = "N/A"
    resolved_with_time = all_inc[all_inc["resolved_at"].notna() & all_inc["detected_at"].notna()]
    if not resolved_with_time.empty:
        durations = (pd.to_datetime(resolved_with_time["resolved_at"]) - pd.to_datetime(resolved_with_time["detected_at"])).dt.total_seconds() / 60.0
        avg_min = durations[durations >= 0].mean()
        if pd.notna(avg_min):
            mttr_str = f"{avg_min:.1f} min"

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        metric_card("Open Incidents", f"{open_count}", is_positive_delta=(open_count == 0), subtitle="Awaiting triage")
    with k2:
        metric_card("Under Investigation", f"{inv_count}", subtitle="Ops team assigned")
    with k3:
        metric_card("Resolved Incidents", f"{res_count}", is_positive_delta=True, subtitle="Closed post-reconciliation")
    with k4:
        metric_card("MTTR (Mean Triage)", mttr_str, subtitle="Detection to resolution")

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # 3. Incidents Triage List
    if df_inc.empty:
        empty_state("No Incidents Found", "No incidents match your current filter parameters.", icon="🎉")
        return

    st.markdown(f"<div class='section-header'>Incident Registry ({len(df_inc)} Items)</div>", unsafe_allow_html=True)

    for _, inc in df_inc.iterrows():
        inc_id = inc["id"]
        status_val = inc["status"]
        sev_val = inc["severity"]
        batch_val = inc["batch_id"]
        stage_val = inc["stage"]
        type_val = inc["type"]
        detected_val = pd.to_datetime(inc["detected_at"]).strftime("%Y-%m-%d %H:%M:%S UTC")
        resolved_val = pd.to_datetime(inc["resolved_at"]).strftime("%Y-%m-%d %H:%M:%S UTC") if pd.notna(inc["resolved_at"]) else "Active / Unresolved"
        notes_val = inc["notes"] or ""

        # Parse evidence
        raw_evidence = inc["evidence"]
        evidence_list = raw_evidence if isinstance(raw_evidence, list) else json.loads(raw_evidence) if isinstance(raw_evidence, str) else []

        with st.expander(f"Incident #{inc_id} • [{batch_val}] {type_val.upper()} ({stage_val.capitalize()}) — {status_val.upper()}", expanded=(status_val == "open")):
            c_meta1, c_meta2 = st.columns(2)
            with c_meta1:
                st.markdown(f"""
                <div style="font-size: 0.85rem; line-height: 1.8;">
                    <b>Severity:</b> {severity_badge(sev_val)}<br>
                    <b>Status:</b> {status_chip(status_val)}<br>
                    <b>Detected:</b> <span style="color: #94A3B8;">{detected_val}</span><br>
                    <b>Resolved:</b> <span style="color: {'#10B981' if status_val == 'resolved' else '#94A3B8'};">{resolved_val}</span>
                </div>
                """, unsafe_allow_html=True)
            with c_meta2:
                st.markdown(f"""
                <div style="font-size: 0.85rem; line-height: 1.8;">
                    <b>Batch ID:</b> <code style="color: #93C5FD;">{batch_val}</code><br>
                    <b>Pipeline Stage:</b> <code style="color: #93C5FD;">{stage_val}</code><br>
                    <b>Affected Records:</b> <b style="color: #F8FAFC;">{len(evidence_list):,} transaction IDs</b>
                </div>
                """, unsafe_allow_html=True)

            # Show Evidence Sample
            if evidence_list:
                st.markdown("<div style='margin-top: 0.8rem; font-weight: 600; font-size: 0.82rem; color: #94A3B8;'>OFFENDING TRANSACTION IDS (SAMPLE)</div>", unsafe_allow_html=True)
                sample_display = ", ".join(str(x) for x in evidence_list[:15])
                if len(evidence_list) > 15:
                    sample_display += f" ... (+{len(evidence_list) - 15} more)"
                st.code(sample_display, language="text")

            # Notes & Status Transition Actions
            st.markdown("<div style='margin-top: 0.8rem; font-weight: 600; font-size: 0.82rem; color: #94A3B8;'>OPERATIONAL TRIAGE & ACTIONS</div>", unsafe_allow_html=True)
            note_col, btn_col1, btn_col2 = st.columns([3, 1, 1])

            with note_col:
                new_note = st.text_input("Triage Notes / Root Cause Analysis", value=notes_val, key=f"note_in_{inc_id}")

            with btn_col1:
                st.markdown("<div style='margin-top: 1.7rem;'></div>", unsafe_allow_html=True)
                if st.button("🔍 Investigate", key=f"btn_inv_{inc_id}", use_container_width=True, disabled=(status_val == "investigating")):
                    update_incident(inc_id, "investigating", new_note)
                    st.toast(f"Incident #{inc_id} marked as Investigating.", icon="🔍")
                    st.rerun()

            with btn_col2:
                st.markdown("<div style='margin-top: 1.7rem;'></div>", unsafe_allow_html=True)
                if st.button("✅ Resolve", key=f"btn_res_{inc_id}", use_container_width=True, disabled=(status_val == "resolved")):
                    update_incident(inc_id, "resolved", new_note or "Root cause verified and remediated.")
                    st.toast(f"Incident #{inc_id} marked as Resolved.", icon="✅")
                    st.rerun()
