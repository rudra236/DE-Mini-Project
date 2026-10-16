"""
BankFlow Sentinel - Reconciliation Page (dashboard/pages/reconciliation.py)
Stage-by-stage counts, missing IDs (downloadable), amount deltas, duplicates, and latency delays.
"""

import streamlit as st

from dashboard.components import empty_state, metric_card
from dashboard.db import (
    get_amount_mismatches,
    get_reconciliation_results,
    get_stage_discrepancy_details,
)


def render_page(selected_batch: str):
    st.markdown("<div class='section-header'>⚖️ Reconciliation & Discrepancy Breakdown</div>", unsafe_allow_html=True)

    target_batch = selected_batch if selected_batch != "All Batches" else "B003"
    st.caption(f"Inspecting reconciliation metrics for: **{target_batch}**")

    # 1. Stage Count Comparison
    df_recon = get_reconciliation_results(target_batch)
    if df_recon.empty:
        empty_state("No Reconciliation Data", f"No reconciliation results found for batch {target_batch}.", icon="ℹ️")
        return

    # Render summary metric row
    col1, col2, col3, col4, col5 = st.columns(5)
    total_missing = int(df_recon["missing_count"].sum())
    total_mismatches = int(df_recon["mismatch_count"].sum())
    total_dups = int(df_recon["duplicate_count"].sum())
    total_delayed = int(df_recon["delayed_count"].sum())
    total_invalid = int(df_recon["invalid_count"].sum())

    with col1:
        metric_card("Missing Records", f"{total_missing:,}", is_positive_delta=False, subtitle="Dropped downstream")
    with col2:
        metric_card("Amount Mismatches", f"{total_mismatches:,}", is_positive_delta=False, subtitle="Values modified")
    with col3:
        metric_card("Duplicate Records", f"{total_dups:,}", is_positive_delta=False, subtitle="Duplicate txn_ids")
    with col4:
        metric_card("Delayed (>30m)", f"{total_delayed:,}", is_positive_delta=False, subtitle="SLA breaches")
    with col5:
        metric_card("Invalid Records", f"{total_invalid:,}", is_positive_delta=False, subtitle="Schema / syntax faults")

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # 2. Stage Tabs
    stage_tabs = st.tabs(["💳 Source → Payment (Payment Stage)", "🏦 Payment → Settlement (Settlement Stage)"])

    for idx, (tab, stage_name) in enumerate(zip(stage_tabs, ["payment", "settlement"])):
        with tab:
            stage_row = df_recon[df_recon["stage"] == stage_name]
            if stage_row.empty:
                st.info(f"No records for {stage_name} stage in batch {target_batch}.")
                continue

            r = stage_row.iloc[0]
            st.markdown(f"""
            <div style="background: rgba(255,255,255,0.02); border: 1px solid #1E293B; border-radius: 8px; padding: 0.75rem 1rem; margin-bottom: 1rem; display: flex; gap: 2rem;">
                <div><span style="color: #94A3B8; font-size: 0.8rem;">UPSTREAM COUNT:</span> <b style="color: #F8FAFC;">{r['source_count']:,}</b></div>
                <div><span style="color: #94A3B8; font-size: 0.8rem;">DOWNSTREAM COUNT:</span> <b style="color: #F8FAFC;">{r['target_count']:,}</b></div>
                <div><span style="color: #94A3B8; font-size: 0.8rem;">NET LOSS / GAIN:</span> <b style="color: {'#10B981' if r['target_count'] >= r['source_count'] else '#EF4444'};">{r['target_count'] - r['source_count']:,}</b></div>
            </div>
            """, unsafe_allow_html=True)

            sub_tab1, sub_tab2, sub_tab3, sub_tab4 = st.tabs([
                "❌ Missing IDs", "💵 Amount Deltas", "🔁 Duplicate IDs", "⏱️ Delayed Transactions"
            ])

            # Sub-Tab 1: Missing IDs
            with sub_tab1:
                df_missing = get_stage_discrepancy_details(target_batch, stage_name, "missing_record")
                if not df_missing.empty:
                    col_search, col_dl = st.columns([3, 1])
                    with col_search:
                        search_term = st.text_input("Search Missing Transaction ID", "", key=f"search_miss_{stage_name}")
                    with col_dl:
                        st.markdown("<div style='margin-top: 1.8rem;'></div>", unsafe_allow_html=True)
                        csv_data = df_missing.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv_data,
                            file_name=f"{target_batch}_{stage_name}_missing_txns.csv",
                            mime="text/csv",
                            key=f"dl_miss_{stage_name}"
                        )

                    filtered_missing = df_missing
                    if search_term:
                        filtered_missing = df_missing[df_missing["txn_id"].astype(str).str.contains(search_term, case=False)]

                    st.dataframe(filtered_missing[["txn_id", "account_id", "amount", "currency", "ts", "status"]], use_container_width=True)
                else:
                    empty_state("Zero Missing Records", f"No missing records detected in {stage_name} stage.", icon="✅")

            # Sub-Tab 2: Amount Mismatches
            with sub_tab2:
                df_mismatch = get_amount_mismatches(target_batch, stage_name)
                if not df_mismatch.empty:
                    st.caption("Transactions where downstream amount differs from upstream source amount:")
                    st.dataframe(
                        df_mismatch[["txn_id", "account_id", "upstream_amount", "downstream_amount", "delta", "currency", "upstream_ts", "downstream_ts"]],
                        use_container_width=True
                    )
                else:
                    empty_state("Zero Amount Mismatches", f"All matched records have identical amounts in {stage_name} stage.", icon="✅")

            # Sub-Tab 3: Duplicate IDs
            with sub_tab3:
                df_dups = get_stage_discrepancy_details(target_batch, stage_name, "duplicate")
                if not df_dups.empty:
                    st.caption("Downstream records with identical txn_id:")
                    st.dataframe(df_dups[["txn_id", "account_id", "amount", "currency", "ts", "status"]], use_container_width=True)
                else:
                    empty_state("Zero Duplicates", f"No duplicate transaction IDs in {stage_name} stage.", icon="✅")

            # Sub-Tab 4: Delayed Transactions
            with sub_tab4:
                df_delayed = get_stage_discrepancy_details(target_batch, stage_name, "delayed")
                if not df_delayed.empty:
                    st.caption("Transactions with timestamp latency delta > 30 minutes:")
                    st.dataframe(df_delayed[["txn_id", "account_id", "amount", "currency", "ts", "status"]], use_container_width=True)
                else:
                    empty_state("Zero SLA Delays", f"All records arrived within the 30-minute SLA threshold in {stage_name} stage.", icon="✅")
