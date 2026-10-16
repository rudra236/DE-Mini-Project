"""
BankFlow Sentinel - UI Components (dashboard/components.py)
Reusable fintech operations widgets, badges, chips, and cards.
"""

import streamlit as st

from dashboard.db import check_system_services


def metric_card(title: str, value: str, delta: str = None, subtitle: str = None, is_positive_delta: bool = True):
    """Renders a styled fintech metric card."""
    delta_html = ""
    if delta:
        delta_class = "metric-delta-pos" if is_positive_delta else "metric-delta-neg"
        arrow = "↑" if is_positive_delta else "↓"
        delta_html = f"<span class='{delta_class}'>{arrow} {delta}</span>"

    subtitle_html = f"<div class='metric-subtitle'>{subtitle}</div>" if subtitle else ""

    html = f"""
    <div class="metric-card">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{value} {delta_html}</div>
        {subtitle_html}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def severity_badge(severity: str) -> str:
    """Returns HTML badge for incident severity."""
    sev = str(severity).lower()
    if sev == "high":
        return '<span class="badge badge-high">● HIGH</span>'
    elif sev == "medium":
        return '<span class="badge badge-medium">● MEDIUM</span>'
    else:
        return '<span class="badge badge-low">● LOW</span>'


def status_chip(status: str) -> str:
    """Returns HTML chip for incident status."""
    st_val = str(status).lower()
    if st_val == "open":
        return '<span class="chip-open">OPEN</span>'
    elif st_val == "investigating":
        return '<span class="chip-investigating">INVESTIGATING</span>'
    elif st_val == "resolved":
        return '<span class="chip-resolved">RESOLVED</span>'
    else:
        return f'<span class="badge badge-low">{status.upper()}</span>'


def system_status_sidebar():
    """Renders active system status dots in the sidebar."""
    services = check_system_services()

    def dot_html(is_up: bool, label: str):
        dot_class = "dot-pass" if is_up else "dot-fail"
        status_txt = "ONLINE" if is_up else "OFFLINE"
        status_col = "#10B981" if is_up else "#EF4444"
        return f'<div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.35rem; font-size: 0.8rem;"><span style="color: #94A3B8;">{label}</span><span><span class="status-dot {dot_class}"></span><b style="color: {status_col};">{status_txt}</b></span></div>'

    st.sidebar.markdown("<hr style='border: none; border-top: 1px solid #1E293B; margin: 1.25rem 0;'>", unsafe_allow_html=True)
    st.sidebar.markdown("<div style='color: #64748B; font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem;'>SYSTEM TELEMETRY</div>", unsafe_allow_html=True)

    status_block = f'<div style="background: rgba(0,0,0,0.2); border: 1px solid #1E293B; border-radius: 8px; padding: 0.75rem 0.85rem;">{dot_html(services["postgres"], "PostgreSQL 16")}{dot_html(services["kafka"], "Kafka 3.7")}{dot_html(services["mongo"], "MongoDB 7")}{dot_html(services["api"], "FastAPI Gateway")}</div>'
    st.sidebar.html(status_block)


def footer_disclaimer():
    """Renders mandatory project disclaimer footer."""
    html = """
    <div class="footer-disclaimer">
        <b>BankFlow Sentinel Operations Console</b> • <span style="color: #F59E0B;">Synthetic data only.</span>
        Discrepancies represent simulated banking pipeline processing issues to investigate, not confirmed monetary loss.
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def empty_state(title: str, description: str = "", icon: str = "🔍"):
    """Displays a clean empty state card."""
    html = f"""
    <div class="content-box" style="text-align: center; padding: 2.5rem 1rem;">
        <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">{icon}</div>
        <div style="color: #F8FAFC; font-size: 1.1rem; font-weight: 600; margin-bottom: 0.3rem;">{title}</div>
        <div style="color: #94A3B8; font-size: 0.85rem; max-width: 450px; margin: 0 auto;">{description}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)
