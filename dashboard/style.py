"""
BankFlow Sentinel - Dashboard Styling & Plotly Themes (dashboard/style.py)
Fintech Operations Dark Mode Design System
"""

import plotly.graph_objects as go
import streamlit as st

# Color Palette Constants
COLOR_BG = "#0B1220"
COLOR_CARD_BG = "#111A2E"
COLOR_CARD_BORDER = "#1E293B"
COLOR_PRIMARY = "#3B82F6"
COLOR_TEXT = "#E5E7EB"
COLOR_TEXT_MUTED = "#94A3B8"

# Severity Colors
COLOR_SEV_HIGH = "#EF4444"
COLOR_SEV_MED = "#F59E0B"
COLOR_SEV_LOW = "#38BDF8"

# Status Colors
COLOR_STATUS_OPEN = "#EF4444"
COLOR_STATUS_INV = "#F59E0B"
COLOR_STATUS_RES = "#10B981"


def apply_custom_css():
    """Injects high-end fintech dark theme CSS styles into Streamlit."""
    custom_css = f"""
    <style>
    /* Global Styles & Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }}

    /* Main Container Padding */
    .block-container {{
        padding-top: 1.5rem !important;
        padding-bottom: 2rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 95% !important;
    }}

    /* Hide Default Streamlit Chrome */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    header {{ visibility: hidden; }}
    div[data-testid="stSidebarNav"] {{ display: none !important; }}

    /* Sidebar Styling */
    section[data-testid="stSidebar"] {{
        background-color: {COLOR_CARD_BG} !important;
        border-right: 1px solid {COLOR_CARD_BORDER};
    }}
    
    section[data-testid="stSidebar"] .block-container {{
        padding-top: 2rem !important;
        padding-left: 1.25rem !important;
        padding-right: 1.25rem !important;
    }}

    /* Metric Card Styling */
    .metric-card {{
        background: linear-gradient(135deg, {COLOR_CARD_BG} 0%, #162238 100%);
        border: 1px solid {COLOR_CARD_BORDER};
        border-radius: 10px;
        padding: 1.25rem 1.25rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
        transition: transform 0.2s ease, border-color 0.2s ease;
        margin-bottom: 0.5rem;
    }}
    .metric-card:hover {{
        transform: translateY(-2px);
        border-color: {COLOR_PRIMARY};
    }}
    .metric-title {{
        color: {COLOR_TEXT_MUTED};
        font-size: 0.82rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.4rem;
    }}
    .metric-value {{
        color: #F8FAFC;
        font-size: 1.75rem;
        font-weight: 700;
        letter-spacing: -0.02em;
        line-height: 1.2;
    }}
    .metric-subtitle {{
        color: {COLOR_TEXT_MUTED};
        font-size: 0.78rem;
        margin-top: 0.35rem;
    }}
    .metric-delta-pos {{
        color: #10B981;
        font-weight: 600;
        font-size: 0.82rem;
    }}
    .metric-delta-neg {{
        color: #EF4444;
        font-weight: 600;
        font-size: 0.82rem;
    }}

    /* Badges & Chips */
    .badge {{
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 9999px;
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }}
    .badge-high {{
        background-color: rgba(239, 68, 68, 0.15);
        color: #FCA5A5;
        border: 1px solid rgba(239, 68, 68, 0.4);
    }}
    .badge-medium {{
        background-color: rgba(245, 158, 11, 0.15);
        color: #FCD34D;
        border: 1px solid rgba(245, 158, 11, 0.4);
    }}
    .badge-low {{
        background-color: rgba(56, 189, 248, 0.15);
        color: #7DD3FC;
        border: 1px solid rgba(56, 189, 248, 0.4);
    }}

    .chip-open {{
        background-color: rgba(239, 68, 68, 0.18);
        color: #F87171;
        padding: 0.18rem 0.55rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        border: 1px solid rgba(239, 68, 68, 0.35);
    }}
    .chip-investigating {{
        background-color: rgba(245, 158, 11, 0.18);
        color: #FBBF24;
        padding: 0.18rem 0.55rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        border: 1px solid rgba(245, 158, 11, 0.35);
    }}
    .chip-resolved {{
        background-color: rgba(16, 185, 129, 0.18);
        color: #34D399;
        padding: 0.18rem 0.55rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        border: 1px solid rgba(16, 185, 129, 0.35);
    }}

    /* Status Dot */
    .status-dot {{
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 6px;
    }}
    .dot-pass {{
        background-color: #10B981;
        box-shadow: 0 0 6px rgba(16, 185, 129, 0.6);
    }}
    .dot-fail {{
        background-color: #EF4444;
        box-shadow: 0 0 6px rgba(239, 68, 68, 0.6);
    }}

    /* Card Containers */
    .content-box {{
        background-color: {COLOR_CARD_BG};
        border: 1px solid {COLOR_CARD_BORDER};
        border-radius: 10px;
        padding: 1.5rem;
        margin-bottom: 1.25rem;
    }}

    /* Section Subheaders */
    .section-header {{
        color: #F1F5F9;
        font-size: 1.15rem;
        font-weight: 700;
        letter-spacing: -0.01em;
        margin-bottom: 0.8rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }}

    /* Modern Table Tweaks */
    div[data-testid="stDataFrame"] {{
        border: 1px solid {COLOR_CARD_BORDER};
        border-radius: 8px;
        overflow: hidden;
    }}

    /* Disclaimer Footer */
    .footer-disclaimer {{
        margin-top: 3rem;
        padding: 1rem 1.25rem;
        background-color: rgba(17, 26, 46, 0.6);
        border: 1px solid {COLOR_CARD_BORDER};
        border-radius: 8px;
        color: #64748B;
        font-size: 0.78rem;
        text-align: center;
        line-height: 1.4;
    }}
    </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)


def get_plotly_layout(height: int = 380, title: str = "") -> dict:
    """Returns a unified dark fintech theme dictionary for Plotly charts."""
    return dict(
        height=height,
        title=dict(
            text=f"<b>{title}</b>" if title else "",
            font=dict(family="Inter, sans-serif", size=15, color="#F8FAFC"),
            x=0.01,
            y=0.96
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=COLOR_TEXT, size=11),
        margin=dict(l=30, r=30, t=50 if title else 20, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=11, color=COLOR_TEXT_MUTED)
        ),
        xaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            zerolinecolor="rgba(255, 255, 255, 0.08)",
            linecolor="rgba(255, 255, 255, 0.08)",
            tickfont=dict(size=10, color=COLOR_TEXT_MUTED)
        ),
        yaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.05)",
            zerolinecolor="rgba(255, 255, 255, 0.08)",
            linecolor="rgba(255, 255, 255, 0.08)",
            tickfont=dict(size=10, color=COLOR_TEXT_MUTED)
        ),
        hoverlabel=dict(
            bgcolor="#1E293B",
            font_size=12,
            font_family="Inter, sans-serif"
        )
    )


def style_plotly_fig(fig: go.Figure, height: int = 380, title: str = "") -> go.Figure:
    """Applies the dark fintech styling directly to an existing Plotly figure."""
    fig.update_layout(**get_plotly_layout(height=height, title=title))
    return fig
