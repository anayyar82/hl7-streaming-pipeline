"""
Reusable sidebar filter components for HL7App pages.

Each helper renders Streamlit sidebar widgets and returns filter values.
Pages call these, then apply filters to their DataFrames client-side.
"""

import datetime
import streamlit as st
import pandas as pd


def facility_filter(df: pd.DataFrame, col: str = "location_facility", key: str = "fac") -> list[str]:
    """Multiselect sidebar filter for facility. Returns selected values (empty = all)."""
    if df.empty or col not in df.columns:
        return []
    options = sorted(df[col].dropna().unique())
    if len(options) <= 1:
        return options
    selected = st.sidebar.multiselect("Facility", options, default=options, key=key)
    return selected


def department_filter(df: pd.DataFrame, col: str = "department", key: str = "dept") -> list[str]:
    """Radio filter for department. Returns selected values list."""
    if df.empty or col not in df.columns:
        return []
    options = sorted(df[col].dropna().unique())
    choice = st.sidebar.radio("Department", ["All"] + options, key=key, horizontal=True)
    if choice == "All":
        return options
    return [choice]


def date_range_filter(
    df: pd.DataFrame,
    col: str = "activity_date",
    key: str = "dr",
    label: str = "Date Range",
) -> tuple[datetime.date, datetime.date]:
    """Date-range picker. Returns (start, end) dates."""
    if df.empty or col not in df.columns:
        today = datetime.date.today()
        return today - datetime.timedelta(days=30), today
    dates = pd.to_datetime(df[col])
    min_d, max_d = dates.min().date(), dates.max().date()
    start, end = st.sidebar.date_input(
        label, value=(min_d, max_d), min_value=min_d, max_value=max_d, key=key,
    )
    return start, end


def weekend_toggle(key: str = "wknd") -> str:
    """Toggle for weekday/weekend/all. Returns 'All', 'Weekdays', or 'Weekends'."""
    return st.sidebar.radio("Day Type", ["All", "Weekdays", "Weekends"], key=key, horizontal=True)


def apply_facility(df: pd.DataFrame, selected: list[str], col: str = "location_facility") -> pd.DataFrame:
    if not selected or col not in df.columns:
        return df
    return df[df[col].isin(selected)]


def apply_date_range(df: pd.DataFrame, start, end, col: str = "activity_date") -> pd.DataFrame:
    if col not in df.columns:
        return df
    dates = pd.to_datetime(df[col]).dt.date
    return df[(dates >= start) & (dates <= end)]


def apply_weekend(df: pd.DataFrame, choice: str, col: str = "is_weekend") -> pd.DataFrame:
    if choice == "All" or col not in df.columns:
        return df
    if choice == "Weekends":
        return df[df[col] == True]  # noqa: E712
    return df[df[col] == False]  # noqa: E712


def sidebar_section(title: str = "Filters"):
    """Render a sidebar section header."""
    st.sidebar.markdown(f"### {title}")
    st.sidebar.markdown("---")
