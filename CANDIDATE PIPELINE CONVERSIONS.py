import streamlit as st
import pandas as pd

# Set page title
st.set_page_config(page_title="CANDIDATE PIPELINE CONVERSIONS", layout="wide")

# Define system folders globally (converted to lowercase for consistent comparison)
SYSTEM_FOLDERS = [
    'Inbox', 'Unresponsive', 'Completed', 'Unresponsive Talkscore', 'Passed MQ', 'Failed MQ',
    'TalkScore Retake', 'Unresponsive Talkscore Retake', 'Failed TalkScore', 'Cold Leads',
    'Cold Leads Talkscore', 'Cold Leads Talkscore Retake', 'On hold', 'Rejected',
    'Talent Pool', 'Shortlisted', 'Hired'
]
SYSTEM_FOLDERS_LOWER = {s.lower() for s in SYSTEM_FOLDERS} # Use a set for faster lookups

# Custom colors for styling (if needed later, currently only used for table styling)
CUSTOM_COLORS = ["#2F76B9", "#3B9790", "#F5BA2E",
                 "#6A4C93", "#F77F00", "#B4BBBE", "#e6657b",
                 "#026df5", "#5aede2"]

# Load the data
cp_original = pd.read_csv("SOURCING & EARLY STAGE METRICS.csv")

# Convert date columns to datetime
cp_original['INVITATIONDT'] = pd.to_datetime(cp_original['INVITATIONDT'], errors='coerce')
cp_original['ACTIVITY_CREATED_AT'] = pd.to_datetime(cp_original['ACTIVITY_CREATED_AT'], errors='coerce')
cp_original['INSERTEDDATE'] = pd.to_datetime(cp_original['INSERTEDDATE'], errors='coerce')

# Pre-process folder title columns for efficient string operations
cp_original['FOLDER_FROM_TITLE_CLEAN'] = cp_original['FOLDER_FROM_TITLE'].fillna('').str.strip().str.lower()
cp_original['FOLDER_TO_TITLE_CLEAN'] = cp_original['FOLDER_TO_TITLE'].fillna('').str.strip().str.lower()

# --- Main App ---
st.title("CANDIDATE PIPELINE CONVERSIONS")

st.divider()

# --- Filters ---
st.subheader("Filters")

# Ensure valid dates before showing date filter
valid_invitation_dates = cp_original['INVITATIONDT'].dropna()
if valid_invitation_dates.empty:
    st.error("No valid INVITATIONDT values available in the data.")
    st.stop()

min_date, max_date = valid_invitation_dates.min(), valid_invitation_dates.max()

if pd.isna(min_date) or pd.isna(max_date):
    st.error("Could not determine a valid date range from INVITATIONDT.")
    st.stop()

default_start_date = (max_date - pd.Timedelta(days=60)).date()

start_date_val, end_date_val = st.date_input(
    "Select Date Range (based on Invitation Date)",
    value=[default_start_date, max_date.date()],
    min_value=min_date.date(),
    max_value=max_date.date()
)
start_datetime = pd.to_datetime(start_date_val)
end_datetime = pd.to_datetime(end_date_val) + pd.Timedelta(days=1)

with st.expander("Select Work Location(s)"):
    unique_worklocations = sorted(cp_original['CAMPAIGN_SITE'].dropna().unique())
    selected_worklocations = st.multiselect("Work Location", options=unique_worklocations, default=[])

with st.expander("Select Campaign Title(s)"):
    unique_campaigns = sorted(cp_original['CAMPAIGNTITLE'].dropna().unique())
    selected_campaigns = st.multiselect("Campaign Title", options=unique_campaigns, default=[])
st.divider()

# --- Filter Data Based on Selections (Centralized) ---
cp_filtered = cp_original[
    (cp_original['INVITATIONDT'] >= start_datetime) &
    (cp_original['INVITATIONDT'] < end_datetime)
]

if selected_worklocations:
    cp_filtered = cp_filtered[cp_filtered['CAMPAIGN_SITE'].isin(selected_worklocations)]

if selected_campaigns:
    cp_filtered = cp_filtered[cp_filtered['CAMPAIGNTITLE'].isin(selected_campaigns)]

if cp_filtered.empty:
    st.warning("No data matches the current filter criteria.")
    st.stop()

total_unique_ids_for_percentage = cp_filtered['CAMPAIGNINVITATIONID'].nunique()

# --- OPTIMIZATION: Pre-calculation Block ---
# Perform expensive calculations once on the filtered dataframe.

# 1. Pre-calculate "Unengaged" Candidates
# Sort once by candidate and time
cp_sorted = cp_filtered.sort_values(['CAMPAIGNINVITATIONID', 'ACTIVITY_CREATED_AT'])
# Calculate time diff between consecutive activities
time_diffs = cp_sorted['ACTIVITY_CREATED_AT'].diff()
# Check if the activity is for the same candidate as the previous row
is_same_candidate = cp_sorted['CAMPAIGNINVITATIONID'] == cp_sorted['CAMPAIGNINVITATIONID'].shift(1)
# A candidate is unengaged if they have a gap > 7 days between activities
unengaged_mask = (time_diffs > pd.Timedelta(days=7)) & is_same_candidate
# Get the unique set of unengaged candidate IDs
unengaged_cids_set = set(cp_sorted.loc[unengaged_mask, 'CAMPAIGNINVITATIONID'].unique())

# 2. Pre-calculate 'from_times' and 'to_times' for all transitions
# Group by candidate to get overall earliest and latest times
time_boundaries = cp_filtered.groupby('CAMPAIGNINVITATIONID')['ACTIVITY_CREATED_AT'].agg(['min', 'max']).to_dict('index')


def compute_metric_optimized(
    df: pd.DataFrame,
    metric_title: str,
    from_condition: str,
    to_condition: str,
    total_cids: int,
    unengaged_cids: set,
    time_bounds: dict
):
    """
    Computes a single metric using pre-calculated data for speed.
    """
    from_cond_lower = from_condition.strip().lower()
    to_cond_lower = to_condition.strip().lower()

    # --- 1. Identify Transitions (Count) - This part remains similar ---
    if from_cond_lower == 'any':
        from_mask = df['FOLDER_FROM_TITLE'].notna()
    elif from_cond_lower == 'client folder':
        from_mask = (~df['FOLDER_FROM_TITLE_CLEAN'].isin(SYSTEM_FOLDERS_LOWER)) & (df['FOLDER_FROM_TITLE_CLEAN'] != '')
    else:
        from_mask = df['FOLDER_FROM_TITLE_CLEAN'] == from_cond_lower

    if to_cond_lower == 'client folder':
        to_mask = (~df['FOLDER_TO_TITLE_CLEAN'].isin(SYSTEM_FOLDERS_LOWER)) & (df['FOLDER_TO_TITLE_CLEAN'] != '')
    else:
        to_mask = df['FOLDER_TO_TITLE_CLEAN'] == to_cond_lower

    event_mask = from_mask & to_mask
    cids_with_transition = set(df.loc[event_mask, 'CAMPAIGNINVITATIONID'].unique())
    
    count = len(cids_with_transition)
    percentage = f"{(count / total_cids * 100):.2f}" if total_cids > 0 else "0.00"

    # --- 2. Use Pre-calculated data for time calculations ---
    unengaged_in_this_metric = cids_with_transition.intersection(unengaged_cids)
    engaged_in_this_metric = cids_with_transition.difference(unengaged_in_this_metric)
    
    unengaged_count = len(unengaged_in_this_metric)
    
    # Get specific start and end times for this transition
    # This is the only remaining group-by, but it's on a much smaller, pre-filtered set of rows.
    relevant_to_times = df[df['CAMPAIGNINVITATIONID'].isin(cids_with_transition) & to_mask]
    to_times_per_cid = relevant_to_times.groupby('CAMPAIGNINVITATIONID')['ACTIVITY_CREATED_AT'].max()

    avg_durations = []
    avg_time_threshold_durations = []
    
    for cid in cids_with_transition:
        from_time = time_bounds.get(cid, {}).get('min') # Get pre-calculated earliest time
        to_time = to_times_per_cid.get(cid, pd.NaT)

        if pd.notna(from_time) and pd.notna(to_time) and to_time >= from_time:
            delta_days = (to_time - from_time).days
            avg_durations.append(delta_days)
            if cid in engaged_in_this_metric:
                avg_time_threshold_durations.append(delta_days)
    
    avg_time_display = f"{(sum(avg_durations) / len(avg_durations)):.1f}" if avg_durations else "N/A"
    avg_time_threshold_display = f"{(sum(avg_time_threshold_durations) / len(avg_time_threshold_durations)):.1f}" if avg_time_threshold_durations else "N/A"
    
    return {
        "Metric": metric_title,
        "Count": count,
        "Percentage(%)": percentage,
        "Avg Time (In Days)": avg_time_display,
        "Avg Time(Threshold)": avg_time_threshold_display,
        "Unengaged Candidates Count": unengaged_count
    }

# --- Calculate All Required Metrics using the optimized function ---
st.markdown("### Folder Movement Summary")

if total_unique_ids_for_percentage > 0:
    # List of metrics to compute
    metrics_to_run = [
        ("Application to Completed", 'Any', 'Completed'),
        ("Application to Passed Prescreening", 'Any', 'Passed MQ'),
        ("Passed Prescreening to Talent Pool", 'Passed MQ', 'Talent Pool'),
        ("Application to Talent Pool", 'Any', 'Talent Pool'),
        ("Application to Client Folder", 'Any', 'Client Folder'),
        ("Application to Shortlisted", 'Any', 'Shortlisted'),
        ("Application to Hired", 'Any', 'Hired'),
        ("Talent Pool to Client Folder", 'Talent Pool', 'Client Folder'),
        ("Talent Pool to Shortlisted", 'Talent Pool', 'Shortlisted'),
        ("Client Folder to Shortlisted", 'Client Folder', 'Shortlisted'),
        ("Shortlisted to Hired", 'Shortlisted', 'Hired'),
        ("Shortlisted to Rejected", 'Shortlisted', 'Rejected')
    ]
    
    summary_data = []
    with st.spinner('Calculating metrics...'):
        for title, from_cond, to_cond in metrics_to_run:
            result = compute_metric_optimized(
                df=cp_filtered,
                metric_title=title,
                from_condition=from_cond,
                to_condition=to_cond,
                total_cids=total_unique_ids_for_percentage,
                unengaged_cids=unengaged_cids_set,
                time_bounds=time_boundaries
            )
            summary_data.append(result)

    summary_df = pd.DataFrame(summary_data)
    st.dataframe(summary_df, use_container_width=True)
else:
    st.info("No data to compute metrics after filtering.")

