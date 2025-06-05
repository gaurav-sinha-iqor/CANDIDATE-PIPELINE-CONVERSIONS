import streamlit as st
import pandas as pd

# Set page title
st.set_page_config(page_title="CANDIDATE PIPELINE CONVERSIONS")

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
    




# Set the main title
st.title("CANDIDATE PIPELINE CONVERSIONS")
st.divider()

# --- Filters ---
st.subheader("Filters")

# Ensure valid dates before showing date filter
valid_invitation_dates = cp_original['INVITATIONDT'].dropna()
if valid_invitation_dates.empty:
    st.error("No valid INVITATIONDT values available in the data.")
    st.stop()

min_date = valid_invitation_dates.min()
max_date = valid_invitation_dates.max()

# Ensure min_date and max_date are not NaT before passing to date_input
if pd.isna(min_date) or pd.isna(max_date):
    st.error("Could not determine a valid date range from INVITATIONDT.")
    st.stop()

# Compute default start as max_date - 60 days
default_start_date = (max_date - pd.Timedelta(days=60)).date()

start_date_val, end_date_val = st.date_input(
    "Select Date Range (based on Invitation Date)",
    value=[default_start_date, max_date.date()],   # Default = [max_date - 60 days, max_date]
    min_value=min_date.date(),
    max_value=max_date.date()
)
start_datetime = pd.to_datetime(start_date_val)
end_datetime = pd.to_datetime(end_date_val) + pd.Timedelta(days=1)  # Ensure end_date is inclusive up to end of day

with st.expander("Select Work Location(s)"):
    unique_worklocations = sorted(cp_original['CAMPAIGN_SITE'].dropna().unique())
    selected_worklocations = st.multiselect(
        "Work Location",
        options=unique_worklocations,
        default=[]  # Explicitly empty default
    )

with st.expander("Select Campaign Title(s)"):
    unique_campaigns = sorted(cp_original['CAMPAIGNTITLE'].dropna().unique())
    selected_campaigns = st.multiselect(
        "Campaign Title",
        options=unique_campaigns,
        default=[]  # Explicitly empty default
    )
st.divider()


# --- Filter Data Based on Selections ---
# Start with a copy of the original preprocessed data
cp_filtered = cp_original.copy()

# Apply date filter
cp_filtered = cp_filtered[
    (cp_filtered['INVITATIONDT'] >= start_datetime) &
    (cp_filtered['INVITATIONDT'] < end_datetime) # Use < for end_datetime as it's start of next day
]

if selected_worklocations:
    cp_filtered = cp_filtered[cp_filtered['CAMPAIGN_SITE'].isin(selected_worklocations)]

if selected_campaigns:
    cp_filtered = cp_filtered[cp_filtered['CAMPAIGNTITLE'].isin(selected_campaigns)]

# Drop rows without campaign ID (essential for metrics)
cp_filtered = cp_filtered.dropna(subset=['CAMPAIGNINVITATIONID'])

if cp_filtered.empty:
    st.warning("No data matches the current filter criteria.")
    st.stop()

# Get total unique campaign invitation IDs for percentage calculation from the *filtered* data
total_unique_ids_for_percentage = cp_filtered['CAMPAIGNINVITATIONID'].nunique()


def compute_metric(
    df_input: pd.DataFrame,
    metric_title: str,
    from_condition_str: str,
    to_condition_str: str,
    total_cids_for_pct: int
):
    """
    Computes a single metric: count, percentage, and average time for transitions.
    Args:
        df_input (pd.DataFrame): The filtered DataFrame with pre-cleaned folder titles.
        metric_title (str): Name of the metric.
        from_condition_str (str): The 'from' folder condition (e.g., 'Any', 'Passed MQ', 'Client Folder').
        to_condition_str (str): The 'to' folder condition.
        total_cids_for_pct (int): Total unique CAMPAIGNINVITATIONIDs for percentage calculation.
    """
    from_cond_lower = from_condition_str.strip().lower()
    to_cond_lower = to_condition_str.strip().lower()

    # --- 1. Identify Transitions (for Count) ---
    # Mask for 'from' condition of the event
    if from_cond_lower == 'empty':
        # Original logic: an activity record where FOLDER_FROM_TITLE was literally NaN
        from_event_mask = df_input['FOLDER_FROM_TITLE'].isna()
    elif from_cond_lower == 'any':
        # Original logic: any activity record where FOLDER_FROM_TITLE was not NaN
        from_event_mask = df_input['FOLDER_FROM_TITLE'].notna()
    elif from_cond_lower == 'client folder':
        from_event_mask = (~df_input['FOLDER_FROM_TITLE_CLEAN'].isin(SYSTEM_FOLDERS_LOWER)) & \
                          (df_input['FOLDER_FROM_TITLE_CLEAN'] != '')
    else:
        from_event_mask = df_input['FOLDER_FROM_TITLE_CLEAN'] == from_cond_lower

    # Mask for 'to' condition of the event
    if to_cond_lower == 'client folder':
        to_event_mask = (~df_input['FOLDER_TO_TITLE_CLEAN'].isin(SYSTEM_FOLDERS_LOWER)) & \
                        (df_input['FOLDER_TO_TITLE_CLEAN'] != '')
    else:
        to_event_mask = df_input['FOLDER_TO_TITLE_CLEAN'] == to_cond_lower
    
    # Combined mask for the transition event itself
    event_mask = from_event_mask & to_event_mask
    transitions_df = df_input[event_mask]
    
    count = transitions_df['CAMPAIGNINVITATIONID'].nunique()
    percentage = f"{(count / total_cids_for_pct * 100):.2f}" if total_cids_for_pct > 0 else "0.00"

    # --- 2. Calculate Average Time for these transitions ---
    avg_durations = []
    cids_with_this_transition = transitions_df['CAMPAIGNINVITATIONID'].unique()

    if count == 0 or not cids_with_this_transition.any():
        avg_time_display = "N/A"
    else:
        # Consider only activities for CIDs that made the specific transition
        relevant_activities_df = df_input[df_input['CAMPAIGNINVITATIONID'].isin(cids_with_this_transition)].copy()

        # Determine 'from_time' (earliest activity time meeting from_condition logic) for each relevant CID
        if from_cond_lower == 'any': # Special 'any' logic from original code for *time calculation*
            from_time_logic_mask = relevant_activities_df['FOLDER_FROM_TITLE_CLEAN'].isin(['inbox', ''])
        elif from_cond_lower == 'empty': # Based on FOLDER_FROM_TITLE being NaN
            from_time_logic_mask = relevant_activities_df['FOLDER_FROM_TITLE'].isna()
        elif from_cond_lower == 'client folder':
            from_time_logic_mask = (~relevant_activities_df['FOLDER_FROM_TITLE_CLEAN'].isin(SYSTEM_FOLDERS_LOWER)) & \
                                   (relevant_activities_df['FOLDER_FROM_TITLE_CLEAN'] != '')
        else:
            from_time_logic_mask = relevant_activities_df['FOLDER_FROM_TITLE_CLEAN'] == from_cond_lower
        
        from_times_per_cid = relevant_activities_df[from_time_logic_mask].groupby('CAMPAIGNINVITATIONID')['ACTIVITY_CREATED_AT'].min()

        # Determine 'to_time' (latest activity time meeting to_condition logic) for each relevant CID
        if to_cond_lower == 'client folder':
            to_time_logic_mask = (~relevant_activities_df['FOLDER_TO_TITLE_CLEAN'].isin(SYSTEM_FOLDERS_LOWER)) & \
                                 (relevant_activities_df['FOLDER_TO_TITLE_CLEAN'] != '')
        else:
            to_time_logic_mask = relevant_activities_df['FOLDER_TO_TITLE_CLEAN'] == to_cond_lower
            
        to_times_per_cid = relevant_activities_df[to_time_logic_mask].groupby('CAMPAIGNINVITATIONID')['ACTIVITY_CREATED_AT'].max()

        # Calculate durations for CIDs that had the transition
        for cid in cids_with_this_transition:
            from_time = from_times_per_cid.get(cid, pd.NaT)
            to_time = to_times_per_cid.get(cid, pd.NaT)

            if pd.notna(from_time) and pd.notna(to_time) and to_time >= from_time:
                delta_days = (to_time - from_time).days
                avg_durations.append(delta_days)
        
        avg_time_display = f"{(sum(avg_durations) / len(avg_durations)):.1f}" if avg_durations else "N/A"

    return {
        "Metric": metric_title,
        "Count": count,
        "Percentage(%)": percentage,
        "Avg Time (In Days)": avg_time_display
    }

# --- Calculate All Required Metrics ---
# Ensure cp_filtered is not empty before proceeding
if total_unique_ids_for_percentage > 0:
    summary_data = [
        compute_metric(cp_filtered, "Application to Completed", 'Any', 'Completed', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Application to Passed Prescreening", 'Any', 'Passed MQ', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Passed Prescreening to Talent Pool", 'Passed MQ', 'Talent Pool', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Application to Talent Pool", 'Any', 'Talent Pool', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Application to Client Folder", 'Any', 'Client Folder', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Application to Shortlisted", 'Any', 'Shortlisted', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Application to Hired", 'Any', 'Hired', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Talent Pool to Client Folder", 'Talent Pool', 'Client Folder', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Talent Pool to Shortlisted", 'Talent Pool', 'Shortlisted', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Client Folder to Shortlisted", 'Client Folder', 'Shortlisted', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Shortlisted to Hired", 'Shortlisted', 'Hired', total_unique_ids_for_percentage),
        compute_metric(cp_filtered, "Shortlisted to Rejected", 'Shortlisted', 'Rejected', total_unique_ids_for_percentage)
    ]
    summary_df = pd.DataFrame(summary_data)

    # --- Display Summary Table ---
    st.markdown("### Folder Movement Summary")
    st.dataframe(
        summary_df.style.applymap(lambda _: 'color: black', subset=pd.IndexSlice[:, ['Count', 'Percentage(%)']])
    )
else:
    st.info("No data to compute metrics after filtering and dropping rows with missing Campaign Invitation IDs.")


        
