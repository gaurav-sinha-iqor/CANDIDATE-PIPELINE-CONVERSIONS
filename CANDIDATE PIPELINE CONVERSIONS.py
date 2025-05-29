import streamlit as st 
import pandas as pd

# Set page title
st.set_page_config(page_title="CANDIDATE PIPELINE CONVERSIONS")

# Load the data
cp = pd.read_csv("SOURCING & EARLY STAGE METRICS.csv")

# Convert date columns to datetime
cp['INVITATIONDT'] = pd.to_datetime(cp['INVITATIONDT'], errors='coerce')
cp['ACTIVITY_CREATED_AT'] = pd.to_datetime(cp['ACTIVITY_CREATED_AT'], errors='coerce')
cp['INSERTEDDATE'] = pd.to_datetime(cp['INSERTEDDATE'], errors='coerce')

# Custom colors for styling
custom_colors = ["#2F76B9", "#3B9790", "#F5BA2E", "#6A4C93", "#F77F00", "#B4BBBE", "#e6657b", "#026df5", "#5aede2"]

# Set the main title
st.title("CANDIDATE PIPELINE CONVERSIONS")

# Filters
st.subheader("Filters")

# Ensure valid dates before showing date filter
if cp['INVITATIONDT'].dropna().empty:
    st.error("No valid INVITATIONDT values available in the data.")
    st.stop()

min_date = cp['INVITATIONDT'].min()
max_date = cp['INVITATIONDT'].max()

date_range = st.date_input("Select Date Range", (min_date, max_date), min_value=min_date, max_value=max_date)
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        st.error("Please select a valid date range (start and end dates).")
        st.stop()

with st.expander("Select Work Location(s)"):
    selected_worklocations = st.multiselect(
        "Work Location",
        options=sorted(cp['WORKLOCATION'].dropna().unique()),
        default=None
    )

with st.expander("Select Campaign Title(s)"):
    selected_campaigns = st.multiselect(
        "Campaign Title",
        options=sorted(cp['CAMPAIGNTITLE'].dropna().unique()),
        default=None
    )

# Filter data based on selections
cp_filtered = cp[
    (cp['INVITATIONDT'] >= pd.to_datetime(start_date)) &
    (cp['INVITATIONDT'] <= pd.to_datetime(end_date))
]

if selected_worklocations:
    cp_filtered = cp_filtered[cp_filtered['WORKLOCATION'].isin(selected_worklocations)]

if selected_campaigns:
    cp_filtered = cp_filtered[cp_filtered['CAMPAIGNTITLE'].isin(selected_campaigns)]

st.write("✅ Filtered Rows:", cp_filtered.shape[0])
st.write("✅ Unique Campaign IDs:", cp_filtered['CAMPAIGNINVITATIONID'].nunique())
    
if cp_filtered.empty:
    st.warning("No data after filtering. Please adjust filters.")
    st.stop()
    
st.write("🚀 Starting metric computation...")
total_unique_ids = cp_filtered['CAMPAIGNINVITATIONID'].nunique()

def compute_metric_1(title, from_condition, to_condition):
    filtered = cp_filtered.copy()

    # Define known system folders
    system_folders = [
        'Inbox', 'Unresponsive', 'Completed', 'Unresponsive Talkscore', 'Passed MQ', 'Failed MQ',
        'TalkScore Retake', 'Unresponsive Talkscore Retake', 'Failed TalkScore', 'Cold Leads',
        'Cold Leads Talkscore', 'Cold Leads Talkscore Retake', 'On hold', 'Rejected',
        'Talent Pool', 'Shortlisted', 'Hired'
    ]
    system_folders = [s.lower() for s in system_folders]

    # Handle 'from_condition'
    if from_condition.strip().lower() == 'empty':
        from_mask = filtered['FOLDER_FROM_TITLE'].isna()
    elif from_condition.strip().lower() == 'any':
        from_mask = filtered['FOLDER_FROM_TITLE'].notna()
    elif from_condition.strip().lower() == 'client folder':
        from_mask = ~filtered['FOLDER_FROM_TITLE'].fillna('').str.strip().str.lower().isin(system_folders)
    else:
        from_mask = filtered['FOLDER_FROM_TITLE'].fillna('').str.strip().str.lower() == from_condition.strip().lower()

    # Handle 'to_condition'
    if to_condition.strip().lower() == 'client folder':
        to_mask = ~filtered['FOLDER_TO_TITLE'].fillna('').str.strip().str.lower().isin(system_folders)
    else:
        to_mask = filtered['FOLDER_TO_TITLE'].fillna('').str.strip().str.lower() == to_condition.strip().lower()

    # Combined filter for matched transitions
    mask = from_mask & to_mask
    matched_rows = filtered[mask]

    # Unique invitation count
    count = matched_rows['CAMPAIGNINVITATIONID'].nunique()
    percentage = f"{(count / total_unique_ids * 100):.2f}" if total_unique_ids else "0.00"

    # Prepare to calculate Avg Time
    avg_durations = []

    avg_durations = []

    for cid in matched_rows['CAMPAIGNINVITATIONID'].unique():
        cid_rows = filtered[filtered['CAMPAIGNINVITATIONID'] == cid]

        # Get 'to_time'
        if to_condition.strip().lower() == 'client folder':
            to_rows = cid_rows[
                ~cid_rows['FOLDER_TO_TITLE'].fillna('').str.strip().str.lower().isin(system_folders)
            ]
        else:
            to_rows = cid_rows[
                cid_rows['FOLDER_TO_TITLE'].fillna('').str.strip().str.lower() == to_condition.strip().lower()
            ]
        to_time = to_rows.groupby("CAMPAIGNINVITATIONID")['ACTIVITY_CREATED_AT'].max()

        # Get 'from_time'
        if from_condition.strip().lower() == 'any':
            from_rows = cid_rows[
                cid_rows['FOLDER_FROM_TITLE'].fillna('').str.strip().str.lower().isin(['inbox', ''])
            ]
            from_time = from_rows['ACTIVITY_CREATED_AT'].min()
        elif from_condition.strip().lower() == 'client folder':
            from_rows = cid_rows[
                ~cid_rows['FOLDER_FROM_TITLE'].fillna('').str.strip().str.lower().isin(system_folders)
            ]
            from_time = from_rows['ACTIVITY_CREATED_AT'].min()
        elif from_condition.strip().lower() == 'empty':
            from_rows = cid_rows[cid_rows['FOLDER_FROM_TITLE'].isna()]
            from_time = from_rows['ACTIVITY_CREATED_AT'].min()
        else:
            from_rows = cid_rows[
                cid_rows['FOLDER_FROM_TITLE'].fillna('').str.strip().str.lower() == from_condition.strip().lower()
            ]
            from_time = from_rows.groupby("CAMPAIGNINVITATIONID")['ACTIVITY_CREATED_AT'].min()

        # Calculate delta
        if pd.notna(from_time) and pd.notna(to_time):
            delta_days = (to_time - from_time).days
            avg_durations.append(delta_days)

    avg_time_display = f"{(sum(avg_durations)/len(avg_durations)):.1f}" if avg_durations else "N/A"

    return {
        "Metric": title,
        "Count": count,
        "Percentage(%)": percentage,
        "Avg Time (In Days)": avg_time_display
    }

# Calculate all required metrics
summary_data = [
    compute_metric_1("Application to Completed", 'Any', 'Completed'),
    compute_metric_1("Application to Passed Prescreening", 'Any', 'Passed MQ'),
    compute_metric_1("Passed Prescreening to Talent Pool", 'Passed MQ', 'Talent Pool'),
    compute_metric_1("Application to Talent Pool", 'Any', 'Talent Pool'),
    compute_metric_1("Application to Client Folder ", 'Any', 'Client Folder'),
    compute_metric_1("Application to Shortlisted", 'Any', 'Shortlisted'),
    compute_metric_1("Application to Hired", 'Any', 'Hired'),
    compute_metric_1("Talent Pool to Client Folder", 'Talent Pool', 'Client Folder'),
    compute_metric_1("Talent Pool to Shortlisted", 'Talent Pool', 'Shortlisted'),
    compute_metric_1("Client Folder to Shortlisted", 'Client Folder', 'Shortlisted'),
    compute_metric_1("Shortlisted to Hired", 'Shortlisted', 'Hired'),
    compute_metric_1("Shortlisted to Rejected", 'Shortlisted', 'Rejected')
]

# Create a DataFrame
summary_df_1 = pd.DataFrame(summary_data)

# Display summary table
st.markdown("### Folder Movement Summary")
st.dataframe(
    summary_df_1.style        
        .applymap(lambda _: 'color: black', subset=pd.IndexSlice[:, ['Count', 'Percentage(%)']])
)
