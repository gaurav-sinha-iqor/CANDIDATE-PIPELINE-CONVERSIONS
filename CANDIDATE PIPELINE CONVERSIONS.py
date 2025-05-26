import streamlit as st 
import pandas as pd

# Set page title
st.set_page_config(page_title="CANDIDATE PIPELINE CONVERSIONS")

# Load the data
sg = pd.read_csv("SOURCING & EARLY STAGE METRICS.csv")

# Convert date columns to datetime
sg['INVITATIONDT'] = pd.to_datetime(sg['INVITATIONDT'], errors='coerce')
sg['ACTIVITY_CREATED_AT'] = pd.to_datetime(sg['ACTIVITY_CREATED_AT'], errors='coerce')
sg['INSERTEDDATE'] = pd.to_datetime(sg['INSERTEDDATE'], errors='coerce')

# Custom colors for styling
custom_colors = ["#2F76B9", "#3B9790", "#F5BA2E", "#6A4C93", "#F77F00", "#B4BBBE", "#e6657b", "#026df5", "#5aede2"]

# Set the main title
st.title("CANDIDATE PIPELINE CONVERSIONS")

# Filters
st.subheader("Filters")

# Ensure valid dates before showing date filter
if sg['INVITATIONDT'].dropna().empty:
    st.error("No valid INVITATIONDT values available in the data.")
    st.stop()

min_date = sg['INVITATIONDT'].min()
max_date = sg['INVITATIONDT'].max()

start_date, end_date = st.date_input("Select Date Range", [min_date, max_date])

with st.expander("Select Work Location(s)"):
    selected_worklocations = st.multiselect(
        "Work Location",
        options=sorted(sg['WORKLOCATION'].dropna().unique()),
        default=None
    )

with st.expander("Select Campaign Title(s)"):
    selected_campaigns = st.multiselect(
        "Campaign Title",
        options=sorted(sg['CAMPAIGNTITLE'].dropna().unique()),
        default=None
    )

# Filter data based on selections
sg_filtered = sg[
    (sg['INVITATIONDT'] >= pd.to_datetime(start_date)) &
    (sg['INVITATIONDT'] <= pd.to_datetime(end_date))
]

if selected_worklocations:
    sg_filtered = sg_filtered[sg_filtered['WORKLOCATION'].isin(selected_worklocations)]

if selected_campaigns:
    sg_filtered = sg_filtered[sg_filtered['CAMPAIGNTITLE'].isin(selected_campaigns)]

# Drop rows without campaign ID
sg_filtered = sg_filtered.dropna(subset=['CAMPAIGNINVITATIONID'])

# Get total unique campaign invitation IDs for percentage calculation
total_unique_ids = sg_filtered['CAMPAIGNINVITATIONID'].nunique()

# Updated compute_metric function with 'Any' and 'Client Folder' logic
def compute_metric(title, from_condition, to_condition):
    filtered = sg_filtered.copy()
    
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

    # Combined filter
    mask = from_mask & to_mask

    count = filtered[mask]['CAMPAIGNINVITATIONID'].nunique()
    percentage = f"{(count / total_unique_ids * 100):.2f}" if total_unique_ids else "0.00"

    return {"Metric": title, "Count": count, "Percentage(%)": percentage}

# Calculate all required metrics
summary_data = [
    compute_metric("Application to Completed", 'Inbox', 'Completed'),
    compute_metric("Application to Passed Prescreening", 'Completed', 'Passed MQ'),
    compute_metric("Passed Prescreening to Talent Pool", 'Passed MQ', 'Talent Pool'),
    compute_metric("Application to Talent Pool", 'Inbox', 'Talent Pool'),
    
    compute_metric("Application to Client Folder ", 'Any', 'Client Folder'),
    
    compute_metric("Application to Shortlisted", 'Any', 'Shortlisted'),
    
    compute_metric("Application to Hired", 'Any', 'Hired'),
    
    compute_metric("Talent Pool to Client Folder", 'Talent Pool', 'Client Folder'),
    
    compute_metric("Talent Pool to Shortlisted", 'Talent Pool', 'Shortlisted'),
    
    compute_metric("Client Folder to Shortlisted", 'Client Folder', 'Shortlisted')   
]

# Create a DataFrame
summary_df = pd.DataFrame(summary_data)

# Display summary table
st.markdown("### Folder Movement Summary")
st.dataframe(
    summary_df.style        
        .applymap(lambda _: 'color: black', subset=pd.IndexSlice[:, ['Count', 'Percentage(%)']])
)
