import streamlit as st
import pandas as pd
import numpy as np

# Set page title
st.set_page_config(page_title="CANDIDATE PIPELINE CONVERSIONS")

# Cache data loading and preprocessing
@st.cache_data
def load_data():
    df = pd.read_csv("SOURCING & EARLY STAGE METRICS.csv")
    df['INVITATIONDT'] = pd.to_datetime(df['INVITATIONDT'], errors='coerce')
    df['ACTIVITY_CREATED_AT'] = pd.to_datetime(df['ACTIVITY_CREATED_AT'], errors='coerce')
    df['INSERTEDDATE'] = pd.to_datetime(df['INSERTEDDATE'], errors='coerce')
    
    # Precompute lowercase and stripped folder titles
    df['FOLDER_FROM_TITLE_CLEAN'] = df['FOLDER_FROM_TITLE'].fillna('').str.strip().str.lower()
    df['FOLDER_TO_TITLE_CLEAN'] = df['FOLDER_TO_TITLE'].fillna('').str.strip().str.lower()
    
    return df

cp = load_data()

# Custom colors (can be used in graphs)
custom_colors = ["#2F76B9", "#3B9790", "#F5BA2E", "#6A4C93", "#F77F00", "#B4BBBE", "#e6657b", "#026df5", "#5aede2"]

# Title
st.title("CANDIDATE PIPELINE CONVERSIONS")

# Filters
st.subheader("Filters")

if cp['INVITATIONDT'].dropna().empty:
    st.error("No valid INVITATIONDT values available in the data.")
    st.stop()

min_date = cp['INVITATIONDT'].min()
max_date = cp['INVITATIONDT'].max()

start_date, end_date = st.date_input("Select Date Range", [min_date, max_date])

with st.expander("Select Work Location(s)"):
    selected_worklocations = st.multiselect(
        "Work Location",
        options=sorted(cp['WORKLOCATION'].dropna().unique())
    )

with st.expander("Select Campaign Title(s)"):
    selected_campaigns = st.multiselect(
        "Campaign Title",
        options=sorted(cp['CAMPAIGNTITLE'].dropna().unique())
    )

# Filter based on selections
cp_filtered = cp[
    (cp['INVITATIONDT'] >= pd.to_datetime(start_date)) &
    (cp['INVITATIONDT'] <= pd.to_datetime(end_date))
]

if selected_worklocations:
    cp_filtered = cp_filtered[cp_filtered['WORKLOCATION'].isin(selected_worklocations)]

if selected_campaigns:
    cp_filtered = cp_filtered[cp_filtered['CAMPAIGNTITLE'].isin(selected_campaigns)]

cp_filtered = cp_filtered.dropna(subset=['CAMPAIGNINVITATIONID'])
total_unique_ids = cp_filtered['CAMPAIGNINVITATIONID'].nunique()

# System folders precomputed
system_folders = set([
    'inbox', 'unresponsive', 'completed', 'unresponsive talkscore', 'passed mq', 'failed mq',
    'talkscore retake', 'unresponsive talkscore retake', 'failed talkscore', 'cold leads',
    'cold leads talkscore', 'cold leads talkscore retake', 'on hold', 'rejected',
    'talent pool', 'shortlisted', 'hired'
])

def compute_metric_1(title, from_condition, to_condition):
    df = cp_filtered.copy()
    
    from_cond = from_condition.strip().lower()
    to_cond = to_condition.strip().lower()

    # FROM mask
    if from_cond == 'empty':
        from_mask = df['FOLDER_FROM_TITLE'].isna()
    elif from_cond == 'any':
        from_mask = df['FOLDER_FROM_TITLE'].notna()
    elif from_cond == 'client folder':
        from_mask = ~df['FOLDER_FROM_TITLE_CLEAN'].isin(system_folders)
    else:
        from_mask = df['FOLDER_FROM_TITLE_CLEAN'] == from_cond

    # TO mask
    if to_cond == 'client folder':
        to_mask = ~df['FOLDER_TO_TITLE_CLEAN'].isin(system_folders)
    else:
        to_mask = df['FOLDER_TO_TITLE_CLEAN'] == to_cond

    mask = from_mask & to_mask
    matched = df[mask]
    unique_ids = matched['CAMPAIGNINVITATIONID'].unique()
    count = len(unique_ids)
    percentage = f"{(count / total_unique_ids * 100):.2f}" if total_unique_ids else "0.00"

    # Duration calc optimization
    group = df[df['CAMPAIGNINVITATIONID'].isin(unique_ids)]
    group = group[['CAMPAIGNINVITATIONID', 'FOLDER_FROM_TITLE_CLEAN', 'FOLDER_TO_TITLE_CLEAN', 'ACTIVITY_CREATED_AT']]

    def get_from_time(g):
        if from_cond == 'any':
            mask = g['FOLDER_FROM_TITLE_CLEAN'].isin(['inbox', ''])
        elif from_cond == 'client folder':
            mask = ~g['FOLDER_FROM_TITLE_CLEAN'].isin(system_folders)
        elif from_cond == 'empty':
            mask = g['FOLDER_FROM_TITLE_CLEAN'] == ''
        else:
            mask = g['FOLDER_FROM_TITLE_CLEAN'] == from_cond
        return g.loc[mask, 'ACTIVITY_CREATED_AT'].min()

    def get_to_time(g):
        if to_cond == 'client folder':
            mask = ~g['FOLDER_TO_TITLE_CLEAN'].isin(system_folders)
        else:
            mask = g['FOLDER_TO_TITLE_CLEAN'] == to_cond
        return g.loc[mask, 'ACTIVITY_CREATED_AT'].max()

    avg_durations = []
    for _, group_df in group.groupby('CAMPAIGNINVITATIONID'):
        from_time = get_from_time(group_df)
        to_time = get_to_time(group_df)
        if pd.notna(from_time) and pd.notna(to_time):
            avg_durations.append((to_time - from_time).days)

    avg_time = f"{(np.mean(avg_durations)):.1f}" if avg_durations else "N/A"

    return {
        "Metric": title,
        "Count": count,
        "Percentage(%)": percentage,
        "Avg Time (In Days)": avg_time
    }

# Metric calculations
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
    compute_metric_1("Client Folder to Shortlisted", 'Client Folder', 'Shortlisted')   
]

summary_df = pd.DataFrame(summary_data)

# Display summary
st.markdown("### Folder Movement Summary")
st.dataframe(
    summary_df.style.applymap(lambda _: 'color: black', subset=pd.IndexSlice[:, ['Count', 'Percentage(%)']])
)
