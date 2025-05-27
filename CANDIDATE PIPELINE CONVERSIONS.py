import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="CANDIDATE PIPELINE CONVERSIONS")

@st.cache_data
def load_and_preprocess():
    df = pd.read_csv("SOURCING & EARLY STAGE METRICS.csv")
    df['INVITATIONDT'] = pd.to_datetime(df['INVITATIONDT'], errors='coerce')
    df['ACTIVITY_CREATED_AT'] = pd.to_datetime(df['ACTIVITY_CREATED_AT'], errors='coerce')
    df['INSERTEDDATE'] = pd.to_datetime(df['INSERTEDDATE'], errors='coerce')
    
    # Pre-clean strings once
    for col in ['FOLDER_FROM_TITLE', 'FOLDER_TO_TITLE']:
        df[col] = df[col].fillna('').astype(str).str.strip().str.lower()
    
    return df

df = load_and_preprocess()

# UI
st.title("CANDIDATE PIPELINE CONVERSIONS")
st.subheader("Filters")

if df['INVITATIONDT'].dropna().empty:
    st.error("No valid INVITATIONDT values available.")
    st.stop()

min_date, max_date = df['INVITATIONDT'].min(), df['INVITATIONDT'].max()
start_date, end_date = st.date_input("Select Date Range", [min_date, max_date])

with st.expander("Select Work Location(s)"):
    work_locs = st.multiselect("Work Location", sorted(df['WORKLOCATION'].dropna().unique()))

with st.expander("Select Campaign Title(s)"):
    campaigns = st.multiselect("Campaign Title", sorted(df['CAMPAIGNTITLE'].dropna().unique()))

# Filter once based on user selection
mask = (df['INVITATIONDT'] >= pd.to_datetime(start_date)) & (df['INVITATIONDT'] <= pd.to_datetime(end_date))
if work_locs:
    mask &= df['WORKLOCATION'].isin(work_locs)
if campaigns:
    mask &= df['CAMPAIGNTITLE'].isin(campaigns)

df = df[mask].copy()
df = df.dropna(subset=['CAMPAIGNINVITATIONID'])

if df.empty:
    st.warning("No data available after filters.")
    st.stop()

# For reuse
invitation_ids = df['CAMPAIGNINVITATIONID'].unique()
total_ids = len(invitation_ids)

# System folders set
system_folders = {
    'inbox', 'unresponsive', 'completed', 'unresponsive talkscore', 'passed mq', 'failed mq',
    'talkscore retake', 'unresponsive talkscore retake', 'failed talkscore', 'cold leads',
    'cold leads talkscore', 'cold leads talkscore retake', 'on hold', 'rejected',
    'talent pool', 'shortlisted', 'hired'
}

# Group by ID once for speed
grouped = df.groupby('CAMPAIGNINVITATIONID')

def get_transition_stats(title, from_cond, to_cond):
    from_cond, to_cond = from_cond.lower(), to_cond.lower()
    count = 0
    durations = []

    for cid, group in grouped:
        from_match = to_match = False
        from_time = to_time = None

        # TO logic
        if to_cond == "client folder":
            to_rows = group[~group['FOLDER_TO_TITLE'].isin(system_folders)]
        else:
            to_rows = group[group['FOLDER_TO_TITLE'] == to_cond]
        if not to_rows.empty:
            to_time = to_rows['ACTIVITY_CREATED_AT'].max()
            to_match = True

        # FROM logic
        if from_cond == "any":
            from_rows = group[group['FOLDER_FROM_TITLE'].isin(["inbox", ""])]
        elif from_cond == "client folder":
            from_rows = group[~group['FOLDER_FROM_TITLE'].isin(system_folders)]
        elif from_cond == "empty":
            from_rows = group[group['FOLDER_FROM_TITLE'] == ""]
        else:
            from_rows = group[group['FOLDER_FROM_TITLE'] == from_cond]

        if not from_rows.empty:
            from_time = from_rows['ACTIVITY_CREATED_AT'].min()
            from_match = True

        if from_match and to_match:
            count += 1
            if pd.notna(from_time) and pd.notna(to_time):
                durations.append((to_time - from_time).days)

    percent = f"{(count / total_ids * 100):.2f}" if total_ids else "0.00"
    avg_days = f"{(np.mean(durations)):.1f}" if durations else "N/A"

    return {
        "Metric": title,
        "Count": count,
        "Percentage(%)": percent,
        "Avg Time (In Days)": avg_days
    }

# Define transitions
metrics = [
    ("Application to Completed", "Any", "Completed"),
    ("Application to Passed Prescreening", "Any", "Passed MQ"),
    ("Passed Prescreening to Talent Pool", "Passed MQ", "Talent Pool"),
    ("Application to Talent Pool", "Any", "Talent Pool"),
    ("Application to Client Folder", "Any", "Client Folder"),
    ("Application to Shortlisted", "Any", "Shortlisted"),
    ("Application to Hired", "Any", "Hired"),
    ("Talent Pool to Client Folder", "Talent Pool", "Client Folder"),
    ("Talent Pool to Shortlisted", "Talent Pool", "Shortlisted"),
    ("Client Folder to Shortlisted", "Client Folder", "Shortlisted")
]

# Compute all
summary_data = [get_transition_stats(title, from_c, to_c) for title, from_c, to_c in metrics]
summary_df = pd.DataFrame(summary_data)

# Display
st.markdown("### Folder Movement Summary")
st.dataframe(summary_df.style.applymap(lambda _: 'color: black', subset=['Count', 'Percentage(%)']))
