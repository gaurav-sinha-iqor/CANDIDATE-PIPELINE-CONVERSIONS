import streamlit as st 
import pandas as pd
import numpy as np
from functools import lru_cache
import warnings
warnings.filterwarnings('ignore')

# Set page title
st.set_page_config(page_title="CANDIDATE PIPELINE CONVERSIONS")

@st.cache_data
def load_and_preprocess_data():
    """Load and preprocess data with caching for better performance"""
    # Load the data
    cp = pd.read_csv("SOURCING & EARLY STAGE METRICS.csv")
    
    # Convert date columns to datetime with efficient method
    date_columns = ['INVITATIONDT', 'ACTIVITY_CREATED_AT', 'INSERTEDDATE']
    for col in date_columns:
        if col in cp.columns:
            cp[col] = pd.to_datetime(cp[col], errors='coerce', format='mixed', cache=True)
    
    # Drop rows without campaign ID early to reduce dataset size
    cp = cp.dropna(subset=['CAMPAIGNINVITATIONID'])
    
    # Optimize data types to reduce memory usage
    # Note: Avoid converting to category initially to prevent assignment issues
    # We'll optimize memory after all data processing is complete
    pass
    
    return cp

@st.cache_data
def get_filter_options(cp):
    """Cache filter options to avoid recomputation"""
    work_locations = sorted(cp['WORKLOCATION'].dropna().unique())
    campaign_titles = sorted(cp['CAMPAIGNTITLE'].dropna().unique())
    min_date = cp['INVITATIONDT'].min()
    max_date = cp['INVITATIONDT'].max()
    
    return work_locations, campaign_titles, min_date, max_date

def filter_data_efficiently(cp, start_date, end_date, selected_worklocations, selected_campaigns):
    """Efficiently filter data using vectorized operations"""
    # Convert dates once
    start_date_ts = pd.to_datetime(start_date)
    end_date_ts = pd.to_datetime(end_date)
    
    # Create boolean masks
    date_mask = (cp['INVITATIONDT'] >= start_date_ts) & (cp['INVITATIONDT'] <= end_date_ts)
    
    # Apply filters progressively to reduce data size
    cp_filtered = cp[date_mask].copy()  # Use copy to avoid SettingWithCopyWarning
    
    if selected_worklocations:
        location_mask = cp_filtered['WORKLOCATION'].isin(selected_worklocations)
        cp_filtered = cp_filtered[location_mask]
    
    if selected_campaigns:
        campaign_mask = cp_filtered['CAMPAIGNTITLE'].isin(selected_campaigns)
        cp_filtered = cp_filtered[campaign_mask]
    
    # Optimize memory usage after filtering is complete
    categorical_columns = ['WORKLOCATION', 'CAMPAIGNTITLE', 'FOLDER_FROM_TITLE', 'FOLDER_TO_TITLE']
    for col in categorical_columns:
        if col in cp_filtered.columns and not cp_filtered[col].empty:
            try:
                cp_filtered[col] = cp_filtered[col].astype('category')
            except:
                pass  # Skip if conversion fails
    
    return cp_filtered

@lru_cache(maxsize=128)
def get_system_folders_set():
    """Cache system folders as a set for faster lookups"""
    system_folders = {
        'inbox', 'unresponsive', 'completed', 'unresponsive talkscore', 'passed mq', 'failed mq',
        'talkscore retake', 'unresponsive talkscore retake', 'failed talkscore', 'cold leads',
        'cold leads talkscore', 'cold leads talkscore retake', 'on hold', 'rejected',
        'talent pool', 'shortlisted', 'hired'
    }
    return system_folders

def compute_metric_optimized(cp_filtered, title, from_condition, to_condition, total_unique_ids):
    """Optimized metric computation using vectorized operations"""
    system_folders = get_system_folders_set()
    
    # Prepare string columns once - handle potential categorical columns
    folder_from_clean = cp_filtered['FOLDER_FROM_TITLE'].astype(str).fillna('').str.strip().str.lower()
    folder_to_clean = cp_filtered['FOLDER_TO_TITLE'].astype(str).fillna('').str.strip().str.lower()
    
    # Handle 'from_condition' with vectorized operations
    if from_condition.strip().lower() == 'empty':
        from_mask = cp_filtered['FOLDER_FROM_TITLE'].isna()
    elif from_condition.strip().lower() == 'any':
        from_mask = cp_filtered['FOLDER_FROM_TITLE'].notna()
    elif from_condition.strip().lower() == 'client folder':
        from_mask = ~folder_from_clean.isin(system_folders)
    else:
        from_mask = folder_from_clean == from_condition.strip().lower()

    # Handle 'to_condition' with vectorized operations
    if to_condition.strip().lower() == 'client folder':
        to_mask = ~folder_to_clean.isin(system_folders)
    else:
        to_mask = folder_to_clean == to_condition.strip().lower()

    # Combined filter for matched transitions
    mask = from_mask & to_mask
    matched_rows = cp_filtered[mask]

    # Unique invitation count
    count = matched_rows['CAMPAIGNINVITATIONID'].nunique()
    percentage = f"{(count / total_unique_ids * 100):.2f}" if total_unique_ids else "0.00"

    # Optimized average time calculation
    avg_time_display = calculate_avg_time_optimized(
        cp_filtered, matched_rows, from_condition, to_condition, system_folders
    )

    return {
        "Metric": title,
        "Count": count,
        "Percentage(%)": percentage,
        "Avg Time (In Days)": avg_time_display
    }

def calculate_avg_time_optimized(cp_filtered, matched_rows, from_condition, to_condition, system_folders):
    """Optimized average time calculation using groupby operations"""
    if matched_rows.empty:
        return "N/A"
    
    unique_cids = matched_rows['CAMPAIGNINVITATIONID'].unique()
    
    # Prepare data for vectorized operations
    relevant_data = cp_filtered[cp_filtered['CAMPAIGNINVITATIONID'].isin(unique_cids)]
    folder_from_clean = relevant_data['FOLDER_FROM_TITLE'].astype(str).fillna('').str.strip().str.lower()
    folder_to_clean = relevant_data['FOLDER_TO_TITLE'].astype(str).fillna('').str.strip().str.lower()
    
    durations = []
    
    # Group by campaign ID for efficient processing
    grouped = relevant_data.groupby('CAMPAIGNINVITATIONID')
    
    for cid, group in grouped:
        # Get 'to_time'
        if to_condition.strip().lower() == 'client folder':
            to_mask = ~folder_to_clean[group.index].isin(system_folders)
        else:
            to_mask = folder_to_clean[group.index] == to_condition.strip().lower()
        
        to_rows = group[to_mask]
        if not to_rows.empty:
            to_time = to_rows['ACTIVITY_CREATED_AT'].max()
        else:
            continue

        # Get 'from_time'
        if from_condition.strip().lower() == 'any':
            from_mask = folder_from_clean[group.index].isin(['inbox', ''])
            from_rows = group[from_mask]
            from_time = from_rows['ACTIVITY_CREATED_AT'].min() if not from_rows.empty else None
        elif from_condition.strip().lower() == 'client folder':
            from_mask = ~folder_from_clean[group.index].isin(system_folders)
            from_rows = group[from_mask]
            from_time = from_rows['ACTIVITY_CREATED_AT'].min() if not from_rows.empty else None
        elif from_condition.strip().lower() == 'empty':
            from_rows = group[group['FOLDER_FROM_TITLE'].isna()]
            from_time = from_rows['ACTIVITY_CREATED_AT'].min() if not from_rows.empty else None
        else:
            from_mask = folder_from_clean[group.index] == from_condition.strip().lower()
            from_rows = group[from_mask]
            from_time = from_rows['ACTIVITY_CREATED_AT'].min() if not from_rows.empty else None

        # Calculate delta
        if pd.notna(from_time) and pd.notna(to_time):
            delta_days = (to_time - from_time).days
            durations.append(delta_days)

    return f"{np.mean(durations):.1f}" if durations else "N/A"

# Main application
def main():
    # Custom colors for styling
    custom_colors = ["#2F76B9", "#3B9790", "#F5BA2E", "#6A4C93", "#F77F00", "#B4BBBE", "#e6657b", "#026df5", "#5aede2"]

    # Set the main title
    st.title("CANDIDATE PIPELINE CONVERSIONS")

    # Load data with progress indicator
    with st.spinner('Loading and preprocessing data...'):
        cp = load_and_preprocess_data()
        work_locations, campaign_titles, min_date, max_date = get_filter_options(cp)

    # Ensure valid dates before showing date filter
    if pd.isna(min_date) or pd.isna(max_date):
        st.error("No valid INVITATIONDT values available in the data.")
        st.stop()

    # Filters
    st.subheader("Filters")

    start_date, end_date = st.date_input("Select Date Range", [min_date, max_date])

    with st.expander("Select Work Location(s)"):
        selected_worklocations = st.multiselect(
            "Work Location",
            options=work_locations,
            default=None
        )

    with st.expander("Select Campaign Title(s)"):
        selected_campaigns = st.multiselect(
            "Campaign Title",
            options=campaign_titles,
            default=None
        )

    # Filter data efficiently
    with st.spinner('Filtering data...'):
        cp_filtered = filter_data_efficiently(cp, start_date, end_date, selected_worklocations, selected_campaigns)

    # Get total unique campaign invitation IDs for percentage calculation
    total_unique_ids = cp_filtered['CAMPAIGNINVITATIONID'].nunique()

    if total_unique_ids == 0:
        st.warning("No data available for the selected filters.")
        return

    # Define metrics to calculate
    metrics_config = [
        ("Application to Completed", 'Any', 'Completed'),
        ("Application to Passed Prescreening", 'Any', 'Passed MQ'),
        ("Passed Prescreening to Talent Pool", 'Passed MQ', 'Talent Pool'),
        ("Application to Talent Pool", 'Any', 'Talent Pool'),
        ("Application to Client Folder ", 'Any', 'Client Folder'),
        ("Application to Shortlisted", 'Any', 'Shortlisted'),
        ("Application to Hired", 'Any', 'Hired'),
        ("Talent Pool to Client Folder", 'Talent Pool', 'Client Folder'),
        ("Talent Pool to Shortlisted", 'Talent Pool', 'Shortlisted'),
        ("Client Folder to Shortlisted", 'Client Folder', 'Shortlisted'),
        ("Shortlisted to Hired", 'Shortlisted', 'Hired'),
        ("Shortlisted to Rejected", 'Shortlisted', 'Rejected')
    ]

    # Calculate all required metrics with progress bar
    summary_data = []
    progress_bar = st.progress(0)
    
    for i, (title, from_cond, to_cond) in enumerate(metrics_config):
        metric_result = compute_metric_optimized(cp_filtered, title, from_cond, to_cond, total_unique_ids)
        summary_data.append(metric_result)
        progress_bar.progress((i + 1) / len(metrics_config))

    # Create a DataFrame
    summary_df_1 = pd.DataFrame(summary_data)

    # Display summary table
    st.markdown("### Folder Movement Summary")
    st.dataframe(
        summary_df_1.style        
            .applymap(lambda _: 'color: black', subset=pd.IndexSlice[:, ['Count', 'Percentage(%)']])
    )

    # Display data info
    st.sidebar.markdown("### Data Info")
    st.sidebar.write(f"Total Records: {len(cp_filtered):,}")
    st.sidebar.write(f"Unique Campaign IDs: {total_unique_ids:,}")
    st.sidebar.write(f"Date Range: {start_date} to {end_date}")

if __name__ == "__main__":
    main()
