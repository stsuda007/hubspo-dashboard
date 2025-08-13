# --- Helper function for dynamic date ranges ---
def get_fiscal_dates(today, fiscal_start_month=4):
    """
    Calculates the start and end dates for the current fiscal year and half-year.
    Assumes a fiscal year starting in April.
    """
    
    current_year = today.year
    current_month = today.month

    # Calculate fiscal year dates
    if current_month >= fiscal_start_month:
        fiscal_year_start = datetime(current_year, fiscal_start_month, 1).date()
        fiscal_year_end = datetime(current_year + 1, fiscal_start_month, 1).date() - timedelta(days=1)
    else:
        fiscal_year_start = datetime(current_year - 1, fiscal_start_month, 1).date()
        fiscal_year_end = datetime(current_year, fiscal_start_month, 1).date() - timedelta(days=1)

    # Calculate fiscal half-year dates (H1: Apr-Sep, H2: Oct-Mar)
    if current_month >= fiscal_start_month and current_month < fiscal_start_month + 6:
        half_year_start = datetime(current_year, fiscal_start_month, 1).date()
        half_year_end = datetime(current_year, fiscal_start_month + 6, 1).date() - timedelta(days=1)
    else:
        # This handles H2 of the current fiscal year, or H1 of the next fiscal year if we're in Q1
        half_year_start = datetime(current_year, fiscal_start_month + 6, 1).date()
        if fiscal_start_month + 6 > 12: # Check if the half year rolls into the next calendar year
            half_year_start = datetime(current_year + 1, (fiscal_start_month + 6) % 12, 1).date()
        
        # End date logic needs to be careful about year boundaries
        if half_year_start.month < fiscal_start_month: # e.g. Oct -> Mar
            half_year_end = datetime(half_year_start.year + 1, half_year_start.month - 6, 1).date() - timedelta(days=1)
        else:
            half_year_end = datetime(half_year_start.year, half_year_start.month + 6, 1).date() - timedelta(days=1)


    return fiscal_year_start, fiscal_year_end, half_year_start, half_year_end


# --- Sidebar Filters ---
st.sidebar.header("フィルタ")

# 案件ステータスの選択
if '受注/失注' in merged_df.columns:
    deal_status_options = ["すべて"] + list(merged_df["受注/失注"].dropna().unique())
    selected_deal_status = st.sidebar.selectbox("受注/失注", deal_status_options)
else:
    selected_deal_status = "すべて"

# リードの選択
lead_options = ["すべて"] + list(merged_df["リード経路"].dropna().unique())
selected_lead_path = st.sidebar.selectbox("リード経路", lead_options)

# 営業担当者の選択
sales_rep_options = ["すべて"] + list(merged_df["Full Name"].dropna().unique())
selected_sales_reps = st.sidebar.multiselect("営業担当者", sales_rep_options, default=["すべて"])

# 日付範囲の選択
# Filter by a date range preset
date_filter_preset = st.sidebar.radio(
    "日付範囲のプリセット",
    ("カスタム", "今半期", "今年度", "全期間")
)

# Get today's date for calculations
today = datetime.now().date()

# Calculate the dynamic dates
# We need to import timedelta from datetime for this to work
from datetime import timedelta
fiscal_year_start, fiscal_year_end, half_year_start, half_year_end = get_fiscal_dates(today)

min_date_val = merged_df[date_col].min().date() if not merged_df[date_col].isna().all() else today
max_date_val = merged_df[date_col].max().date() if not merged_df[date_col].isna().all() else today

# Set start and end dates based on the preset
if date_filter_preset == "今半期":
    start_date = half_year_start
    end_date = half_year_end
elif date_filter_preset == "今年度":
    start_date = fiscal_year_start
    end_date = fiscal_year_end
elif date_filter_preset == "全期間":
    start_date = min_date_val
    end_date = max_date_val
else: # "カスタム"
    # Show the date input only for custom range
    start_date, end_date = st.sidebar.date_input(
        "カスタム日付範囲",
        value=(min_date_val, max_date_val),
        min_value=min_date_val,
        max_value=max_date_val
    )

# --- Apply filters ---
filtered_df = merged_df.copy()

if selected_deal_status != "すべて":
    filtered_df = filtered_df[filtered_df["受注/失注"] == selected_deal_status]

if selected_lead_path != "すべて":
    filtered_df = filtered_df[filtered_df["リード経路"] == selected_lead_path]

if "すべて" not in selected_sales_reps:
    filtered_df = filtered_df[filtered_df["Full Name"].isin(selected_sales_reps)]

filtered_df = filtered_df[(filtered_df[date_col].dt.date >= start_date) & (filtered_df[date_col].dt.date <= end_date)]
