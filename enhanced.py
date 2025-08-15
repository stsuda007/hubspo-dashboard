import time
import json
import gspread
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# --- Streamlitページの基本設定 ---
st.set_page_config(layout="wide", page_title="HubSpotダッシュボード")

# --- 設定値 ---
# Google Sheetsの設定を辞書にまとめる
CONFIG = {
    "scope": ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"],
    "deals_sheet": "Deals",
    "stages_sheet": "OtherParams",
    "users_sheet": "Users",
    "max_retries": 3,
    "retry_delay": 5
}

# --- Authentication ---
try:
    credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, CONFIG["scope"])
    gc = gspread.authorize(creds)
except KeyError:
    st.error("Googleサービスアカウントの認証情報が設定されていません。`st.secrets`に`GOOGLE_SERVICE_ACCOUNT`を設定してください。")
    st.stop()

try:
    SPREADSHEET_KEY = st.secrets["SPREADSHEET_KEY"]
except KeyError:
    st.error("Google Sheetsのスプレッドシートキーが設定されていません。`st.secrets`に`SPREADSHEET_KEY`を設定してください。")
    st.stop()

# --- Data fetching function (cached & with retry) ---
@st.cache_data(ttl=300, show_spinner="Google Sheets からデータ取得中...")
def load_data_with_retry():
    """
    Fetches data from Google Sheets and retries if an API rate limit is reached.
    """
    attempt = 0
    while attempt < CONFIG["max_retries"]:
        try:
            deals_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(CONFIG["deals_sheet"])
            stages_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(CONFIG["stages_sheet"])
            users_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(CONFIG["users_sheet"])

            deals_data = pd.DataFrame(deals_ws.get_all_records())
            stages_data = pd.DataFrame(stages_ws.get("A2:B23"), columns=["Stage ID", "Stage Name"])
            users_data = pd.DataFrame(users_ws.get_all_records())
            # 新しいファネルマッピングデータを追加
            funnel_mapping_raw = stages_ws.get("E1:H13")
            funnel_mapping = pd.DataFrame(funnel_mapping_raw[1:], columns=funnel_mapping_raw[0])
            return deals_data, stages_data, users_data, funnel_mapping

        except APIError as e:
            if "429" in str(e):
                st.warning(f"API制限に達しました。{CONFIG['retry_delay']}秒待機して再試行します...（{attempt + 1}/{CONFIG['max_retries']}）")
                time.sleep(CONFIG["retry_delay"])
                attempt += 1
            else:
                st.error(f"Google Sheets API エラー: {e}")
                break

    st.error("Google Sheetsの読み込みに失敗しました。後ほど再試行してください。")
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# Load data
deals_df, stages_df, users_df, funnel_mapping_df = load_data_with_retry()

if deals_df.empty or stages_df.empty or users_df.empty or funnel_mapping_df.empty:
    st.stop()

# --- Data preprocessing ---
def preprocess_data(deals, stages, users, funnel_mapping):
    """
    データの前処理を1つの関数にまとめる
    """
    # ユーザーデータの前処理
    users_df = users.copy()
    users_df["Full Name"] = users_df["Last Name"].fillna("") + " " + users_df["First Name"].fillna("")
    users_df = users_df.rename(columns={"ID": "User ID"})
    
    # 案件データの前処理
    deals_df = deals.copy()
    deals_df = deals_df.rename(columns={"Deal owner": "User ID", "Deal Stage (name)": "Stage ID"})

    # 列を数値型に安全に変換
    deals_df["User ID"] = pd.to_numeric(deals_df["User ID"], errors="coerce")
    deals_df["Stage ID"] = pd.to_numeric(deals_df["Stage ID"], errors="coerce")
    stages_df = stages.copy()
    stages_df["Stage ID"] = pd.to_numeric(stages_df["Stage ID"], errors="coerce")

    # 受注金額のクレンジング
    deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")
    #deals_df["受注金額"] = (deals_df["受注金額"] / 10000).fillna(0)

    # データフレームのマージ
    merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
    merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")

    # --- 案件タイプの名寄せ ---
    anken_type_categories = ["New", "Upsell", "Renewal", "Other"]
    def agg_anken_type(val) -> str:
        if pd.isna(val):
            return "Other"
        s = str(val).strip()
        if s in ("CSアカウント", "CS導入サービス"):
            return "Upsell"
        if s == "Partner":
            return "Other"
        sl = s.lower()
        if sl in ("newbusiness", "new business", "new"):
            return "New"
        if sl in ("existingbusiness", "existing business", "upsell", "cross-sell", "cross sell", "expansion"):
            return "Upsell"
        if sl in ("renewal", "renew"):
            return "Renewal"
        if sl in ("partner",):
            return "Other"
        return "Other" #それ以外はOther
    merged_df["Anken Type"] = (
        merged_df["Deal Type"]
        .apply(agg_anken_type)
        .astype(pd.CategoricalDtype(categories=anken_type_categories, ordered=True))
    ) 
    # 日付列をdatetimeオブジェクトに変換
    # 日付列をdatetime（Timestamp）に統一しておくのが重要
    date_columns = [
        '初回商談実施日', '受注日', '受注目標日', '有償ライセンス発行', '概算見積提出日', '報告/提案日',
        '最終見積提出日', 'Create Date', '活動提案アクション', '実施予定日', 'Close Date',
        '現地デモ実施日', '営業引継ぎ日', '撮像/解析完了日', '撮影日', '失注日',
        'Snapshot_date', '治具手配日', '検証_開始日'
    ]
    for col in date_columns:
        if col in merged_df.columns:
            merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce').dt.tz_localize(None)
            
    # Stage ID判定とファネル名称付与の追加
    # ▼ Stage/Funnelの付与：比較は型ブレを避けるため“文字列化”で合わせる
    def determine_stage_and_funnel(row, mapping_df):
        pipeline   = str(row.get('Pipeline', '')).strip()
        deal_stage = str(row.get('Stage ID', '')).strip()  # ← 'Deal Stage' ではなく 'Stage ID'
        # デバッグ用に print を追加
        print(f"Checking row: Pipeline='{pipeline}', Deal Stage='{deal_stage}'")

        # 完全一致（Pipeline & 取引ステージ）
        exact_match = mapping_df[
            (mapping_df['Pipeline'].astype(str).str.strip() == pipeline) &
            (mapping_df['取引ステージ'].astype(str).str.strip() == deal_stage)
        ]
        if not exact_match.empty:
            return exact_match.iloc[0]['Stage ID'], exact_match.iloc[0]['ファネル名称']

        # 取引ステージが空欄時はPipelineのみで
        pipeline_match = mapping_df[
            (mapping_df['Pipeline'].astype(str).str.strip() == pipeline) &
            (mapping_df['取引ステージ'].astype(str).str.strip() == '')
        ]
        if not pipeline_match.empty:
            return pipeline_match.iloc[0]['Stage ID'], pipeline_match.iloc[0]['ファネル名称']

        return (None, None)
    
    # 各行にStage IDとファネル名称を付与
    funnel_results = merged_df.apply(lambda row: determine_stage_and_funnel(row, funnel_mapping), axis=1)
    merged_df['Funnel_Stage_ID'] = [result[0] for result in funnel_results]
    merged_df['Funnel_Name'] = [result[1] for result in funnel_results]
    
    return merged_df, stages_df, funnel_mapping

## updated merged_df, stages_df = preprocess_data(deals_df, stages_df, users_df)
merged_df, stages_df, funnel_mapping_df = preprocess_data(deals_df, stages_df, users_df, funnel_mapping_df)

# --- Helper function for dynamic date ranges　年度計算 fiscal_start_monthは年度始まりの月 ---
def get_fiscal_dates(today, fiscal_start_month=1):
    """
    Calculates the start and end dates for the current fiscal year and half-year
    using standard datetime and timedelta libraries.
    """
    current_year = today.year
    current_month = today.month

    # --- Calculate fiscal year dates ---
    if current_month >= fiscal_start_month:
        fiscal_year_start = datetime(current_year, fiscal_start_month, 1).date()
        fiscal_year_end = datetime(current_year + 1, fiscal_start_month, 1).date() - timedelta(days=1)
    else:
        fiscal_year_start = datetime(current_year - 1, fiscal_start_month, 1).date()
        fiscal_year_end = datetime(current_year, fiscal_start_month, 1).date() - timedelta(days=1)

    # --- Calculate fiscal half-year dates ---
    # Determine the half-year's start month and year
    if current_month >= fiscal_start_month and current_month < fiscal_start_month + 6:
        # First half of the fiscal year
        half_year_start = fiscal_year_start
    else:
        # Second half of the fiscal year
        start_month_h2 = fiscal_start_month + 6
        if start_month_h2 > 12:
            start_month_h2 = start_month_h2 % 12
            start_year_h2 = fiscal_year_start.year + 1
        else:
            start_year_h2 = fiscal_year_start.year
            
        half_year_start = datetime(start_year_h2, start_month_h2, 1).date()

    # Determine the half-year's end month and year
    end_month = half_year_start.month + 6
    end_year = half_year_start.year

    if end_month > 12:
        end_month = end_month % 12
        end_year += 1
    
    half_year_end = datetime(end_year, end_month, 1).date() - timedelta(days=1)

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

# 案件タイプの選択

new_upsell = ["すべて"] + list(merged_df["Anken Type"].dropna().unique())
selected_new_upsell = st.sidebar.selectbox("案件タイプ", new_upsell)

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
fiscal_year_start, fiscal_year_end, half_year_start, half_year_end = get_fiscal_dates(today) #年度の計算
date_col = 'Snapshot_date'
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

if selected_new_upsell != "すべて":
    filtered_df = filtered_df[filtered_df["Anken Type"] == selected_new_upsell]

if "すべて" not in selected_sales_reps:
    filtered_df = filtered_df[filtered_df["Full Name"].isin(selected_sales_reps)]

filtered_df = filtered_df[(filtered_df[date_col].dt.date >= start_date) & (filtered_df[date_col].dt.date <= end_date)]
# Snapshot_date が存在しないケースに備えてフォールバック
date_col = 'Snapshot_date' if 'Snapshot_date' in filtered_df.columns else 'Create Date'
filtered_df = filtered_df[
    (filtered_df[date_col].dt.date >= start_date) & (filtered_df[date_col].dt.date <= end_date)
]




# --- KPI Section ---
def display_kpis(df):
    st.subheader("主要KPI")
    st.markdown(f"**日付範囲:** {start_date.strftime('%Y/%m/%d')} ~ {end_date.strftime('%Y/%m/%d')}")
    won_deals_df = df[df['受注/失注'] == '受注'].copy()
    
    total_won_value = won_deals_df["受注金額"].sum() if not won_deals_df.empty else 0
    num_won_deals = len(won_deals_df)

    avg_deal_duration = 0
    if not won_deals_df.empty:
        won_deals_df = won_deals_df.dropna(subset=['初回商談実施日', '受注日'])
        if not won_deals_df.empty:
            won_deals_df['deal_duration'] = (won_deals_df['受注日'] - won_deals_df['初回商談実施日']).dt.days
            avg_deal_duration = won_deals_df['deal_duration'].mean()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="合計受注金額", value=f"{total_won_value:,.0f} 万円")
    with col2:
        st.metric(label="受注案件数", value=f"{num_won_deals}")
    with col3:
        st.metric(label="平均案件期間", value=f"{avg_deal_duration:,.0f} 日")


# --- Funnel Chart ---
def create_funnel_chart(df, funnel_mapping_df):
    st.subheader("案件ステージ別ファネルチャート")
    if df.empty:
        st.info("データがありません。")
        return

    # 並び順は Stage ID の数値昇順に（型を数値化しておく）
    fm = funnel_mapping_df.drop_duplicates('ファネル名称').copy()
    fm['Stage ID'] = pd.to_numeric(fm['Stage ID'], errors='coerce')
    stage_order = fm.sort_values('Stage ID')['ファネル名称'].tolist()
    
    # ファネル名称でグループ化してカウント
    funnel_data = df["Funnel_Name"].dropna().value_counts().reset_index()
    funnel_data.columns = ["Funnel_Name", "Count"]

    # Stage IDによる順序付け
    stage_order = funnel_mapping_df.drop_duplicates('ファネル名称').sort_values('Stage ID')['ファネル名称'].tolist()
    funnel_data['Funnel_Name'] = pd.Categorical(funnel_data['Funnel_Name'], categories=stage_order, ordered=True)
    funnel_data = funnel_data.sort_values("Funnel_Name", ascending=False)
    
    # ファネルチャート作成（既存のコードと同様）
    fig = go.Figure(go.Funnel(
        y = funnel_data["Funnel_Name"],
        x = funnel_data["Count"],
        textinfo = "value+percent initial",
        marker = {"color": ["deepskyblue", "lightseagreen", "cadetblue", "teal", "dodgerblue", "steelblue", "skyblue", "powderblue", "lightblue", "lightsteelblue"]}
    ))
    fig.update_layout(height=500, width=800, margin=dict(t=0, b=0, l=0, r=0))
    st.plotly_chart(fig, use_container_width=True)
    
def create_funnel_chart_obsolete(df, stages_df):
    st.subheader("案件ステージ別ファネルチャート")
    if df.empty:
        st.info("データがありません。")
        return

    # Deal Stageでグループ化してカウント
    funnel_data = df["Stage Name"].value_counts().reset_index()
    funnel_data.columns = ["Stage Name", "Count"]

    # 適切な順序に並び替え
    stage_order = stages_df["Stage Name"].tolist()
    funnel_data['Stage Name'] = pd.Categorical(funnel_data['Stage Name'], categories=stage_order, ordered=True)
    funnel_data = funnel_data.sort_values("Stage Name", ascending=False)
    
    fig = go.Figure(go.Funnel(
        y = funnel_data["Stage Name"],
        x = funnel_data["Count"],
        textinfo = "value+percent initial",
        marker = {"color": ["deepskyblue", "lightseagreen", "cadetblue", "teal", "dodgerblue", "steelblue", "skyblue", "powderblue", "lightblue", "lightsteelblue"]}
    ))
    fig.update_layout(height=500, width=800, margin=dict(t=0, b=0, l=0, r=0))
    st.plotly_chart(fig, use_container_width=True)


# --- Monthly Won Amount Bar Chart ---
def create_monthly_bar_chart(df):
    st.subheader("月別受注金額")
    won_deals_df = df[df['受注/失注'] == '受注'].copy()

    if won_deals_df.empty:
        st.info("受注案件データがありません。")
        return

    won_deals_df['受注月'] = won_deals_df['受注日'].dt.to_period('M').astype(str)
    monthly_won = won_deals_df.groupby('受注月')['受注金額'].sum().reset_index()

    fig = px.bar(
        monthly_won,
        x="受注月",
        y="受注金額",
        title="月別受注金額の推移",
        labels={"受注金額": "受注金額 (万円)"},
        color_discrete_sequence=px.colors.qualitative.Plotly
    )
    fig.update_layout(xaxis_title="年月", yaxis_title="受注金額 (万円)", xaxis={'categoryorder':'category ascending'})
    st.plotly_chart(fig, use_container_width=True)


# --- Function to create the deals pipeline chart (updated) ---
def create_pipeline_chart(df):
    st.subheader("案件パイプラインチャート")
    df_plot = df.copy()
    
    # 欠損値処理
    df_plot = df_plot.dropna(subset=['受注日'])
    
    # Ensure 'Create Date' is of the same type as '初回商談実施日'
    # Convert Timestamp objects to date objects
    df_plot['Create Date'] = pd.to_datetime(df_plot['Create Date']).dt.date
    
    # 案件開始日の代替処理
    df_plot['is_start_date_fallback'] = df_plot['初回商談実施日'].isna()
    df_plot['初回商談実施日'] = df_plot['初回商談実施日'].fillna(df_plot['Create Date'])
    df_plot['案件名'] = df_plot['Deal Name'] + '<br>' + '(' + df_plot['リード経路'].fillna('不明') + ')'
    df_plot['Start'] = df_plot['初回商談実施日']
    df_plot['Finish'] = df_plot['受注日']
    df_plot = df_plot.dropna(subset=['Start', 'Finish'])

    if df_plot.empty:
        st.info("プロット可能な案件がありませんでした。")
        return

    # Now all values in the 'Start' column are of the same type,
    # so sorting will work without a TypeError.
    df_plot = df_plot.sort_values('Start', ascending=False)
    
    fig = go.Figure()

    for _, row in df_plot.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Start'], row['Finish']],
            y=[row['案件名'], row['案件名']],
            mode='lines',
            line=dict(color='black', width=3),
            showlegend=False,
            hoverinfo='none'
        ))

        marker_color_start = 'gray' if row['is_start_date_fallback'] else 'blue'
        start_date_label = "案件作成日" if row['is_start_date_fallback'] else "初回商談実施日"
        
        fig.add_trace(go.Scatter(
            x=[row['Start']],
            y=[row['案件名']],
            mode='markers',
            marker=dict(color=marker_color_start, size=10, symbol='circle'),
            name=f"{row['Deal Name']} (開始)",
            showlegend=False,
            hoverinfo='text',
            hovertext=f"案件名: {row['Deal Name']}<br>営業担当:{row['Full Name']}<br>日付: {row['Start'].strftime('%Y-%m-%d')}<br>種別: {start_date_label}"
        ))

        text_label = f"{row['受注金額']:,.0f}万円" if row['受注/失注'] == '受注' and pd.notna(row['受注金額']) else '失注'
        marker_color_end = 'red' if row['受注/失注'] == '受注' else 'gray'

        fig.add_trace(go.Scatter(
            x=[row['Finish']],
            y=[row['案件名']],
            mode='markers+text',
            marker=dict(color=marker_color_end, size=10, symbol='circle'),
            text=[text_label],
            textposition="middle right",
            name=f"{row['Deal Name']} (終了)",
            showlegend=False,
            hoverinfo='text',
            hovertext=f"案件名: {row['Deal Name']}<br>金額: {text_label}"
        ))
            
        for mid_col, mid_label, mid_color, mid_symbol in [
            ('報告/提案日', '報告/提案日', 'green', 'diamond'),
            ('概算見積提出日', '概算見積提出日', 'purple', 'diamond')
        ]:
            if mid_col in df_plot.columns and pd.notna(row[mid_col]):
                fig.add_trace(go.Scatter(
                    x=[row[mid_col]],
                    y=[row['案件名']],
                    mode='markers',
                    marker=dict(color=mid_color, size=7, symbol=mid_symbol),
                    name=f"{row['Deal Name']} ({mid_label})",
                    showlegend=False,
                    hoverinfo='text',
                    hovertext=f"{mid_label}: {row[mid_col].strftime('%Y-%m-%d')}"
                ))

    fig.update_layout(
        title="案件パイプライン（開始日〜終了日）",
        xaxis_title="年月",
        yaxis_title="",
        showlegend=False,
        height=min(1200, 200 + 50 * len(df_plot)),
        xaxis=dict(
            range=[start_date, end_date],
            tickmode="linear",
            dtick="M3",
            tickformat="%Y-%m",
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128,128,128,0.5)'
        ),
        yaxis=dict(automargin=True)
    )

    st.plotly_chart(fig, use_container_width=True)

# --- Main app layout ---
st.title("HubSpot Deals ダッシュボード")

# KPIセクション
display_kpis(filtered_df)

st.divider()

# ファネルチャートとバーチャートを横並びに配置
col1, col2 = st.columns(2)
with col1:
    create_funnel_chart(filtered_df, funnel_mapping_df)
with col2:
    create_monthly_bar_chart(filtered_df)
st.write("Funnel_Name 列のユニークな値:", filtered_df["Funnel_Name"].dropna().unique())
st.divider()

# パイプラインチャート
create_pipeline_chart(filtered_df)
