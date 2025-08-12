import time
import json
import gspread
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# --- Streamlitページの基本設定 ---
st.set_page_config(layout="wide", page_title="強化版HubSpot Deals ダッシュボード")

# --- Authentication ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(creds)
except KeyError:
    st.error("Googleサービスアカウントの認証情報が設定されていません。`st.secrets`に`GOOGLE_SERVICE_ACCOUNT`を設定してください。")
    st.stop()
# ハードコードされたスプレッドシートキーを、st.secretsから読み込むように変更
try:
    SPREADSHEET_KEY = st.secrets["SPREADSHEET_KEY"]
except KeyError:
    st.error("Google Sheetsのスプレッドシートキーが設定されていません。`st.secrets`に`SPREADSHEET_KEY`を設定してください。")
    st.stop()

# --- Spreadsheet settings ---
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"

# --- Data fetching function (cached & with retry) ---
@st.cache_data(ttl=300, show_spinner="Google Sheets からデータ取得中...")
def load_data_with_retry(max_retries=3, delay=5):
    """
    Fetches data from Google Sheets and retries if an API rate limit is reached.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            deals_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(DEALS_SHEET)
            stages_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(STAGES_SHEET)
            users_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(USERS_SHEET)

            deals_data = pd.DataFrame(deals_ws.get_all_records())
            stages_data = pd.DataFrame(stages_ws.get("A2:B12"), columns=["Stage ID", "Stage Name"])
            users_data = pd.DataFrame(users_ws.get_all_records())
            return deals_data, stages_data, users_data

        except APIError as e:
            if "429" in str(e):
                st.warning(f"API制限に達しました。{delay}秒待機して再試行します...（{attempt + 1}/{max_retries}）")
                time.sleep(delay)
                attempt += 1
            else:
                st.error(f"Google Sheets API エラー: {e}")
                break

    st.error("Google Sheetsの読み込みに失敗しました。後ほど再試行してください。")
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# --- Load data ---
deals_df, stages_df, users_df = load_data_with_retry()

if deals_df.empty or stages_df.empty or users_df.empty:
    st.stop()

# --- Data preprocessing ---
users_df["Full Name"] = users_df["First Name"].fillna("") + " " + users_df["Last Name"].fillna("")
users_df = users_df.rename(columns={"ID": "User ID"})
deals_df = deals_df.rename(columns={"Deal owner": "User ID", "Deal Stage": "Stage ID"})

# Convert columns to numeric safely
deals_df["User ID"] = pd.to_numeric(deals_df["User ID"], errors="coerce")
deals_df["Stage ID"] = pd.to_numeric(deals_df["Stage ID"], errors="coerce")
stages_df["Stage ID"] = pd.to_numeric(stages_df["Stage ID"], errors="coerce")

deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")
deals_df["受注金額"] = (deals_df["受注金額"] / 10000).fillna(0).astype(int)

# Merge dataframes
merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")

# Convert date columns to datetime objects
date_columns = ['初回商談実施日', '受注日', '受注目標日', '有償ライセンス発行', '概算見積提出日', '報告/提案日','最終見積提出日', 'Create Date']
for col in date_columns:
    if col in merged_df.columns:
        merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce')


# --- Sidebar Filters ---
st.sidebar.header("フィルタ")

# 案件ステータスの選択
deal_status_options = ["すべて"] + list(merged_df["Pipeline (name)"].unique())
selected_deal_status = st.sidebar.selectbox("パイプライン", deal_status_options)
# リードの選択
lead_options = ["すべて"] + list(merged_df["リード経路"].unique())
selected_deal_status = st.sidebar.selectbox("リード", deal_status_options)
# 営業担当者の選択
sales_rep_options = ["すべて"] + list(merged_df["Full Name"].dropna().unique())
selected_sales_reps = st.sidebar.multiselect("営業担当者", sales_rep_options, default=["すべて"])

# 日付範囲の選択
min_date = merged_df["Create Date"].min().date() if not merged_df["Create Date"].isna().all() else datetime.now().date()
max_date = merged_df["Create Date"].max().date() if not merged_df["Create Date"].isna().all() else datetime.now().date()

start_date, end_date = st.sidebar.date_input(
    "日付範囲",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# --- Apply filters ---
filtered_df = merged_df.copy()

if selected_deal_status != "すべて":
    filtered_df = filtered_df[filtered_df["受注/失注"] == selected_deal_status]

if "すべて" not in selected_sales_reps:
    filtered_df = filtered_df[filtered_df["Full Name"].isin(selected_sales_reps)]
# Snapshot_Dateには案件ごとの最新更新日が格納されているので、その日にちでフィルタ
filtered_df = filtered_df[(filtered_df["Snapshot_Date"].dt.date >= start_date) & (filtered_df["Snapshot_Date"].dt.date <= end_date)]

# --- KPI Section ---
def display_kpis(df):#KPIセクション
    st.subheader("主要KPI")
    won_deals_df = df[df['受注/失注'] == '受注'].copy()
    total_won_value = won_deals_df["受注金額"].sum()
    num_won_deals = len(won_deals_df)

    if not won_deals_df.empty:
        # 案件期間の計算（初回商談日〜受注日）
        won_deals_df = won_deals_df.dropna(subset=['初回商談実施日', '受注日'])
        if not won_deals_df.empty:
            won_deals_df['deal_duration'] = (won_deals_df['受注日'] - won_deals_df['初回商談実施日']).dt.days
            avg_deal_duration = won_deals_df['deal_duration'].mean()
        else:
            avg_deal_duration = 0
    else:
        avg_deal_duration = 0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="合計受注金額", value=f"{total_won_value:,} 万円")
    with col2:
        st.metric(label="受注案件数", value=f"{num_won_deals}")
    with col3:
        st.metric(label="平均案件期間", value=f"{int(avg_deal_duration)} 日")


# --- Funnel Chart ---
def create_funnel_chart(df):
    """
    Creates a sales funnel chart from the deals data.
    """
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
    funnel_data = funnel_data.sort_values("Stage Name")

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
    """
    Creates a bar chart for monthly won amounts.
    """
    st.subheader("月別受注金額")
    if df.empty:
        st.info("データがありません。")
        return
    
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
    """
    Creates a pipeline chart for deals from the start of the first negotiation to the closing date.
    """
    st.subheader("案件パイプラインチャート")
    
    # 日付列の欠損値処理と整形
    df_plot = df.copy()
    df_plot = df_plot.dropna(subset=['受注日'])
    df_plot['is_start_date_fallback'] = df_plot['初回商談実施日'].isna()
    df_plot['初回商談実施日'] = df_plot['初回商談実施日'].fillna(df_plot['Create Date'])
    df_plot['案件名'] = df_plot['Deal Name'] + '<br>' + '(' + df_plot['リード経路'] + ')'
    df_plot['Start'] = df_plot['初回商談実施日']
    df_plot['Finish'] = df_plot['受注日']
    df_plot = df_plot.dropna(subset=['Start', 'Finish'])

    if df_plot.empty:
        st.info("プロット可能な案件がありませんでした。")
        return

    df_plot = df_plot.sort_values('Start')
    
    fig = go.Figure()

    for index, row in df_plot.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Start'], row['Finish']],
            y=[row['案件名'], row['案件名']],
            mode='lines',
            line=dict(color='black', width=3),
            showlegend=False,
            hoverinfo='none'
        ))

        marker_color = 'grey' if row['is_start_date_fallback'] else 'blue'
        start_date_label = "案件作成日" if row['is_start_date_fallback'] else "初回商談実施日"
        
        fig.add_trace(go.Scatter(
            x=[row['Start']],
            y=[row['案件名']],
            mode='markers',
            marker=dict(color=marker_color, size=10, symbol='circle'),
            name=f"{row['案件名']} ({start_date_label})",
            showlegend=False,
            hoverinfo='text',
            hovertext=f"案件名: {row['Deal Name']}<br>営業担当:{row['Full Name']}<br>日付: {row['Start'].strftime('%Y-%m-%d')}<br>種別: {start_date_label}"
        ))

        text_label = f"{row['受注金額']:,}万円" if row['受注/失注'] == '受注' else '失注'
        marker_color = 'red' if row['受注/失注'] == '受注' else 'grey'

        fig.add_trace(go.Scatter(
            x=[row['Finish']],
            y=[row['案件名']],
            mode='markers+text',
            marker=dict(color=marker_color, size=10, symbol='circle'),
            text=[text_label],
            textposition="middle right",
            name=f"{row['案件名']} (終了日)",
            showlegend=False,
            hoverinfo='text',
            hovertext=f"案件名: {row['Deal Name']}<br>金額: {text_label}"
        ))
        
        if '報告/提案日' in df_plot.columns and pd.notna(row['報告/提案日']):
            fig.add_trace(go.Scatter(
                x=[row['報告/提案日']],
                y=[row['案件名']],
                mode='markers',
                marker=dict(color='rgba(0, 0, 0, 0)', size=7, symbol='circle', line=dict(color='green', width=2)),
                name=f"{row['案件名']} (報告/提案)",
                showlegend=False,
                hoverinfo='text',
                hovertext=f"報告/提案日: {row['報告/提案日'].strftime('%Y-%m-%d')}"
            ))
        if '概算見積提出日' in df_plot.columns and pd.notna(row['概算見積提出日']):
            fig.add_trace(go.Scatter(
                x=[row['概算見積提出日']],
                y=[row['案件名']],
                mode='markers',
                marker=dict(color='rgba(0, 0, 0, 0)', size=7, symbol='circle', line=dict(color='green', width=2)),
                name=f"{row['案件名']} (概算見積提出日)",
                showlegend=False,
                hoverinfo='text',
                hovertext=f"概算見積提出日: {row['概算見積提出日'].strftime('%Y-%m-%d')}"
            ))

    fig.update_layout(
        title="案件パイプライン（開始日〜終了日）",
        xaxis_title="年月",
        yaxis_title="",
        showlegend=False,
        height=min(800, 400 + 50 * len(df_plot)),
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
st.title("ダッシュボード")

# KPIセクション
display_kpis(filtered_df)

st.divider()

# ファネルチャートとバーチャートを横並びに配置
col1, col2 = st.columns(2)
with col1:
    create_funnel_chart(filtered_df)
with col2:
    create_monthly_bar_chart(filtered_df)

st.divider()

# パイプラインチャート
create_pipeline_chart(filtered_df)
