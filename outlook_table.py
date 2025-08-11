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

# --- Authentication ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(creds)
except KeyError:
    st.error("Googleサービスアカウントの認証情報が設定されていません。`st.secrets`に`GOOGLE_SERVICE_ACCOUNT`を設定してください。")
    st.stop()

# --- Spreadsheet settings ---
SPREADSHEET_KEY = "1Ra_tPm2u5K4ikxacw1vdQqY_YQg-JekMsM-ZhaaVFKg"
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"

# --- Data fetching function (cached & with retry) ---
@st.cache_data(ttl=300, show_spinner="Google Sheets からデータ取得中...")
def load_data_with_retry(max_retries=3, delay=5):
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

if deals_df.empty:
    st.stop()

# --- Convert IDs to names ---
users_df["Full Name"] = users_df["First Name"].fillna("") + " " + users_df["Last Name"].fillna("")
users_df = users_df.rename(columns={"ID": "User ID"})
deals_df = deals_df.rename(columns={"Deal owner": "User ID", "Deal Stage": "Stage ID"})

# Convert columns to numeric safely
deals_df["User ID"] = pd.to_numeric(deals_df["User ID"], errors="coerce")
deals_df["Stage ID"] = pd.to_numeric(deals_df["Stage ID"], errors="coerce")
stages_df["Stage ID"] = pd.to_numeric(stages_df["Stage ID"], errors="coerce")

# Clean up non-numeric characters in deal amount and convert to numeric
deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")

# Adjust deal amount for better readability
deals_df["受注金額"] = (deals_df["受注金額"] / 10000).fillna(0).astype(int)

merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")

# --- Function to create the deals pipeline chart ---
def pipeline_chart_juchu(df):
    st.title("HubSpot Deals ダッシュボード")
    st.subheader("受注案件のパイプラインチャート")
    st.write("元のデータ数:", len(df))

    # Filter for '受注' (won) deals
    df_filtered = df[df['受注/失注'] == '受注'].copy()
    st.write("受注フラグのデータ数:", len(df_filtered))

    # Convert date columns to datetime
    date_columns = ['初回商談実施日', '受注日', '受注目標日', '有償ライセンス発行', '概算見積提出日', '報告/提案日','最終見積提出日', 'Create Date']
    for col in date_columns:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce')

    # Remove deals with no '受注日'
    df_filtered = df_filtered.dropna(subset=['受注日'])
    st.write("受注日不記載のデータを削除しました。データ数:", len(df_filtered))
    
    # Fill missing '初回商談実施日' with 'Create Date'
    df_filtered['初回商談実施日'] = df_filtered['初回商談実施日'].fillna(df_filtered['Create Date'])
    
    if df_filtered.empty:
        st.info("条件に一致する受注案件がありませんでした。")
        return

    df_plot = df_filtered.copy()
    df_plot['案件名'] = df_plot['Deal Name'] + '<br>' + '(' + df_plot['リード経路'] + ')'
    df_plot['Start'] = df_plot['初回商談実施日']
    df_plot['Finish'] = df_plot['受注日']

    df_plot = df_plot.dropna(subset=['Start', 'Finish'])
    st.write("最終的なグラフ表示データ数:", len(df_plot))

    if df_plot.empty:
        st.info("プロット可能な受注案件がありませんでした。")
        return

    df_plot = df_plot.sort_values('Start')

    # Ensure the x_start and x_end columns are datetime
    df_plot['Start'] = pd.to_datetime(df_plot['Start'], errors='coerce')
    df_plot['Finish'] = pd.to_datetime(df_plot['Finish'], errors='coerce')

    # Check for any NaT (Not a Time) values after conversion
    if df_plot['Start'].isna().any() or df_plot['Finish'].isna().any():
        st.error("Some dates in 'Start' or 'Finish' are invalid or missing.")
        st.stop()

    # Create the Plotly Gantt chart
    fig = px.timeline(
        df_plot,
        x_start="Start",
        x_end="Finish",
        y="案件名",  # Adjust as per your column name for the task
        color="営業担当者",  # Adjust as per your column name for the owner
        title="受注案件のパイプライン"
    )

    fig.update_layout(
        xaxis_title="日時",
        yaxis_title="案件名",
        showlegend=True,
        height=400
    )

    st.plotly_chart(fig)

# --- NEW: Pipeline Projects Table Function ---
def table_of_pipeline_projects(df):
    st.subheader("📊 パイプライン案件一覧")
    
    # Convert date columns
    for col in ['受注目標日', '納品予定日']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Filter for pipeline condition
    df_pipeline = df[df['受注目標日'].notna() | df['納品予定日'].notna()]

    if df_pipeline.empty:
        st.info("受注目標日または納品予定日が記載されている案件がありません。")
        return

    # Display the filtered DataFrame
    display_df = df_pipeline.copy()
    display_df = display_df.rename(columns={'Full Name': '営業担当者', 'Deal Name': '案件名', '受注金額': '見込売上額（万円）'})
    
    # Format date columns
    display_df['予定日'] = display_df.apply(lambda row: f"受注目標: {row['受注目標日'].strftime('%Y-%m-%d')}" if pd.notna(row['受注目標日']) else "", axis=1)

    # Sorting
    display_df = display_df.sort_values(by=['営業担当者', '見込売上額（万円）'], ascending=[True, False])

    st.dataframe(display_df)

# --- MAIN APPLICATION ---
pipeline_chart_juchu(merged_df)
st.divider()
table_of_pipeline_projects(merged_df)
