import time
import json
import gspread
import pandas as pd
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

    # Create the Plotly Gantt chart
    fig = go.Figure()

    # Add markers and connecting lines
    for index, row in df_plot.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Start'], row['Finish']],
            y=[row['案件名'], row['案件名']],
            mode='lines',
            line=dict(color='black', width=3),
            showlegend=False,
            hoverinfo='none'
        ))

        marker_color = 'grey' if pd.isna(row['初回商談実施日']) else 'blue'
        start_date_label = "案件作成日" if pd.isna(row['初回商談実施日']) else "初回商談実施日"
        
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

        fig.add_trace(go.Scatter(
            x=[row['Finish']],
            y=[row['案件名']],
            mode='markers+text',
            marker=dict(color='red', size=10, symbol='circle'),
            text=[f"{row['受注金額']:,}万円"],
            textposition="middle right",
            name=f"{row['案件名']} (受注日)",
            showlegend=False,
            hoverinfo='text',
            hovertext=f"案件名: {row['Deal Name']}<br>金額: {row['受注金額']:,}万円"
        ))

        # Report/Proposal date markers
        if pd.notna(row.get('報告/提案日')):
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

    fig.update_layout(
        title="受注案件のパイプライン（初回商談日〜受注日）",
        xaxis_title="年月",
        yaxis_title="",
        showlegend=False,
        height=400 + 50 * len(df_plot),
        xaxis=dict(
            range=[datetime(2024, 1, 1), datetime(2025, 12, 31)],
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
