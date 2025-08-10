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
deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")

merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")


# --- Function to create the deals pipeline chart ---
def pipeline_chart_juchu(df):
    """
    Creates a pipeline chart for '受注' (won) deals from the start of the first negotiation to the closing date.
    """
    st.title("HubSpot Deals ダッシュボード")
    st.subheader("受注案件のパイプラインチャート")

    # Filter data for '受注' (won) deals only, or where '受注日' is not empty
    df_filtered = df[(df['受注/失注'] == '受注') | (df['受注日'].notna())]

    # Convert date columns to datetime objects
    date_columns = ['初回商談実施日', '受注日', '受注目標日', 'その他日付', '報告/提案日']
    for col in date_columns:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce')

    # Remove invalid or NaN data
    df_filtered = df_filtered.dropna(subset=['初回商談実施日', '受注日'])
    
    if df_filtered.empty:
        st.info("条件に一致する受注案件がありませんでした。")
        return

    # Create a DataFrame for plotting
    df_plot = df_filtered.copy()
    # 案件名にリード経路を追加
    df_plot['案件名'] = df_plot['Deal Name'] + '<br>' + '(' + df_plot['リード経路'] + ')'
    df_plot['Start'] = df_plot['初回商談実施日']
    df_plot['Finish'] = df_plot['受注日']
    df_plot = df_plot.sort_values('Start')

    # Create the Plotly Gantt chart
    fig = go.Figure()

    # Add markers and connecting lines for each deal
    for index, row in df_plot.iterrows():
        # Add a line connecting the start and end points
        fig.add_trace(go.Scatter(
            x=[row['Start'], row['Finish']],
            y=[row['案件名'], row['案件名']],
            mode='lines',
            line=dict(color='black', width=3), # 3ptの太い黒線
            showlegend=False,
            hoverinfo='none' # Line itself doesn't need hover info
        ))

        # Add a marker for the start date (blue circle)
        fig.add_trace(go.Scatter(
            x=[row['Start']],
            y=[row['案件名']],
            mode='markers',
            marker=dict(color='blue', size=10, symbol='circle'),
            name=f"{row['案件名']} (初回商談)",
            showlegend=False,
            hoverinfo='text',
            hovertext=f"案件名: {row['Deal Name']}<br>リード経路: {row['リード経路']}<br>開始日: {row['Start'].strftime('%Y-%m-%d')}<br>金額: {row['受注金額']:,}万円"
        ))

        # Add a marker for the end date (red circle) with text for the amount
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
            hovertext=f"案件名: {row['Deal Name']}<br>リード経路: {row['リード経路']}<br>終了日: {row['Finish'].strftime('%Y-%m-%d')}<br>金額: {row['受注金額']:,}万円"
        ))
        
        # Add markers for '報告/提案日' (if they exist)
        if '報告/提案日' in df_plot.columns and pd.notna(row['報告/提案日']):
            fig.add_trace(go.Scatter(
                x=[row['報告/提案日']],
                y=[row['案件名']],
                mode='markers',
                marker=dict(color='green', size=10, symbol='circle'),
                name=f"{row['案件名']} (報告/提案)",
                showlegend=False,
                hoverinfo='text',
                hovertext=f"案件名: {row['Deal Name']}<br>リード経路: {row['リード経路']}<br>報告/提案日: {row['報告/提案日'].strftime('%Y-%m-%d')}"
            ))

    fig.update_layout(
        title="受注案件のパイプライン（初回商談日〜受注日）",
        xaxis_title="年月",
        yaxis_title="",
        showlegend=False,
        # グラフの高さを動的に調整
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
        # Y軸の文字を2行に折り返すように設定
        yaxis=dict(automargin=True)
    )

    st.plotly_chart(fig, use_container_width=True)

# Main part of the app
pipeline_chart_juchu(merged_df)
