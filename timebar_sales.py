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

# --- 認証 ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(creds)
except KeyError:
    st.error("Googleサービスアカウントの認証情報が設定されていません。`st.secrets`に`GOOGLE_SERVICE_ACCOUNT`を設定してください。")
    st.stop()


# --- スプレッドシート設定 ---
SPREADSHEET_KEY = "1Ra_tPm2u5K4ikxacw1vdQqY_YQg-JekMsM-ZhaaVFKg"
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"

# --- データ取得関数（キャッシュ & リトライ） ---
@st.cache_data(ttl=300, show_spinner="Google Sheets からデータ取得中...")
def load_data_with_retry(max_retries=3, delay=5):
    """
    Google Sheetsからデータを取得し、API制限に達した場合にリトライする関数。
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

# --- データ読み込み ---
deals_df, stages_df, users_df = load_data_with_retry()

if deals_df.empty:
    st.stop()

# --- IDを人名・ステージ名に変換 ---
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


# --- 新規追加：案件パイプラインの受注チャート作成関数 ---
def pipeline_chart_juchu(df):
    """
    案件の中で「受注」したものに絞り込み、初回商談日から受注日までのパイプラインチャートを作成します。
    """
    st.title("HubSpot Deals ダッシュボード")
    st.subheader("受注案件のパイプラインチャート")

    # データフィルタリング
    # 「受注/失注」が「受注」のものだけをピックアップする
    df_filtered = df[(df['受注/失注'] == '受注')]

    # 日付列をdatetime型に変換
    date_columns = ['初回商談実施日', '受注日', '受注目標日', 'その他日付'] # 'その他日付'は仮の列名
    for col in date_columns:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce')

    # df_filteredから無効なデータやNaNを除外
    df_filtered = df_filtered.dropna(subset=['初回商談実施日', '受注日', '受注目標日'])
    
    if df_filtered.empty:
        st.info("条件に一致する受注案件がありませんでした。")
        return

    # プロット用のデータフレームを作成
    df_plot = df_filtered.copy()
    df_plot['案件名'] = df_plot['Deal Name']
    df_plot['Start'] = df_plot['初回商談実施日']
    df_plot['Finish'] = df_plot['受注日']
    df_plot = df_plot.sort_values('Start')

    # PlotlyのGanttチャートを作成
    fig = go.Figure()

    # 各案件のバーを追加
    for index, row in df_plot.iterrows():
        fig.add_trace(go.Bar(
            y=[row['案件名']],
            x=[row['Finish']],
            base=[row['Start']],
            orientation='h',
            marker=dict(color='lightgray', line=dict(color='darkgray', width=1)),
            name=row['案件名'],
            hoverinfo='text',
            hovertext=f"案件名: {row['案件名']}<br>開始日: {row['Start'].strftime('%Y-%m-%d')}<br>終了日: {row['Finish'].strftime('%Y-%m-%d')}<br>金額: {row['受注金額']}万円"
        ))
        
        # 初回商談実施日(開始)にマーカーを追加
        fig.add_trace(go.Scatter(
            x=[row['Start']],
            y=[row['案件名']],
            mode='markers+text',
            marker=dict(color='blue', size=10, symbol='circle'),
            text=[f"{row['受注金額']:,}万円"],
            textposition="middle right",
            name=f"{row['案件名']} (初回商談)",
            showlegend=False
        ))

        # 受注日(終了)にマーカーと金額を追加
        fig.add_trace(go.Scatter(
            x=[row['Finish']],
            y=[row['案件名']],
            mode='markers+text',
            marker=dict(color='red', size=10, symbol='circle'),
            text=[f"{row['受注金額']:,}万円"],
            textposition="middle right",
            name=f"{row['案件名']} (受注日)",
            showlegend=False
        ))
        
        # その他の日付（もしあれば）にもマーカーを追加
        if 'その他日付' in df_plot.columns and pd.notna(row['その他日付']):
            fig.add_trace(go.Scatter(
                x=[row['その他日付']],
                y=[row['案件名']],
                mode='markers',
                marker=dict(color='green', size=10, symbol='circle'),
                name=f"{row['案件名']} (その他)",
                showlegend=False
            ))

    fig.update_layout(
        title="受注案件のパイプライン（初回商談日〜受注日）",
        xaxis_title="年月",
        yaxis_title="",
        showlegend=False,
        barmode='stack',
        height=400 + 50 * len(df_plot),
        xaxis=dict(
            range=[datetime(2024, 1, 1), datetime(2025, 12, 31)],
            tickmode="linear",
            dtick="M1",
            tickformat="%Y-%m"
        )
    )

    st.plotly_chart(fig, use_container_width=True)

# アプリのメイン実行部分
pipeline_chart_juchu(merged_df)
