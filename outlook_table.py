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

# --- Constants and Configuration ---
SPREADSHEET_KEY = "1Ra_tPm2u5K4ikxacw1vdQqY_YQg-JekMsM-ZhaaVFKg"
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

# --- Authentication ---
@st.cache_resource
def authenticate_gspread():
    """
    Authenticates with Google Sheets API and returns the client.
    """
    try:
        credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, SCOPE)
        return gspread.authorize(creds)
    except KeyError:
        st.error("Googleサービスアカウントの認証情報が設定されていません。`st.secrets`に`GOOGLE_SERVICE_ACCOUNT`を設定してください。")
        st.stop()
    except Exception as e:
        st.error(f"認証中にエラーが発生しました: {e}")
        st.stop()

gc = authenticate_gspread()

# --- Data fetching function (cached & with retry) ---
@st.cache_data(ttl=300, show_spinner="Google Sheets からデータ取得中...")
def load_raw_data(max_retries=RETRY_ATTEMPTS, delay=RETRY_DELAY):
    """
    Fetches raw data from Google Sheets with retry logic.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            sh = gc.open_by_key(SPREADSHEET_KEY)
            deals_data = pd.DataFrame(sh.worksheet(DEALS_SHEET).get_all_records())
            stages_data = pd.DataFrame(sh.worksheet(STAGES_SHEET).get("A2:B12"), columns=["Stage ID", "Stage Name"])
            users_data = pd.DataFrame(sh.worksheet(USERS_SHEET).get_all_records())
            return deals_data, stages_data, users_data
        except APIError as e:
            if "429" in str(e):
                st.warning(f"API制限に達しました。{delay}秒待機して再試行します...（{attempt + 1}/{max_retries}）")
                time.sleep(delay)
                attempt += 1
            else:
                st.error(f"Google Sheets API エラー: {e}")
                st.stop()
        except gspread.exceptions.WorksheetNotFound as e:
            st.error(f"指定されたシートが見つかりません: {e}")
            st.stop()

    st.error("Google Sheetsの読み込みに失敗しました。後ほど再試行してください。")
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# --- Data preprocessing function ---
def preprocess_data(deals_df, stages_df, users_df):
    """
    Cleans and merges dataframes for visualization.
    """
    # ユーザー名とステージ名を結合
    users_df["Full Name"] = users_df["First Name"].fillna("") + " " + users_df["Last Name"].fillna("")
    users_df = users_df.rename(columns={"ID": "User ID"})
    deals_df = deals_df.rename(columns={"Deal owner": "User ID", "Deal Stage": "Stage ID"})

    # データ型の変換
    deals_df["User ID"] = pd.to_numeric(deals_df["User ID"], errors="coerce").astype('Int64')
    deals_df["Stage ID"] = pd.to_numeric(deals_df["Stage ID"], errors="coerce").astype('Int64')
    stages_df["Stage ID"] = pd.to_numeric(stages_df["Stage ID"], errors="coerce").astype('Int64')

    # 受注金額のクリーニングと変換
    deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce").fillna(0)
    deals_df["受注金額"] = (deals_df["受注金額"] / 10000).astype(int)

    # DataFrameの結合
    merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
    merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")

    # 日付列を datetime 型に変換
    date_columns = ['初回商談実施日', '受注日', 'Create Date', '報告/提案日', '概算見積提出日']
    for col in date_columns:
        if col in merged_df.columns:
            merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce')
    
    return merged_df

# --- Function to create the deals pipeline chart (using px.timeline) ---
def create_pipeline_chart(df):
    """
    Creates a deals pipeline chart using Plotly Express.
    """
    st.header("受注案件のパイプラインチャート")

    # 受注案件のみにフィルタリング
    df_won = df[df['受注/失注'] == '受注'].copy()

    # グラフの終点（受注日）がないデータを削除
    df_won = df_won.dropna(subset=['受注日'])
    
    # 初回商談実施日が空の場合は Create Date で補完
    df_won['is_start_date_fallback'] = df_won['初回商談実施日'].isna()
    df_won['初回商談実施日'] = df_won['初回商談実施日'].fillna(df_won['Create Date'])
    
    # グラフの始点（初回商談実施日）がないデータを削除
    df_won = df_won.dropna(subset=['初回商談実施日'])

    if df_won.empty:
        st.info("プロット可能な受注案件がありませんでした。")
        return

    # Plotly Express 用のデータフレームを準備
    df_gantt = pd.DataFrame()
    df_gantt['Task'] = df_won['Deal Name'] + '<br>' + '(' + df_won['リード経路'] + ')'
    df_gantt['Start'] = df_won['初回商談実施日']
    df_gantt['Finish'] = df_won['受注日']
    df_gantt['Amount'] = df_won['受注金額']
    df_gantt['Owner'] = df_won['Full Name']
    df_gantt['Start_Type'] = df_won['is_start_date_fallback'].apply(lambda x: '案件作成日' if x else '初回商談実施日')
    df_gantt['Intermediate_Date'] = df_won['報告/提案日']
    df_gantt['Intermediate_Date2'] = df_won['概算見積提出日']
    
    # Plotly Express を使用してガントチャートを生成
    # px.timeline は Start と Finish を自動で設定してくれる
    fig = px.timeline(
        df_gantt,
        x_start="Start",
        x_end="Finish",
        y="Task",
        color="Owner", # 営業担当者ごとに色分け
        title="受注案件のパイプライン（初回商談日〜受注日）",
        hover_name="Task",
        hover_data={
            "Start": "|%Y-%m-%d",
            "Finish": "|%Y-%m-%d",
            "Owner": True,
            "Amount": ":.0f万円",
            "Start_Type": True,
        }
    )

    # 軸の表示を調整
    fig.update_xaxes(
        title="年月",
        tickformat="%Y-%m",
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(128,128,128,0.5)'
    )
    fig.update_yaxes(title="")

    # 中間イベント（報告/提案日、概算見積提出日）をプロットする
    intermediate_dates_df = df_gantt.dropna(subset=['Intermediate_Date'])
    if not intermediate_dates_df.empty:
        fig.add_trace(go.Scatter(
            x=intermediate_dates_df['Intermediate_Date'],
            y=intermediate_dates_df['Task'],
            mode='markers',
            marker=dict(symbol='diamond', size=10, color='orange', line=dict(width=1, color='orange')),
            name='報告/提案日',
            hovertext=[f"報告/提案日: {d.strftime('%Y-%m-%d')}" for d in intermediate_dates_df['Intermediate_Date']],
            hoverinfo='text'
        ))

    intermediate_dates2_df = df_gantt.dropna(subset=['Intermediate_Date2'])
    if not intermediate_dates2_df.empty:
        fig.add_trace(go.Scatter(
            x=intermediate_dates2_df['Intermediate_Date2'],
            y=intermediate_dates2_df['Task'],
            mode='markers',
            marker=dict(symbol='square', size=10, color='purple', line=dict(width=1, color='purple')),
            name='概算見積提出日',
            hovertext=[f"概算見積提出日: {d.strftime('%Y-%m-%d')}" for d in intermediate_dates2_df['Intermediate_Date2']],
            hoverinfo='text'
        ))
    
    # グラフの動的な高さ調整
    height_per_row = 50
    fig.update_layout(height=400 + height_per_row * len(df_won), legend_title_text='営業担当者')

    st.plotly_chart(fig, use_container_width=True)

# --- Main App Logic ---
st.title("HubSpot Deals ダッシュボード")
raw_deals_df, raw_stages_df, raw_users_df = load_raw_data()

if not raw_deals_df.empty and not raw_stages_df.empty and not raw_users_df.empty:
    preprocessed_df = preprocess_data(raw_deals_df, raw_stages_df, raw_users_df)
    create_pipeline_chart(preprocessed_df)
