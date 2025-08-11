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

# '受注金額'列から非数値文字（カンマ、全角数字など）を削除し、数値に変換
deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")

# 金額を10000で割って切り捨てる前に、NaNを0に置き換える
deals_df["受注金額"] = (deals_df["受注金額"] / 10000).fillna(0).astype(int)

merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")

# --- Function to create the deals pipeline chart ---
def pipeline_chart_juchu(df):
    """
    Creates a pipeline chart for '受注' (won) deals from the start of the first negotiation to the closing date.
    """
    st.title("HubSpot Deals ダッシュボード")
    st.subheader("受注案件のパイプラインチャート")
    st.write("元のデータ数:", len(df))

    # Filter data for '受注' (won) deals only
    df_filtered = df[(df['受注/失注'] == '受注')].copy()
    st.write("受注フラグのデータ数:", len(df_filtered))

    # Convert date columns to datetime objects
    date_columns = ['初回商談実施日', '受注日', '受注目標日', '有償ライセンス発行', '概算見積提出日', '報告/提案日','最終見積提出日', 'Create Date']
    for col in date_columns:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce')
    
    # グラフの終点である受注日がないデータは削除
    df_filtered = df_filtered.dropna(subset=['受注日'])
    st.write("受注日不記載のデータを削除しました。データ数:", len(df_filtered))
    
    # 初回商談実施日が空欄の場合のフラグを作成
    df_filtered['is_start_date_fallback'] = df_filtered['初回商談実施日'].isna()
    st.write("初回商談実施日不記載のデータ数:", df_filtered['is_start_date_fallback'].sum())

    # 初回商談実施日が空欄の場合はCreate Dateで補完
    df_filtered['初回商談実施日'] = df_filtered['初回商談実施日'].fillna(df_filtered['Create Date'])
    
    if df_filtered.empty:
        st.info("条件に一致する受注案件がありませんでした。")
        return

    # Create a DataFrame for plotting
    df_plot = df_filtered.copy()
    
    # 案件名にリード経路を追加
    df_plot['案件名'] = df_plot['Deal Name'] + '<br>' + '(' + df_plot['リード経路'] + ')'
    df_plot['Start'] = df_plot['初回商談実施日']
    df_plot['Finish'] = df_plot['受注日']
    
    # グラフの始点（Start）と終点（Finish）の両方がないデータを削除
    df_plot = df_plot.dropna(subset=['Start', 'Finish'])
    st.write("最終的なグラフ表示データ数:", len(df_plot))

    if df_plot.empty:
        st.info("プロット可能な受注案件がありませんでした。")
        return

    df_plot = df_plot.sort_values('Start')

    # Create the Plotly Gantt chart
    fig = go.Figure()

    # Add markers and connecting lines for each deal
    for index, row in df_plot.iterrows():
        # Add a line connecting the start and end points (no hover info on the line itself)
        fig.add_trace(go.Scatter(
            x=[row['Start'], row['Finish']],
            y=[row['案件名'], row['案件名']],
            mode='lines',
            line=dict(color='black', width=3),
            showlegend=False,
            hoverinfo='none' # Changed to 'none' as hoverinfo on lines is not ideal
        ))

        # Add a marker for the start date (blue circle)
        # 初回商談実施日が空欄だった場合はグレーのマーカーで表示
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
            hovertext=f"案件名: {row['Deal Name']}<br>金額: {row['受注金額']:,}万円"
        ))
        
        # Add markers for '報告/提案日' (if they exist)
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
        # Add markers for '概算見積提出日' (if they exist)
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

# --- NEW: Pipeline Projects Table Function ---
def table_of_pipeline_projects(df):
    """
    パイプライン案件（受注目標日または納品予定日が記載されている案件）を表示
    """
    st.subheader("📊 パイプライン案件一覧")
    
    # 日付列を変換
    date_cols = ['受注目標日', '納品予定日']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # パイプライン条件: 受注目標日または納品予定日のいずれかが存在
    pipeline_condition = (
        df['受注目標日'].notna() | 
        df['納品予定日'].notna()
    )
    
    df_pipeline = df[pipeline_condition].copy()
    
    if df_pipeline.empty:
        st.info("受注目標日または納品予定日が記載されている案件がありません。")
        return
    
    # 表示用データを準備
    display_df = df_pipeline.copy()
    
    # 必要な列を選択・リネーム
    columns_to_show = {
        'Full Name': '営業担当者',
        'Deal Name': '案件名', 
        '受注金額': '見込売上額（万円）'
    }
    
    # Deal Typeがあるかチェック
    if 'Deal Type' in display_df.columns:
        columns_to_show['Deal Type'] = 'Deal Type'
    
    # 存在する列のみを使用
    available_columns = {k: v for k, v in columns_to_show.items() if k in display_df.columns}
    
    if not available_columns:
        st.error("必要な列が見つかりません。")
        return
    
    # 表示用データフレームを作成
    result_df = display_df[list(available_columns.keys())].rename(columns=available_columns)
    
    # 日付情報を追加
    def format_dates(row):
        dates = []
        if pd.notna(row['受注目標日']):
            dates.append(f"受注目標: {row['受注目標日'].strftime('%Y-%m-%d')}")
        if pd.notna(row['納品予定日']):
            dates.append(f"納品予定: {row['納品予定日'].strftime('%Y-%m-%d')}")
        return " / ".join(dates) if dates else ""
    
    result_df['予定日'] = display_df.apply(format_dates, axis=1)
    
    # NaN値を適切に処理
    result_df = result_df.fillna({
        '営業担当者': '未設定',
        'Deal Type': '未設定',
        '見込売上額（万円）': 0
    })
    
    # ソート
    sort_columns = ['営業担当者']
    if 'Deal Type' in result_df.columns:
        sort_columns.append('Deal Type')
    sort_columns.append('見込売上額（万円）')
    
    result_df = result_df.sort_values(sort_columns, ascending=[True, True, False] if len(sort_columns) == 3 else [True, False])
    
    # メイン表示
    st.write(f"**パイプライン案件数: {len(result_df)}件**")
    st.dataframe(result_df, use_container_width=True)
    
    # 営業担当者別集計
    st.write("### 営業担当者別集計")
    sales_summary = result_df.groupby('営業担当者').agg({
        '見込売上額（万円）': ['count', 'sum']
    }).round(0)
    sales_summary.columns = ['案件数', '見込売上額合計（万円）']
    sales_summary = sales_summary.sort_values('見込売上額合計（万円）', ascending=False)
    st.dataframe(sales_summary)
    
    # Deal Type別集計（Deal Type列がある場合）
    if 'Deal Type' in result_df.columns:
        st.write("### Deal Type別集計")
        type_summary = result_df.groupby('Deal Type').agg({
            '見込売上額（万円）': ['count', 'sum']
        }).round(0)
        type_summary.columns = ['案件数', '見込売上額合計（万円）']
        type_summary = type_summary.sort_values('見込売上額合計（万円）', ascending=False)
        st.dataframe(type_summary)
    
    # サマリー
    total_amount = result_df['見込売上額（万円）'].sum()
    unique_sales = result_df['営業担当者'].nunique()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("総案件数", f"{len(result_df)}件")
    with col2:
        st.metric("見込売上額合計", f"{total_amount:,.0f}万円")
    with col3:
        st.metric("営業担当者数", f"{unique_sales}名")

# --- MAIN APPLICATION ---
# 既存の受注案件チャート
pipeline_chart_juchu(merged_df)

# 区切り線を追加
st.divider()

# 新しいパイプライン案件テーブル
table_of_pipeline_projects(merged_df)
