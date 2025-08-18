import time
import json
import gspread
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta, date
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# --- Streamlitページの基本設定 ---
st.set_page_config(layout="wide", page_title="HubSpotダッシュボード")

# --- 設定値 ---
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

# --- Functions (定義をまとめて配置) ---

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
            stages_data = pd.DataFrame(stages_ws.get("A2:B23"), columns=["Stage No", "Stage Name"])
            users_data = pd.DataFrame(users_ws.get_all_records())
            funnel_mapping_raw = stages_ws.get("E1:H14")
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

def preprocess_data(deals, stages, users, funnel_mapping):
    """
    データの前処理を1つの関数にまとめる
    """

    users_df = users.copy()
    users_df["Full Name"] = users_df["Last Name"].fillna("") + " " + users_df["First Name"].fillna("")
    users_df = users_df.rename(columns={"ID": "User ID"})
    
    deals_df = deals.copy()
    
    # 💡 修正点: 列名を最初にリネームします。
    deals_df = deals_df.rename(columns={"Deal owner": "User ID", "Deal Stage (name)": "Stagename", "Deal Stage": "Stage No"})
    
    deals_df["User ID"] = pd.to_numeric(deals_df["User ID"], errors="coerce")
    
    # 💡 修正点: リネーム後、新しい列名 "Stage No" を使って数値変換します。
    deals_df["Stage No"] = pd.to_numeric(deals_df["Stage No"], errors="coerce")
    deals_df['Pipeline (name)'] = deals_df['Pipeline (name)'].astype(str).str.strip()
    deals_df['Stagename'] = deals_df['Stagename'].astype(str).str.strip()

    stages_df = stages.copy()
    stages_df["Stage No"] = pd.to_numeric(stages_df["Stage No"], errors="coerce")

    deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")

    merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
    
    # 💡 修正点: リネームした`Stage No`列をキーとして使用
    merged_df = merged_df.merge(stages_df, on="Stage No", how="left")

    anken_type_categories = ["New", "Upsell", "Renewal", "Other"]
    def agg_anken_type(val) -> str:
        if pd.isna(val): return "Other"
        s = str(val).strip().lower()
        if s in ("newbusiness", "new business", "new"): return "New"
        if s in ("existingbusiness", "existing business", "upsell", "cross-sell", "cross sell", "expansion", "csアカウント", "cs導入サービス"): return "Upsell"
        if s in ("renewal", "renew"): return "Renewal"
        return "Other"
    merged_df["Anken Type"] = (
        merged_df["Deal Type"]
        .apply(agg_anken_type)
        .astype(pd.CategoricalDtype(categories=anken_type_categories, ordered=True))
    )
    
    date_columns = [
        '初回商談実施日', '受注日', '受注目標日', '有償ライセンス発行', '概算見積提出日', '報告/提案日',
        '最終見積提出日', 'Create Date', '活動提案アクション', '実施予定日', 'Close Date',
        '現地デモ実施日', '営業引継ぎ日', '撮像/解析完了日', '撮影日', '失注日',
        'Snapshot_date', '治具手配日', '検証_開始日'
    ]
    for col in date_columns:
        if col in merged_df.columns:
            merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce').dt.tz_localize(None)
            
    # ▼ 修正後のロジック ▼
    def determine_stage_and_funnel_with_debug(row, mapping_df):
        deals_pipeline = str(row.get('Pipeline (name)', ''))
        deals_stage = str(row.get('Stagename', ''))
        
        # 1. Pipelineと取引ステージの両方で完全一致を探す
        exact_match = mapping_df[
            (mapping_df['Pipeline'].astype(str).str.strip() == deals_pipeline) &
            (mapping_df['取引ステージ'].astype(str).str.strip() == deals_stage)
        ]
        if not exact_match.empty:
            debug_message = "Mapping Success!: " + exact_match.iloc[0]['ファネル名称']
            return exact_match.iloc[0]['Stage ID'], exact_match.iloc[0]['ファネル名称'], debug_message

        # 2. Pipelineが一致し、取引ステージが空の行を探す（案件化前、成約）
        empty_stage_mapping = mapping_df[
            (mapping_df['Pipeline'].astype(str).str.strip() == deals_pipeline) &
            (mapping_df['取引ステージ'].fillna('').astype(str).str.strip() == '')
        ]
        if not empty_stage_mapping.empty:
            debug_message = "Mapping Success (empty stage)!: " + empty_stage_mapping.iloc[0]['ファネル名称']
            return empty_stage_mapping.iloc[0]['Stage ID'], empty_stage_mapping.iloc[0]['ファネル名称'], debug_message
        
        # 3. ファジーマッチ（部分一致）のロジック
        fuzzy_match_rows = mapping_df[
            (mapping_df['Pipeline'].astype(str).str.strip() == deals_pipeline) &
            (mapping_df['取引ステージ'].astype(str).str.strip().str.len() > 0)
        ]

        for index, mapping_row in fuzzy_match_rows.iterrows():
            mapping_stage = mapping_row['取引ステージ'].strip()
            if (deals_stage in mapping_stage) or (mapping_stage in deals_stage):
                debug_message = "Mapping Success (fuzzy match)!: " + mapping_row['ファネル名称']
                return mapping_row['Stage ID'], mapping_row['ファネル名称'], debug_message

        # 4. どの条件にも一致しなかった場合
        debug_message = f"Mapping failed. Pipeline (name): '{deals_pipeline}', Deal Stage (name): '{deals_stage}'"
        return None, None, debug_message
    
    # Apply the mapping function to the merged dataframe
    # This unpacks the three values returned by determine_stage_and_funnel_with_debug
    # into new columns on the merged_df.
    merged_df[['Funnel_Stage_ID', 'Funnel_Name', 'Funnel_Debug_Info']] = merged_df.apply(
        lambda row: determine_stage_and_funnel_with_debug(row, funnel_mapping),
        axis=1,
        result_type='expand'
    )
    
    # Return the processed dataframes to the main application
    return merged_df, stages_df, funnel_mapping
def get_fiscal_dates(today, fiscal_start_month=1):
    """
    指定された日付と会計年度の開始月に基づいて、会計年度、半期、四半期の開始日と終了日を計算します。
    """
    current_year = today.year
    current_month = today.month

    # 会計年度の開始と終了を計算
    if current_month >= fiscal_start_month:
        fiscal_year_start = datetime.datetime(current_year, fiscal_start_month, 1).date()
    else:
        fiscal_year_start = datetime.datetime(current_year - 1, fiscal_start_month, 1).date()

    fiscal_year_end = datetime.datetime(fiscal_year_start.year + 1, fiscal_start_month, 1).date() - datetime.timedelta(days=1)

    # 半期の開始と終了を計算
    if current_month >= fiscal_start_month and current_month < fiscal_start_month + 6:
        half_year_start = fiscal_year_start
    else:
        half_year_start = datetime.datetime(fiscal_year_start.year, fiscal_start_month + 6, 1).date()
    
    half_year_end = datetime.datetime(half_year_start.year, half_year_start.month + 6, 1).date() - datetime.timedelta(days=1)

    # 現在の月の四半期の開始月を計算
    # 四半期は3ヶ月ごと
    
    # 会計年度の開始月を基準として、現在の月がどの四半期に属するかを判断します。
    # 例：4月始まりの場合、Q1は4-6月、Q2は7-9月、Q3は10-12月、Q4は1-3月
    
    # 簡潔にするために、現在の月と会計年度開始月の相対的な差を考慮します。
    month_diff = (current_month - fiscal_start_month) % 12
    quarter_index = month_diff // 3
    
    # 基準月を四半期の開始月に合わせる
    quarter_start_month = fiscal_start_month + (quarter_index * 3)

    # 12を超える場合は調整
    if quarter_start_month > 12:
        quarter_start_month -= 12
        
    # 四半期の開始日と終了日を計算
    quarter_start_year = fiscal_year_start.year
    if quarter_start_month < fiscal_start_month:
        quarter_start_year += 1
        
    quarter_start = datetime.datetime(quarter_start_year, quarter_start_month, 1).date()

    end_month = quarter_start.month + 3
    end_year = quarter_start.year
    if end_month > 12:
        end_month -= 12
        end_year += 1
    quarter_end = datetime.datetime(end_year, end_month, 1).date() - datetime.timedelta(days=1)
    
    return (
        fiscal_year_start, fiscal_year_end,
        half_year_start, half_year_end,
        quarter_start, quarter_end
    )

def display_kpis(df, start_date, end_date):
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


def create_funnel_chart(df, funnel_mapping_df):
    st.subheader("案件ステージ別ファネルチャート")
    if df.empty:
        st.info("データがありません。")
        return

    fm = funnel_mapping_df.drop_duplicates('ファネル名称').copy()
    fm['Stage ID'] = pd.to_numeric(fm['Stage ID'], errors='coerce')
    stage_order = fm.sort_values('Stage ID')['ファネル名称'].tolist()
    
    funnel_data = df["Funnel_Name"].dropna().value_counts().reset_index()
    funnel_data.columns = ["Funnel_Name", "Count"]

    stage_order = funnel_mapping_df.drop_duplicates('ファネル名称').sort_values('Stage ID')['ファネル名称'].tolist()
    funnel_data['Funnel_Name'] = pd.Categorical(funnel_data['Funnel_Name'], categories=stage_order, ordered=True)
    funnel_data = funnel_data.sort_values("Funnel_Name", ascending=True)
    
    fig = go.Figure(go.Funnel(
        y = funnel_data["Funnel_Name"],
        x = funnel_data["Count"],
        textinfo = "value+percent initial",
        marker = {"color": ["deepskyblue", "lightseagreen", "cadetblue", "teal", "dodgerblue", "steelblue", "skyblue", "powderblue", "lightblue", "lightsteelblue"]}
    ))
    fig.update_layout(height=500, width=800, margin=dict(t=0, b=0, l=0, r=0))
    st.plotly_chart(fig, use_container_width=True)


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


def create_pipeline_chart(df, start_date, end_date):
    st.subheader("案件パイプラインチャート")
    df_plot = df.copy()
    
    # 欠損値処理
    df_plot = df_plot.dropna(subset=['受注日'])
    
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

# ----------------------------------------
# ▼ メインアプリケーションの実行部分 ▼
# ----------------------------------------

# データの読み込み
deals_df, stages_df, users_df, funnel_mapping_df = load_data_with_retry()

if deals_df.empty or stages_df.empty or users_df.empty or funnel_mapping_df.empty:
    st.stop()

# データの前処理
merged_df, stages_df, funnel_mapping_df = preprocess_data(deals_df, stages_df, users_df, funnel_mapping_df)

# --- Sidebar Filters ---
st.sidebar.header("フィルタ")

if '受注/失注' in merged_df.columns:
    deal_status_options = ["すべて"] + list(merged_df["受注/失注"].dropna().unique())
    selected_deal_status = st.sidebar.selectbox("受注/失注", deal_status_options)
else:
    selected_deal_status = "すべて"

lead_options = ["すべて"] + list(merged_df["リード経路"].dropna().unique())
selected_lead_path = st.sidebar.selectbox("リード経路", lead_options)

new_upsell = ["すべて"] + list(merged_df["Anken Type"].dropna().unique())
selected_new_upsell = st.sidebar.selectbox("案件タイプ", new_upsell)

sales_rep_options = ["すべて"] + list(merged_df["Full Name"].dropna().unique())
selected_sales_reps = st.sidebar.multiselect("営業担当者", sales_rep_options, default=["すべて"])

# 日付範囲の選択
date_filter_preset = st.sidebar.radio(
    "日付範囲のプリセット",
    ("今月","今四半期", "今半期", "今年度", "全期間","カスタム")
)

today = datetime.now().date()
fiscal_year_start, fiscal_year_end, half_year_start, half_year_end, qtr_start, qtr_end, month_start, month_end = get_fiscal_dates(today)
date_col = 'Snapshot_date'
min_date_val = merged_df[date_col].min().date() if not merged_df[date_col].isna().all() else today
max_date_val = merged_df[date_col].max().date() if not merged_df[date_col].isna().all() else today

if date_filter_preset == "今月":
    start_date = month_start
    end_date = month_end
elif date_filter_preset == "今四半期":
    start_date = qtr_start
    end_date = qtr_end
elif date_filter_preset == "今半期":
    start_date = half_year_start
    end_date = half_year_end
elif date_filter_preset == "今年度":
    start_date = fiscal_year_start
    end_date = fiscal_year_end
elif date_filter_preset == "全期間":
    start_date = min_date_val
    end_date = max_date_val
else:
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

date_col = 'Snapshot_date' if 'Snapshot_date' in filtered_df.columns else 'Create Date'
filtered_df = filtered_df[
    (filtered_df[date_col].dt.date >= start_date) & (filtered_df[date_col].dt.date <= end_date)
]


# --- Main app layout ---
st.title("HubSpot Deals ダッシュボード")

# KPIセクション
display_kpis(filtered_df, start_date, end_date)

st.divider()

st.subheader("デバッグ情報（マッピング）")
# debug_df = filtered_df[filtered_df['Funnel_Debug_Info'].notna()]
debug_df = filtered_df.copy()
st.warning("案件のファネルマッピング情報")
st.dataframe(debug_df[['Deal Name', 'Anken Type', 'Stage No', 'Stagename', 'Funnel_Stage_ID', 'Funnel_Name', 'Funnel_Debug_Info']])

# ファネルチャートとバーチャートを横並びに配置
col1, col2 = st.columns(2)
with col1:
    create_funnel_chart(filtered_df, funnel_mapping_df)
with col2:
    create_monthly_bar_chart(filtered_df)
#st.write("Funnel_Name 列のユニークな値:", filtered_df["Funnel_Name"].dropna().unique())
#st.write("Funnel_Name Mappingのユニークな値:", funnel_mapping_df["ファネル名称"].unique())
st.divider()

# パイプラインチャート
create_pipeline_chart(filtered_df, start_date, end_date)
