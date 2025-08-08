import time
import json
import gspread
import pandas as pd
import plotly.express as px
import streamlit as st
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# --- 認証 ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
gc = gspread.authorize(creds)

# --- スプレッドシート設定 ---
SPREADSHEET_KEY = "1Ra_tPm2u5K4ikxacw1vdQqY_YQg-JekMsM-ZhaaVFKg"
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"

# --- データ取得関数（キャッシュ & リトライ） ---
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

merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")

# --- UI：フィルターとボタン ---
st.title("HubSpot Deals ダッシュボード")
st.subheader("取引ステージ別・担当者別の積み上げ棒グラフ")

with st.form("filter_form"):
    default_users = sorted(merged_df["Full Name"].dropna().unique())
    default_stages = sorted(merged_df["Stage Name"].dropna().unique())
    selected_users = st.multiselect("担当者を選択", default_users, default=default_users)
    selected_stages = st.multiselect("取引ステージを選択", default_stages, default=default_stages)
    update_chart = st.form_submit_button("グラフを更新")

if update_chart or st.session_state.get("initial_render", True):
    st.session_state["initial_render"] = False

    # --- データフィルタリング ---
    filtered_df = merged_df[merged_df["Full Name"].isin(selected_users) & merged_df["Stage Name"].isin(selected_stages)]

    # --- グラフ用データ整形 ---
    pivot_df = filtered_df.groupby(["Full Name", "Stage Name"])["Deal Name"].count().unstack(fill_value=0)
    pivot_df = pivot_df.reset_index().melt(id_vars="Full Name", var_name="Stage", value_name="Count")

    # --- グラフ描画 ---
    fig = px.bar(
        pivot_df,
        x="Full Name",
        y="Count",
        color="Stage",
        title="担当者ごとのDeals件数（取引ステージ別）",
        hover_data={"Stage": True, "Count": True},
    )
    fig.update_layout(barmode="stack", xaxis_title="担当者", yaxis_title="件数")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("左のチェックを変更した後は、\"グラフを更新\"ボタンを押してください。")
