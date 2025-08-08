import json
import pandas as pd
import streamlit as st
import plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
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

# --- データ取得 ---
deals_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(DEALS_SHEET)
stages_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(STAGES_SHEET)
users_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(USERS_SHEET)

deals_data = deals_ws.get_all_records()
stages_data = stages_ws.get("A2:B12")
users_data = users_ws.get_all_records()

# --- データ整形 ---
deals_df = pd.DataFrame(deals_data)
stages_df = pd.DataFrame(stages_data, columns=["Stage ID", "Stage Name"])
users_df = pd.DataFrame(users_data)

stages_df["Stage ID"] = stages_df["Stage ID"].astype(int)
deals_df["Deal Stage"] = pd.to_numeric(deals_df["Deal Stage"], errors="coerce")
deals_df = deals_df.dropna(subset=["Deal Stage"])
deals_df["Deal Stage"] = deals_df["Deal Stage"].astype(int)

# --- 結合処理 ---
merged_df = deals_df.merge(stages_df, left_on="Deal Stage", right_on="Stage ID", how="left")
users_df["Full Name"] = users_df["Last Name"].fillna('') + " " + users_df["First Name"].fillna('')
merged_df = merged_df.merge(users_df[["ID", "Full Name"]], left_on="Deal owner", right_on="ID", how="left")

# --- フィルター候補リスト ---
all_owner_names = merged_df["Full Name"].dropna().unique().tolist()
all_stage_names = merged_df["Stage Name"].dropna().unique().tolist()

# --- セッション状態の初期化 ---
if "selected_owners" not in st.session_state:
    st.session_state.selected_owners = all_owner_names
if "selected_stages" not in st.session_state:
    st.session_state.selected_stages = all_stage_names
if "filters_committed" not in st.session_state:
    st.session_state.filters_committed = True  # 初回描画のためTrueにする

# --- UI部品（トグル） ---
owners = st.multiselect("担当者で絞り込み", options=all_owner_names, default=st.session_state.selected_owners, key="owners_selector")
stages = st.multiselect("ステージで絞り込み", options=all_stage_names, default=st.session_state.selected_stages, key="stages_selector")

# --- 更新ボタン ---
if st.button("グラフを更新"):
    st.session_state.selected_owners = owners
    st.session_state.selected_stages = stages
    st.session_state.filters_committed = True

# --- グラフ描画関数 ---
def make_stacked_bar_chart(df):
    pivot_df = df.groupby(["Full Name", "Stage Name", "Deal Name"]).size().reset_index(name="Count")
    fig = px.bar(
        pivot_df,
        x="Full Name",
        y="Count",
        color="Stage Name",
        hover_data=["Deal Name"],
        title="ステージ別 Deals（担当者ごとの積み上げ棒グラフ）"
    )
    fig.update_layout(barmode="stack")
    return fig

# --- グラフ描画 ---
if st.session_state.filters_committed:
    filtered_df = merged_df[
        (merged_df["Full Name"].isin(st.session_state.selected_owners)) &
        (merged_df["Stage Name"].isin(st.session_state.selected_stages))
    ]
    fig = make_stacked_bar_chart(filtered_df)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("上のフィルタを設定してから『グラフを更新』を押してください")
