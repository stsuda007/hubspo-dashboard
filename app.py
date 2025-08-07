import streamlit as st
import json
import pandas as pd
import plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- 認証 ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
gc = gspread.authorize(creds)

# --- スプレッドシート設定 ---
SPREADSHEET_KEY = "【あなたのスプレッドシートキーに置き換え】"
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"

st.title("取引ステージ別 Deals グラフ")
st.caption("ステージと担当者でフィルタし、「更新」で描画")

# --- UI: 更新ボタン ---
if st.button("更新"):

    # --- データ取得 ---
    deals_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(DEALS_SHEET)
    stages_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(STAGES_SHEET)
    users_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(USERS_SHEET)

    deals_data = deals_ws.get_all_records()
    stages_data = stages_ws.get("A2:B12")
    users_data = users_ws.get_all_records()

    deals_df = pd.DataFrame(deals_data)
    stages_df = pd.DataFrame(stages_data, columns=["Stage ID", "Stage Name"])
    users_df = pd.DataFrame(users_data)

    # --- ID変換 ---
    deals_df["Deal Stage"] = pd.to_numeric(deals_df["Deal Stage"], errors="coerce").astype("Int64")
    stages_df["Stage ID"] = stages_df["Stage ID"].astype(int)
    merged_df = deals_df.merge(stages_df, left_on="Deal Stage", right_on="Stage ID", how="left")

    users_df["Full Name"] = users_df["Last Name"].fillna("") + " " + users_df["First Name"].fillna("")
    users_df["ID"] = users_df["ID"].astype("Int64")
    merged_df["Deal owner"] = pd.to_numeric(merged_df["Deal owner"], errors="coerce").astype("Int64")
    merged_df = merged_df.merge(users_df[["ID", "Full Name"]], left_on="Deal owner", right_on="ID", how="left")

    # --- フィルター設定 ---
    unique_owners = merged_df["Full Name"].dropna().unique().tolist()
    selected_owners = st.multiselect("担当者を選択", options=unique_owners, default=unique_owners)

    unique_stages = merged_df["Stage Name"].dropna().unique().tolist()
    selected_stages = st.multiselect("ステージを選択", options=unique_stages, default=unique_stages)

    filtered_df = merged_df[
        merged_df["Full Name"].isin(selected_owners) &
        merged_df["Stage Name"].isin(selected_stages)
    ]

    # --- データ整形（積み上げ棒グラフ用） ---
    grouped_df = (
        filtered_df
        .groupby(["Full Name", "Stage Name"])["Deal Name"]
        .count()
        .reset_index(name="Count")
    )

    # --- グラフ描画 ---
    fig = px.bar(
        grouped_df,
        x="Full Name",
        y="Count",
        color="Stage Name",
        title="担当者ごとのステージ別 Deals",
        hover_data={"Full Name": True, "Stage Name": True}
    )
    fig.update_layout(barmode="stack")
    st.plotly_chart(fig, use_container_width=True)

    # --- 案件詳細のテーブル（参考表示） ---
    with st.expander("案件一覧（フィルタ後）を表示"):
        st.dataframe(filtered_df[["Deal Name", "Full Name", "Stage Name"]])
