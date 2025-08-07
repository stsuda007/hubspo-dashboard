import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Google Sheets認証 ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "your_credentials.json"  # credentialsファイルをリポジトリに追加しておく
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
gc = gspread.authorize(creds)

# --- シートキーとワークシート名 ---
SPREADSHEET_KEY = "1Ra_tPm2u5K4ikxacw1vdQqY_YQg-JekMsM-ZhaaVFKg"
deals_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet("Deals")
stages_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet("OtherParams")
users_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet("Users")

# --- データ読み込み ---
deals_data = deals_ws.get_all_records()
stages_data = stages_ws.get("A2:B12")
users_data = users_ws.get_all_records()

# --- DataFrame化 ---
deals_df = pd.DataFrame(deals_data)
stages_df = pd.DataFrame(stages_data, columns=["Stage ID", "Stage Name"])
users_df = pd.DataFrame(users_data)

# --- 整形 ---
stages_df["Stage ID"] = stages_df["Stage ID"].astype(int)
deals_df["Deal Stage"] = pd.to_numeric(deals_df["Deal Stage"], errors="coerce")
deals_df = deals_df.dropna(subset=["Deal Stage"])
deals_df["Deal Stage"] = deals_df["Deal Stage"].astype(int)

# ユーザー名結合
users_df["ID"] = users_df["ID"].astype(int)
users_df["Full Name"] = users_df["First Name"] + " " + users_df["Last Name"]
deals_df = deals_df.merge(users_df[["ID", "Full Name"]], left_on="Deal owner", right_on="ID", how="left")

# ステージ名結合
deals_df = deals_df.merge(stages_df, left_on="Deal Stage", right_on="Stage ID", how="left")

# --- Streamlit UI ---
st.title("HubSpot Deals: 担当者×ステージ別 積み上げグラフ")

# フィルタ選択
deal_owners = sorted(deals_df["Full Name"].dropna().unique())
stages = sorted(deals_df["Stage Name"].dropna().unique())

selected_owners = st.multiselect("担当者を選択", deal_owners, default=deal_owners)
selected_stages = st.multiselect("ステージを選択", stages, default=stages)

filtered_df = deals_df[
    (deals_df["Full Name"].isin(selected_owners)) &
    (deals_df["Stage Name"].isin(selected_stages))
]

# --- データ整形（棒グラフ用） ---
pivot_df = (
    filtered_df
    .groupby(["Full Name", "Stage Name"])
    .size()
    .unstack(fill_value=0)
    .reset_index()
    .melt(id_vars="Full Name", var_name="Stage", value_name="Count")
)

# --- グラフ描画 ---
fig = px.bar(
    pivot_df,
    x="Full Name",
    y="Count",
    color="Stage",
    title="ステージ別 Deals（積み上げ棒グラフ）",
    hover_name="Stage"
)
fig.update_layout(barmode="stack")

st.plotly_chart(fig, use_container_width=True)
