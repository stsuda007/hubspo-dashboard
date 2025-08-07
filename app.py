import streamlit as st
import pandas as pd
import plotly.express as px

# --- データ準備（ここではCSVの代わりに前処理済みのDataFrameを使用） ---
# 例: merged_df という DataFrame を読み込んでいる前提
# データには以下の列が含まれている必要があります：
# 'Deal owner', 'Stage Name', 'Count'

# デモ用に仮データを作成（あなたの実データに置き換えてください）
# merged_df = pd.read_csv("your_cleaned_data.csv") などに置き換えてください
# ↓ここは本番では削除してください
import numpy as np
owners = ["森谷 謙一", "西本 励照", "根井 正洋"]
stages = ["案件見極", "検証サンプル待ち", "撮像/解析"]
np.random.seed(1)
merged_df = pd.DataFrame([
    {"Deal owner": o, "Stage Name": s, "Count": np.random.randint(1, 10)}
    for o in owners for s in stages
])

# --- サイドバー：フィルター ---
st.sidebar.header("フィルター")

selected_owners = st.sidebar.multiselect(
    "表示する担当者", 
    options=merged_df["Deal owner"].unique(), 
    default=merged_df["Deal owner"].unique()
)

selected_stages = st.sidebar.multiselect(
    "表示する取引ステージ", 
    options=merged_df["Stage Name"].unique(), 
    default=merged_df["Stage Name"].unique()
)

# --- フィルター適用 ---
filtered_df = merged_df[
    (merged_df["Deal owner"].isin(selected_owners)) &
    (merged_df["Stage Name"].isin(selected_stages))
]

# --- グラフのためにデータ整形 ---
pivot_df = filtered_df.pivot_table(index="Deal owner", columns="Stage Name", values="Count", fill_value=0)
pivot_df = pivot_df.reset_index().melt(id_vars="Deal owner", var_name="Stage", value_name="Count")

# --- グラフ描画 ---
fig = px.bar(
    pivot_df,
    x="Deal owner",
    y="Count",
    color="Stage",
    title="ステージ別 Deals（担当者ごとの積み上げ棒グラフ）",
    hover_name="Stage"
)
fig.update_layout(barmode='stack')

# --- 表示 ---
st.plotly_chart(fig, use_container_width=True)
