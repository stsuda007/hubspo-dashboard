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

# --- データ取得関数 ---
@st.cache_data(ttl=600)
def load_data():
    deals_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(DEALS_SHEET)
    stages_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(STAGES_SHEET)
    users_ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(USERS_SHEET)

    deals_df = pd.DataFrame(deals_ws.get_all_records())
    stages_df = pd.DataFrame(stages_ws.get("A2:B"), columns=["Stage ID", "Stage Name"])
    stages_df["Stage ID"] = stages_df["Stage ID"].astype(str)

    users_data = users_ws.get_all_records()
    users_df = pd.DataFrame(users_data)
    users_df["ID"] = users_df["ID"].astype(str)
    users_df["Full Name"] = users_df[["First Name", "Last Name"]].fillna("").agg(" ".join, axis=1).str.strip()

    return deals_df, stages_df, users_df

# --- データ変換関数 ---
def prepare_deals_data(deals_df, stages_df, users_df):
    deals_df = deals_df.copy()
    deals_df["Deal Stage"] = deals_df["Deal Stage"].astype(str)
    deals_df["Deal owner"] = deals_df["Deal owner"].astype(str)

    deals_df = deals_df.merge(stages_df, left_on="Deal Stage", right_on="Stage ID", how="left")
    deals_df = deals_df.merge(users_df[["ID", "Full Name"]], left_on="Deal owner", right_on="ID", how="left")

    return deals_df

# --- グラフ描画関数 ---
def plot_stacked_bar(deals_df, selected_users, selected_stages):
    df_filtered = deals_df[deals_df["Full Name"].isin(selected_users) & deals_df["Stage Name"].isin(selected_stages)]

    df_grouped = df_filtered.groupby(["Full Name", "Stage Name", "Deal Name"]).size().reset_index(name="Count")

    fig = px.bar(
        df_grouped,
        x="Full Name",
        y="Count",
        color="Stage Name",
        hover_data=["Deal Name"],
        title="ステージ別 Deals（積み上げ棒グラフ）"
    )
    fig.update_layout(barmode='stack', xaxis_title="担当者", yaxis_title="件数")
    st.plotly_chart(fig, use_container_width=True)

# --- Streamlit UI ---
st.set_page_config(page_title="HubSpot Deals Dashboard", layout="wide")
st.title("HubSpot Deals Dashboard")

# 初回ロード & 更新ボタン
if "should_update" not in st.session_state:
    st.session_state["should_update"] = True

if st.button("🔁 データ更新"):
    st.session_state["should_update"] = True

if st.session_state["should_update"]:
    deals_df, stages_df, users_df = load_data()
    prepared_df = prepare_deals_data(deals_df, stages_df, users_df)

    user_list = sorted(prepared_df["Full Name"].dropna().unique())
    stage_list = sorted(prepared_df["Stage Name"].dropna().unique())

    selected_users = st.multiselect("担当者を選択：", user_list, default=user_list)
    selected_stages = st.multiselect("ステージを選択：", stage_list, default=stage_list)

    plot_stacked_bar(prepared_df, selected_users, selected_stages)

    st.session_state["should_update"] = False
