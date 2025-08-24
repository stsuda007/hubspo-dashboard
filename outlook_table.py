import json
import gspread
import pandas as pd
import streamlit as st
import time
from datetime import datetime, timedelta
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

# --- Streamlitページの基本設定 ---
st.set_page_config(
    page_title="Hubspot Dashboard",
    page_icon="🧊",
    layout="wide",  # streamlitが画面いっぱいに使う
    initial_sidebar_state="expanded",
)

# --- 認証 ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(creds)
except KeyError:
    st.error("Googleサービスアカウントの認証情報が設定されていません。`st.secrets`に`GOOGLE_SERVICE_ACCOUNT`を設定してください。")
    st.stop()

# ハードコードされたスプレッドシートキーを、st.secretsから読み込むように変更
try:
    SPREADSHEET_KEY = st.secrets["SPREADSHEET_KEY"]
except KeyError:
    st.error("Google Sheetsのスプレッドシートキーが設定されていません。`st.secrets`に`SPREADSHEET_KEY`を設定してください。")
    st.stop()

# --- 定数 ---
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"

# --- helpers ---
def _norm(x):
    if pd.isna(x):
        return ""
    return str(x).strip().lower()
def strike_text(s: str) -> str:
    """U+0336 を使った打消線（DataFrameでも崩れにくい）"""
    if pd.isna(s):
        return s
    s = str(s)
    return "".join(ch + "\u0336" for ch in s) + "\u0336"
def is_lost_row(row: pd.Series) -> bool:
    """失注を多面的に判定（列名は存在すれば使う / 無ければ無視）"""
    stage_name = _norm(row.get("Stage Name"))
    status_jp  = _norm(row.get("受注/失注"))
    deal_stat  = _norm(row.get("Deal Status"))
    lost_date  = row.get("失注日")

    return (
        ("失注" in stage_name) or
        ("lost" in stage_name) or
        ("失注" in status_jp) or
        ("closed lost" in deal_stat) or
        (pd.notna(lost_date) and str(lost_date).strip() != "")
    )
def apply_strike_style(df: pd.DataFrame):
    """is_lost==True の行をまるごと打消し線にする"""
    def strike_style(row):
        if bool(row.get('is_lost', False)):
            return ['text-decoration: line-through'] * len(row)
        return [''] * len(row)
    return df.style.apply(strike_style, axis=1)
def apply_dim_style(df: pd.DataFrame, mode: str = "both",
                    text_gray: str = "#6b7280",   # Tailwind: gray-500
                    bg_gray: str = "#f3f4f6"):    # Tailwind: gray-100
    """
    is_lost==True の行をグレーアウト。
    mode: "text"（文字だけ）, "bg"（背景だけ）, "both"（両方）
    """
    def style_row(row):
        if bool(row.get('is_lost', False)):
            styles = []
            for _ in row.index:
                parts = []
                if mode in ("text", "both"):
                    parts.append(f"color: {text_gray}")
                if mode in ("bg", "both"):
                    parts.append(f"background-color: {bg_gray}")
                styles.append("; ".join(parts))
            return styles
        return [""] * len(row)
    return df.style.apply(style_row, axis=1)


# --- データ取得関数（キャッシュ＆リトライ機能付き） ---
@st.cache_data(ttl=300, show_spinner="Google Sheets からデータ取得中...")
def load_data_with_retry(max_retries=3, delay=5):
    attempt = 0
    while attempt < max_retries:
        try:
            sh = gc.open_by_key(SPREADSHEET_KEY)
            deals_ws = sh.worksheet(DEALS_SHEET)
            stages_ws = sh.worksheet(STAGES_SHEET)
            users_ws = sh.worksheet(USERS_SHEET)

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
        except Exception as e:
            st.error(f"データの読み込み中に予期せぬエラーが発生しました: {e}")
            break

    st.error("Google Sheetsの読み込みに失敗しました。後ほど再試行してください。")
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# --- データ処理関数 ---
def process_and_merge_data(deals_df, stages_df, users_df):
    users_df["Full Name"] = users_df["Last Name"].fillna("") + " " + users_df["First Name"].fillna("")
    users_df = users_df.rename(columns={"ID": "User ID"})
    
    deals_df = deals_df.rename(columns={"Deal owner": "User ID", "Deal Stage": "Stage ID"})

    deals_df["User ID"] = pd.to_numeric(deals_df["User ID"], errors="coerce")
    deals_df["Stage ID"] = pd.to_numeric(deals_df["Stage ID"], errors="coerce")
    stages_df["Stage ID"] = pd.to_numeric(stages_df["Stage ID"], errors="coerce")
    
    # 金額カラムの前処理を強化
    deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")
    deals_df['見込売上額'] = deals_df['見込売上額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["見込売上額"] = pd.to_numeric(deals_df["見込売上額"], errors="coerce")
    
    # データ処理関数内でカンマ区切りの列を生成
    deals_df['見込売上額（円）'] = deals_df['見込売上額'].apply(lambda x: f"￥{x:,.0f}" if pd.notna(x) else "")
    deals_df['受注金額（円）'] = deals_df['受注金額'].apply(lambda x: f"￥{x:,.0f}" if pd.notna(x) else "")
    
    merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
    merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")

    # 失注判定フラグ
    merged_df["is_lost"] = merged_df.apply(is_lost_row, axis=1)
    
    return merged_df

# --- パイプライン案件テーブル表示関数 ---
def display_pipeline_projects_table(df):
    """
    パイプライン案件の一覧をテーブルとして表示する。
    """
    # 日付列をdatetime型に変換
    df['受注目標日_dt'] = pd.to_datetime(df['受注目標日'], errors='coerce')
    df['納品予定日_dt'] = pd.to_datetime(df['納品予定日'], errors='coerce')

    # 現在の日付を取得
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)

    # フィルタリング: 受注目標日または納品予定日が今月以降の案件
    df_pipeline = df[(df['受注目標日_dt'] >= first_day_of_current_month) | (df['納品予定日_dt'] >= first_day_of_current_month)].copy()
    if df_pipeline.empty:
        st.info("今月以降の受注目標日または納品予定日が記載されている案件がありません。")
        return

    # 表示用にカラム名を変更し、is_lost を引き継ぎ
    display_df = (
        df_pipeline
        .rename(columns={
            'Full Name': '営業担当者',
            'Deal Name': '案件名',
            'Stage Name': 'フェーズ'
        })
        .assign(is_lost=df_pipeline['is_lost'].values)
    )

    # 打消線付きの表示用案件名
    # display_df['案件名_表示'] = display_df.apply(
    #    lambda r: strike_text(r['案件名']) if r['is_lost'] else r['案件名'],
    #    axis=1
    # )
    # 置き換え（失注のときもそのまま文字を見せる）
    display_df['案件名_表示'] = display_df['案件名']

    # `cols_to_display`で列の順序を統一（is_lost は内部用に保持、表では非表示）
    cols_to_display = [
        '営業担当者',
        '案件名_表示',
        '受注目標日_dt',
        '納品予定日_dt',
        '見込売上額（円）',
        '受注金額（円）',
        'フェーズ',
        '見込売上額',    # 集計用（非表示）
        '受注金額',      # 集計用（非表示）
        'is_lost'        # 集計用（非表示）
    ]
    display_df = display_df[cols_to_display]

    # --- 月ごとの表示 ---
    st.subheader("月別パイプライン")
    next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    two_months_later = (today.replace(day=1) + timedelta(days=62)).replace(day=1)
    three_months_later = (today.replace(day=1) + timedelta(days=93)).replace(day=1)

    def get_month_group(date):
        if pd.isna(date):
            return "その他"
        if date.year == today.year and date.month == today.month:
            return f"{today.month}月"
        elif date.year == next_month.year and date.month == next_month.month:
            return f"{next_month.month}月"
        elif date.year == two_months_later.year and date.month == two_months_later.month:
            return f"{two_months_later.month}月"
        elif date.year == three_months_later.year and date.month == three_months_later.month:
            return f"{three_months_later.month}月"
        else:
            return "その他"

    display_df['Grouping Month'] = display_df['受注目標日_dt'].apply(get_month_group)
    grouped_by_month = display_df.groupby('Grouping Month')
    current_month_name = f"{today.month}月"
    next_month_name = f"{next_month.month}月"
    two_months_later_name = f"{two_months_later.month}月"
    three_months_later_name = f"{three_months_later.month}月"
    custom_order = [current_month_name, next_month_name, two_months_later_name, three_months_later_name, "その他"]
    sorted_groups = sorted(grouped_by_month, key=lambda x: custom_order.index(x[0]) if x[0] in custom_order else 99)

    # 表示する列の順序を定義（テーブル表示時に is_lost は非表示）
    month_table_order = ('営業担当者', '案件名_表示', '受注目標日_dt', '納品予定日_dt', '見込売上額（円）', '受注金額（円）', 'フェーズ')

    for name, group2 in sorted_groups:
        total_outlook2 = group2.loc[~group2['is_lost'], '見込売上額'].sum()
        with st.expander(f"{name} ー 売上見込額: {total_outlook2:,.0f}"):
            view_df = (
                group2
                .drop(columns=['Grouping Month'])
                .sort_values(by=['受注目標日_dt','is_lost'], ascending=[True,True], na_position='last')
                # 列順はここで揃える（column_order を使わない想定）
                [['営業担当者','案件名_表示','受注目標日_dt','納品予定日_dt','見込売上額（円）','受注金額（円）','フェーズ']]
            )
            #styled = apply_strike_text(view_df)
            #styled = apply_dim_style(view_df, mode="both")

            st.dataframe(
                styled,
                column_config={
                    "案件名_表示": st.column_config.TextColumn("案件名"),
                    "見込売上額（円）": st.column_config.TextColumn("見込売上額", help="案件の予想売上金額"),
                    "受注金額（円）": st.column_config.TextColumn("受注金額", help="受注が確定した金額"),
                    "受注目標日_dt": st.column_config.DateColumn("受注目標日", format="MM/DD"),
                    "納品予定日_dt": st.column_config.DateColumn("納品予定日", format="MM/DD"),
                },
                hide_index=True,
                use_container_width=True,
                height=300,
            )
            total_sum = group2.loc[~group2['is_lost'], '受注金額'].sum()
            total_outlook = group2.loc[~group2['is_lost'], '見込売上額'].sum()
            st.markdown(f"**合計受注金額: {total_sum:,.0f}　合計売上見込額: {total_outlook:,.0f}**")


    # --- 担当者ごとの表示 ---
    st.subheader("営業担当者別パイプライン")
    
    # 担当者ごとのソートとグループ化
    sorted_by_user_df = display_df.sort_values(
        by=['営業担当者', '受注目標日_dt'],
        ascending=[True, True],
        na_position='last'
    )
    grouped_by_user = sorted_by_user_df.groupby('営業担当者')

    # テーブル列順（担当者別）
    display_columns = ('営業担当者', '案件名_表示', '受注目標日_dt', '納品予定日_dt', '見込売上額（円）', '受注金額（円）', 'フェーズ')

    # 各担当者のデータを個別に表示
    for name, group_df in grouped_by_user:
        with st.expander(f"{name} ー 案件数:{group_df.shape[0]}"):
            view_df = (
                group_df
                .drop(columns=['Grouping Month'])
                .sort_values(by=['受注目標日_dt','is_lost'], ascending=[True,True], na_position='last')
                [['営業担当者','案件名_表示','受注目標日_dt','納品予定日_dt','見込売上額（円）','受注金額（円）','フェーズ']]
            )
            # styled = apply_strike_style(view_df)
            styled = apply_dim_style(view_df, mode = "bg")

            st.dataframe(
                styled,
                column_config={
                    "案件名_表示": st.column_config.TextColumn("案件名"),
                    "見込売上額（円）": st.column_config.TextColumn("見込売上額"),
                    "受注金額（円）": st.column_config.TextColumn("受注金額"),
                    "受注目標日_dt": st.column_config.DateColumn("受注目標日", format="MM/DD"),
                    "納品予定日_dt": st.column_config.DateColumn("納品予定日", format="MM/DD"),
                },
                use_container_width=True,
                height=300,
                hide_index=True,
            )

            total_sum = group_df.loc[~group_df['is_lost'], '受注金額'].sum()
            total_outlook = group_df.loc[~group_df['is_lost'], '見込売上額'].sum()
            st.markdown(f"**合計受注金額: {total_sum:,.0f}　合計売上見込額: {total_outlook:,.0f}**")


# --- メインアプリケーションの実行部分 ---
def main():
    st.markdown(f'<h2 style="color:#444444;font-size:24px;">{"受注目標のある案件パイプライン"}</h2>', unsafe_allow_html=True)
    deals_df, stages_df, users_df = load_data_with_retry()
    if deals_df.empty or stages_df.empty or users_df.empty:
        st.error("データの読み込みに失敗したため、アプリケーションを停止します。")
        st.stop()
    merged_df = process_and_merge_data(deals_df, stages_df, users_df)
    display_pipeline_projects_table(merged_df)

if __name__ == "__main__":
    main()
