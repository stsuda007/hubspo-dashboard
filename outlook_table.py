import json
import gspread
import pandas as pd
import streamlit as st
import time
from datetime import datetime, timedelta
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials
st.set_page_config(layout="wide") # streamlitが画面いっぱいに使う

# --- 認証 ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    credentials_dict = json.loads(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    gc = gspread.authorize(creds)
except KeyError:
    st.error("Googleサービスアカウントの認証情報が設定されていません。`st.secrets`に`GOOGLE_SERVICE_ACCOUNT`を設定してください。")
    st.stop()

# --- 定数 ---
SPREADSHEET_KEY = "1Ra_tPm2u5K4ikxacw1vdQqY_YQg-JekMsM-ZhaaVFKg"
DEALS_SHEET = "Deals"
STAGES_SHEET = "OtherParams"
USERS_SHEET = "Users"

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
    
    deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")
    deals_df['見込売上額'] = deals_df['見込売上額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["見込売上額"] = pd.to_numeric(deals_df["見込売上額"], errors="coerce")
    
    merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
    merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")
    
    return merged_df

# --- パイプライン案件テーブル表示関数 ---
def display_pipeline_projects_table(df):
    """
    パイプライン案件の一覧をテーブルとして表示する。
    """
    st.subheader("パイプライン案件一覧")

    # 日付列をdatetime型に変換
    df['受注目標日_dt'] = pd.to_datetime(df['受注目標日'], errors='coerce')
    df['納品予定日_dt'] = pd.to_datetime(df['納品予定日'], errors='coerce')

    # 現在の日付を取得
    today = datetime.now()

    # フィルタリング: 受注目標日が未来の案件、または納品予定日が未来の案件
    df_pipeline = df[(df['受注目標日_dt'] > today) | (df['納品予定日_dt'] > today)].copy()

    if df_pipeline.empty:
        st.info("未来の受注目標日または納品予定日が記載されている案件がありません。")
        return

    # 表示用にカラム名を変更
    display_df = df_pipeline.rename(columns={
        'Full Name': '営業担当者',
        'Deal Name': '案件名',
        'Stage Name': 'フェーズ'
    })

    # 表示するカラムを選択
    cols_to_display = [
        '営業担当者',
        '案件名',
        '受注目標日_dt',
        '納品予定日_dt',
        'フェーズ',
        '見込売上額',
        '受注金額'
    ]
    display_df = display_df[cols_to_display]

    # 日付列を'YYYY-MM-DD'形式の文字列に変換
    display_df['受注目標日'] = display_df['受注目標日_dt'].dt.strftime('%Y-%m-%d').fillna('')
    display_df['納品予定日'] = display_df['納品予定日_dt'].dt.strftime('%Y-%m-%d').fillna('')

    # --- 月ごとの表示 ---
    st.subheader("月別パイプライン")

    # 月ごとのグルーピングロジック
    today = datetime.now()
    next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    two_months_later = (today.replace(day=1) + timedelta(days=62)).replace(day=1)

    def get_month_group(date):
        if pd.isna(date):
            return "その他"
        if date.year == today.year and date.month == today.month:
            # 今月の月名を日本語で返す
            return f"{today.month}月"
        elif date.year == next_month.year and date.month == next_month.month:
            # 来月の月名を日本語で返す
            return f"{next_month.month}月"
        elif date.year == two_months_later.year and date.month == two_months_later.month:
            # 再来月の月名を日本語で返す
            return f"{two_months_later.month}月"
        else:
            return "その他"

    # '受注目標日_dt'列を使ってグルーピング用の新しい列を作成
    display_df['Grouping Month'] = display_df['受注目標日_dt'].apply(get_month_group)

    # 新しい列でデータをグループ化
    grouped_by_month = display_df.groupby('Grouping Month')

    # ソート順を定義（月名の昇順、「その他」を最後）
    month_order = [f"{m}月" for m in range(1, 13)]
    month_order.append("その他")
    # 現在の月に合わせて動的な順序を作成
    current_month_name = f"{today.month}月"
    next_month_name = f"{next_month.month}月"
    two_months_later_name = f"{two_months_later.month}月"
    custom_order = [current_month_name, next_month_name, two_months_later_name, "その他"]

    # グループを定義したカスタム順序でソート
    sorted_groups = sorted(grouped_by_month, key=lambda x: custom_order.index(x[0]) if x[0] in custom_order else 99)

    # 各グループのデータを個別に表示
    for name, group2 in sorted_groups:
        with st.expander(f"{name} 売上見込額:{group2['見込売上額'].sum()}"):
            st.dataframe(
                group2.drop(columns=['受注目標日_dt', '納品予定日_dt', 'Grouping Month']),
                use_container_width=True,
                height=300
            )
            total_outlook2 = group2['見込売上額'].sum()
            st.markdown(f"***合計売上見込額: {total_outlook2:,.0f}***")
        
    
    # --- 担当者ごとの表示 ---
    st.subheader("営業担当者別パイプライン")
    
    # 担当者ごとのソートとグループ化
    sorted_by_user_df = display_df.sort_values(
        by=['営業担当者', '受注目標日_dt', '受注金額'], 
        ascending=[True, True, False],
        na_position='last'
    )
    grouped_by_user = sorted_by_user_df.groupby('営業担当者')

    # 各担当者のデータを個別に表示
    for name, group in grouped_by_user:
        with st.expander(f"営業担当者: {name}"):
            st.dataframe(
                group.drop(columns=['受注目標日_dt', '納品予定日_dt']), 
                use_container_width=True, 
                height=300
            )
            total_amount = group['受注金額'].sum()
            total_outlook = group['見込売上額'].sum()
            st.markdown(f"***合計受注金額: {total_amount:,.0f}***")
            st.markdown(f"***合計売上見込額: {total_outlook:,.0f}***")
            
# --- メインアプリケーションの実行部分 ---
def main():
    st.title("HubSpot Deals ダッシュボード")

    deals_df, stages_df, users_df = load_data_with_retry()
    if deals_df.empty or stages_df.empty or users_df.empty:
        st.error("データの読み込みに失敗したため、アプリケーションを停止します。")
        st.stop()

    merged_df = process_and_merge_data(deals_df, stages_df, users_df)

    display_pipeline_projects_table(merged_df)

if __name__ == "__main__":
    main()
