import json
import gspread
import pandas as pd
import streamlit as st
import time
from datetime import datetime
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

# --- 認証 ---
# Googleサービスアカウントの認証情報を取得し、認証を行う
# 認証情報が設定されていない場合はエラーをスローし、アプリを停止
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
# st.cache_dataを使って、データを300秒間キャッシュし、APIコールを削減
@st.cache_data(ttl=300, show_spinner="Google Sheets からデータ取得中...")
def load_data_with_retry(max_retries=3, delay=5):
    """
    Google Sheetsからデータを取得し、API制限エラーの場合はリトライする。

    Args:
        max_retries (int): 再試行の最大回数。
        delay (int): 再試行までの待機時間（秒）。

    Returns:
        tuple: Deals, Stages, Usersの各DataFrameを返す。失敗した場合は空のDataFrame。
    """
    attempt = 0
    while attempt < max_retries:
        try:
            # スプレッドシートと各ワークシートを開く
            sh = gc.open_by_key(SPREADSHEET_KEY)
            deals_ws = sh.worksheet(DEALS_SHEET)
            stages_ws = sh.worksheet(STAGES_SHEET)
            users_ws = sh.worksheet(USERS_SHEET)

            # データをDataFrameとして取得
            deals_data = pd.DataFrame(deals_ws.get_all_records())
            # OtherParamsシートは指定したセル範囲のみを取得
            stages_data = pd.DataFrame(stages_ws.get("A2:B12"), columns=["Stage ID", "Stage Name"])
            users_data = pd.DataFrame(users_ws.get_all_records())

            return deals_data, stages_data, users_data

        except APIError as e:
            # API制限エラー(429)の場合は再試行
            if "429" in str(e):
                st.warning(f"API制限に達しました。{delay}秒待機して再試行します...（{attempt + 1}/{max_retries}）")
                time.sleep(delay)
                attempt += 1
            else:
                # その他のAPIエラーの場合は処理を中断
                st.error(f"Google Sheets API エラー: {e}")
                break
        except Exception as e:
            st.error(f"データの読み込み中に予期せぬエラーが発生しました: {e}")
            break

    st.error("Google Sheetsの読み込みに失敗しました。後ほど再試行してください。")
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# --- データ処理関数 ---
def process_and_merge_data(deals_df, stages_df, users_df):
    """
    生データをクリーニングし、必要な情報をマージする。
    
    Args:
        deals_df (pd.DataFrame): Dealsシートの生データ。
        stages_df (pd.DataFrame): Stagesシートの生データ。
        users_df (pd.DataFrame): Usersシートの生データ。

    Returns:
        pd.DataFrame: 処理・マージ済みのDataFrame。
    """
    # ユーザー名を作成
    users_df["Full Name"] = users_df["First Name"].fillna("") + " " + users_df["Last Name"].fillna("")
    users_df = users_df.rename(columns={"ID": "User ID"})
    
    # マージしやすいようにカラム名を変更
    deals_df = deals_df.rename(columns={"Deal owner": "User ID", "Deal Stage": "Stage ID"})

    # データ型の変換（エラーを無視して処理）
    deals_df["User ID"] = pd.to_numeric(deals_df["User ID"], errors="coerce")
    deals_df["Stage ID"] = pd.to_numeric(deals_df["Stage ID"], errors="coerce")
    stages_df["Stage ID"] = pd.to_numeric(stages_df["Stage ID"], errors="coerce")
    
    # 受注金額から通貨記号やカンマを除去し、数値に変換
    deals_df['受注金額'] = deals_df['受注金額'].astype(str).str.replace(r'[^\d]', '', regex=True)
    deals_df["受注金額"] = pd.to_numeric(deals_df["受注金額"], errors="coerce")
    
    # 複数DataFrameをマージ
    merged_df = deals_df.merge(users_df[["User ID", "Full Name"]], on="User ID", how="left")
    merged_df = merged_df.merge(stages_df, on="Stage ID", how="left")
    
    return merged_df

# --- パイプライン案件テーブル表示関数 ---
def display_pipeline_projects_table(df):
    """
    パイプライン案件の一覧をテーブルとして表示する。
    
    Args:
        df (pd.DataFrame): 処理済みのDataFrame。
    """
    st.subheader("パイプライン案件一覧")

    
    # 現在の日付を取得
    today = datetime.now()
    # '受注目標日'が未来の日付であるか、または'納品予定日'が記載されている案件を抽出
    df_pipeline = df[(pd.to_datetime(df['受注目標日'], errors='coerce') > today) | df['納品予定日'].notna()].copy()
    
    if df_pipeline.empty:
        st.info("受注目標日または納品予定日が記載されている案件がありません。")
        return

    # 表示用にカラム名を変更
    display_df = df_pipeline.rename(columns={
        'Full Name': '営業担当者', 
        'Deal Name': '案件名'
    })
    
    # 表示するカラムを選択
    cols_to_display = [
        '営業担当者',
        '案件名',
        '受注目標日',
        '納品予定日',
        'Stage Name',
        '受注金額'
    ]
    
    # データフレームから必要なカラムのみを抽出
    display_df = display_df[cols_to_display]
    
    # 日付型に変換
    for col in ['受注目標日', '納品予定日']:
        display_df[col] = pd.to_datetime(display_df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        
    # ソート
    display_df = display_df.sort_values(by=['営業担当者', '受注金額'], ascending=[True, False])
    
    # 日付型に変換
    for col in ['受注目標日', '納品予定日']:
        display_df[col] = pd.to_datetime(display_df[col], errors='coerce')  # Invalid values become NaT
        display_df[col] = display_df[col].fillna(pd.Timestamp('1970-01-01'))  # Fill NaT with a default date if needed
    display_df = display_df.sort_values(by=['受注目標日'],ascending=[True])
        
    # 営業担当者ごとにデータをグループ化
    grouped = display_df.groupby('営業担当者')
    # 各営業担当者のデータを個別に表示
    for name, group in grouped:
        # st.expander を使って開閉可能なセクションを作成
        with st.expander(f"営業担当者: {name}"):
            # Streamlitでデータフレームを表示
            st.dataframe(group, use_container_width=True)

# --- メインアプリケーションの実行部分 ---
def main():
    """
    Streamlitアプリケーションのメイン関数
    """
    st.title("HubSpot Deals ダッシュボード")

    # データのロードとチェック
    deals_df, stages_df, users_df = load_data_with_retry()
    if deals_df.empty or stages_df.empty or users_df.empty:
        st.error("データの読み込みに失敗したため、アプリケーションを停止します。")
        st.stop()

    # データの加工とマージ
    merged_df = process_and_merge_data(deals_df, stages_df, users_df)

    # アプリケーションの各セクションを実行
    display_pipeline_projects_table(merged_df)

if __name__ == "__main__":
    main()
