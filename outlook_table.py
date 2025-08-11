def table_of_pipeline_projects(df):
    """
    現在のパイプライン案件（受注目標日または納品予定日が記載されている案件）を表示する関数
    営業担当者ごと、Deal Typeごとにまとめて見込売上額を集計
    """
    st.subheader("パイプライン案件一覧")
    st.write("元のデータ数:", len(df))
    
    # 受注目標日または納品予定日が記載されている案件を抽出
    df_filtered = df.copy()
    
    # 日付列を datetime に変換
    date_columns = ['受注目標日', '納品予定日']
    for col in date_columns:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_datetime(df_filtered[col], errors='coerce')
    
    # 受注目標日または納品予定日のいずれかが記載されている案件を抽出
    pipeline_condition = (
        df_filtered['受注目標日'].notna() | 
        df_filtered['納品予定日'].notna()
    )
    df_pipeline = df_filtered[pipeline_condition].copy()
    
    st.write("パイプライン案件数:", len(df_pipeline))
    
    if df_pipeline.empty:
        st.info("条件に一致するパイプライン案件がありませんでした。")
        return
    
    # 必要な列を準備
    required_columns = ['Full Name', 'Deal Type', 'Deal Name', '受注金額']
    
    # 列の存在確認
    missing_columns = [col for col in required_columns if col not in df_pipeline.columns]
    if missing_columns:
        st.error(f"必要な列が不足しています: {missing_columns}")
        return
    
    # 表示用のDataFrameを作成
    display_df = df_pipeline[required_columns + ['受注目標日', '納品予定日']].copy()
    
    # 列名を日本語に変更
    display_df = display_df.rename(columns={
        'Full Name': '営業担当者',
        'Deal Type': 'Deal Type',
        'Deal Name': '案件名',
        '受注金額': '見込売上額（万円）'
    })
    
    # NaNを適切な値で置換
    display_df['営業担当者'] = display_df['営業担当者'].fillna('未設定')
    display_df['Deal Type'] = display_df['Deal Type'].fillna('未設定')
    display_df['見込売上額（万円）'] = display_df['見込売上額（万円）'].fillna(0)
    
    # 詳細テーブルを表示
    st.write("### パイプライン案件詳細")
    
    # 受注目標日と納品予定日の表示を整形
    def format_dates(row):
        dates = []
        if pd.notna(row['受注目標日']):
            dates.append(f"受注目標: {row['受注目標日'].strftime('%Y-%m-%d')}")
        if pd.notna(row['納品予定日']):
            dates.append(f"納品予定: {row['納品予定日'].strftime('%Y-%m-%d')}")
        return " / ".join(dates)
    
    display_df['予定日'] = display_df.apply(format_dates, axis=1)
    
    # 表示用の最終テーブル
    final_display = display_df[['営業担当者', 'Deal Type', '案件名', '見込売上額（万円）', '予定日']].copy()
    
    # データフレームをソート（営業担当者→Deal Type→見込売上額の降順）
    final_display = final_display.sort_values([
        '営業担当者', 
        'Deal Type', 
        '見込売上額（万円）'
    ], ascending=[True, True, False])
    
    st.dataframe(final_display, use_container_width=True)
    
    # 営業担当者別の集計
    st.write("### 営業担当者別集計")
    sales_summary = display_df.groupby('営業担当者').agg({
        '見込売上額（万円）': ['count', 'sum'],
        'Deal Type': lambda x: len(x.unique())
    }).round(0)
    
    # カラム名を整理
    sales_summary.columns = ['案件数', '見込売上額合計（万円）', 'Deal Type数']
    sales_summary = sales_summary.sort_values('見込売上額合計（万円）', ascending=False)
    
    st.dataframe(sales_summary, use_container_width=True)
    
    # Deal Type別の集計
    st.write("### Deal Type別集計")
    deal_type_summary = display_df.groupby('Deal Type').agg({
        '見込売上額（万円）': ['count', 'sum'],
        '営業担当者': lambda x: len(x.unique())
    }).round(0)
    
    # カラム名を整理
    deal_type_summary.columns = ['案件数', '見込売上額合計（万円）', '営業担当者数']
    deal_type_summary = deal_type_summary.sort_values('見込売上額合計（万円）', ascending=False)
    
    st.dataframe(deal_type_summary, use_container_width=True)
    
    # 営業担当者 × Deal Type のクロス集計
    st.write("### 営業担当者 × Deal Type クロス集計（見込売上額）")
    
    pivot_table = display_df.pivot_table(
        values='見込売上額（万円）',
        index='営業担当者',
        columns='Deal Type',
        aggfunc='sum',
        fill_value=0,
        margins=True,
        margins_name='合計'
    ).round(0)
    
    st.dataframe(pivot_table, use_container_width=True)
    
    # 全体のサマリー
    st.write("### 全体サマリー")
    total_deals = len(display_df)
    total_amount = display_df['見込売上額（万円）'].sum()
    unique_sales = display_df['営業担当者'].nunique()
    unique_deal_types = display_df['Deal Type'].nunique()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("総案件数", f"{total_deals:,}件")
    with col2:
        st.metric("見込売上額合計", f"{total_amount:,.0f}万円")
    with col3:
        st.metric("営業担当者数", f"{unique_sales}名")
    with col4:
        st.metric("Deal Type数", f"{unique_deal_types}種類")
