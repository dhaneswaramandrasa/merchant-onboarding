def main():    
    import argparse
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    from utils.logger import setup_logger
    logger = setup_logger()
    logger.info("🚀 ETL job started ")
    
    from utils.connect_aurora import get_connection
    from utils.connect_gsheet import GSheetClient
    gclient = GSheetClient()
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
    
    
    from datetime import date, datetime, timedelta
    from dateutil.relativedelta import relativedelta
    import pandas as pd
    
    today = date.today()
    end_date = today
    start_date = end_date - timedelta(days=7)
    dates = pd.date_range(start_date, end_date, freq='D')
    logger.info(f"📆Today {today} Date range: {start_date} → {end_date}")
    
    
    def penarikan_trans(store_ids, start_date, end_date):
        query = f"""
            SELECT
                url_id, 
                date_trx_days, 
                all_sales_order_id_count as `Number of Transaction`
            FROM ci_store_sales_order_recap_days x
            LEFT JOIN ci_store cs ON cs.id = x.store_id
            WHERE date_trx_days BETWEEN '{start_date}' AND '{end_date}'
            AND store_id IN {store_ids}
            AND is_paid = 1
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return pd.DataFrame((cur.fetchall()))
    
    
    # ============================================================
    # 4️⃣ LOAD SOURCE DATA (ONBOARD LIST)
    # ============================================================
    
    # client_source = get_gsheet_client(SERVICE_ACCOUNT_FILE, SCOPES)
    df = gclient.read_df(SPREADSHEET_ID, 0)
    df['acc_date'] = df['Payment Date']
    temp = pd.to_datetime(df['On Board Date'], format='%d/%m/%Y', errors='coerce')
    df['On Board Date'] = temp.dt.to_period('M').astype(str).where(temp.notna(), None)
    df
    
    
    stores = df['url_id'].unique()
    full_index = pd.MultiIndex.from_product([stores, dates],names=['url_id', 'date_trx_days'])
    full_df = pd.DataFrame(index=full_index).reset_index()
    
    
    
    store_tuple = str(tuple(df['store_id'].dropna().tolist()))
    dt = penarikan_trans(store_tuple, start_date, end_date)
    dt['date_trx_days'] = pd.to_datetime(dt['date_trx_days'])
    full_df['date_trx_days'] = pd.to_datetime(full_df['date_trx_days'])
    
    full_df = full_df.merge(dt, on=['url_id', 'date_trx_days'], how='left').fillna(0)
    
    full_df['flag_low_sales'] = full_df['Number of Transaction'].apply(
        lambda x: 1 if x <= 8 else 0
    )
    
    gclient.write_df(SPREADSHEET_ID, full_df, 1)
    logger.info(f"🚀write_df index = {1} sheet = {SPREADSHEET_ID} number_row = {len(full_df)}")
    # full_df
    pivot_df = full_df.pivot_table(
        index='url_id',
        columns='date_trx_days',
        values='Number of Transaction',
        aggfunc='sum',
        fill_value=0
    )
    
    summary_stats = (
        full_df.groupby('url_id')
          .agg(
              no_active_day=('flag_low_sales', 'sum'),
              avg_trx=('Number of Transaction', 'mean')
          )
          .reset_index()
    )
    
    summary_stats['avg_trx'] = summary_stats['avg_trx'].round(2)
    
    summary = pivot_df.merge(summary_stats, on='url_id', how='left')
    
    summary['status'] = summary['no_active_day'].apply(
        lambda x: "Non active" if x >= 4 else "Active"
    )
    
    summary = summary.merge(df, on='url_id', how='left')
    summary['status_product'] = summary['Number of Product'].apply(
        lambda x: "Active" if x > 5 else "Non active"
    )
    summary['store_id_exists'] = summary['store_id'].isna().map({True: "no", False: "yes"})
    
    # summary = summary.merge(df, on='store_id', how='left')
    
    gclient.write_df(SPREADSHEET_ID, summary, 2)
    logger.info(f"🚀write_df index = {2} sheet = {SPREADSHEET_ID} number_row = {len(summary)}")
    
    # ============================================================
    # 🔟 FINAL SUMMARY COUNT
    # ============================================================
    
    # ============================================================
    # 5️⃣ SUMMARY ONBOARDING (AGREGAT PER BULAN)
    # ============================================================
    
    # 1. Store Acquisition per bulan
    store_acq = (
        summary.groupby('acc_date')['url_id']
          .nunique()
          .rename('Store Acq')
    )
    
    # 2. Closing Onboard per bulan
    total_onboard_by_support = (
        summary.dropna(subset=['On Board Date'])
          .groupby('acc_date')['url_id']
          .nunique()
          .rename('total_onboard_by_support')
    )
    
    mask = (summary['On Board Date'].notna()) & (summary['On Board Date'] == summary['acc_date'])
    regular_onboard = (
        summary[mask]
          .groupby('acc_date')['url_id']
          .nunique()
          .rename('Reguler Onboard')
    )
    
    # 3. Late Onboard (onboard > acc_date)
    mask = (summary['On Board Date'].notna()) & (summary['On Board Date'] > summary['acc_date'])
    late_onboard = (
        summary[mask]
          .groupby('acc_date')['store_id']
          .nunique()
          .rename('Late Onboard')
    )
    
    mask = (summary["status"]=="Active") 
    active_merchant = (
        summary[mask]
          .groupby(['acc_date'])['store_id']
          .nunique()
          .rename('Active Merchant')
    )
    
    mask = (summary["status"]=="Non active") 
    non_active_merchant = (
        summary[mask]
          .groupby(['acc_date'])['store_id']
          .nunique()
          .rename('Non Active Merchant')
    )
    
    mask = (summary["status_product"]=="Active") 
    active_merchaant_by_produt = (
        summary[mask]
          .groupby(['acc_date'])['url_id']
          .nunique()
          .rename('Active Merchant By Product')
    )
    mask = (summary["status_product"]=="Non active") 
    non_active_merchaant_by_produt = (
        summary[mask]
          .groupby(['acc_date'])['url_id']
          .nunique()
          .rename('Non active Merchant By Product')
    )
    
    filter =  (summary["store_id_exists"]=="no")
    merchant_not_found = (
        summary[filter]
          .groupby(['acc_date'])['store_id']
          .nunique()
          .rename('Merchant Not Found')
    )
    
    mask = (summary["status_product"]=="Active") & (summary['On Board Date'].isna())
    self_onboard = (
        summary[mask]
          .groupby(['acc_date'])['url_id']
          .nunique()
          .rename('Self_onboard')
    )
    
    total_onboard = total_onboard_by_support + self_onboard
    total_onboard = total_onboard.rename('Total Onboard')
    
    no_onboard_req =store_acq - total_onboard
    no_onboard_req = no_onboard_req.rename('No Onboard Req')
    # # 4. Gabungkan summary per bulan
    final = pd.concat([store_acq,
                        total_onboard,
                        total_onboard_by_support,
                        regular_onboard,
                        late_onboard,
                        self_onboard,
                        no_onboard_req,
                        active_merchant,
                        non_active_merchant,
                        active_merchaant_by_produt,
                        non_active_merchaant_by_produt,
                        merchant_not_found
                        ],axis=1).reset_index()
    final.insert(0, 'Data Collection Date', today)
    final["% Onboard  Acq"] = round(final["total_onboard_by_support"]/final["Store Acq"]*100,2)
    final["% Cohort Merchant"] = round(final["Active Merchant"]/final["Store Acq"]*100,2)
    gclient.append_df(SPREADSHEET_ID, final, 3)
    logger.info(f"🚀write_df index = {3} sheet = {SPREADSHEET_ID} number_row = {len(final)}")
if __name__ == "__main__":
    main()
