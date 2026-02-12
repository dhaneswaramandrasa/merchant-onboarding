import argparse
from utils.logger import setup_logger
logger = setup_logger()
logger.info("🚀 ETL job started ")


from utils.connect_aurora import get_connection
from utils.connect_gsheet import GSheetClient
gclient = GSheetClient()

import os
from dotenv import load_dotenv
load_dotenv()
SPREADSHEET_ID_SOURCE = os.getenv("SPREADSHEET_ID_SOURCE")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")



from datetime import date, datetime
from dateutil.relativedelta import relativedelta


import pandas as pd
def main(start_date,end_date,today):														
    query = f"""
    select 
    date('{today}') as Periode,
    order_no as `No Invoice`,
    ci_store_sales_order_billing.billing_id as `No Billing`,
    ci_store.id as store_id,
    ci_cd_acq_channel.name as `Data Channel`,
    url_id as url_id,
    DATE_FORMAT(ci_store_billing.payment_date, '%Y-%m') as `Payment Date`,
    ci_cd_billing_package_options.name_id as `Note`,
    ci_store.pos_expiry_date as `Expire Date`,
    date(ci_store.created_time) as `Activated Date`,
    count(distinct ci_store_product.id) as `Number of Product`
    from ci_store_sales_order_billing 
    join ci_store_billing on (ci_store_sales_order_billing.billing_id = ci_store_billing.id)
    join ci_store_sales_order on (ci_store_sales_order.id = ci_store_sales_order_billing.sales_order_id)
    join ci_store_billing_item on (ci_store_billing_item.billing_id = ci_store_sales_order_billing.billing_id)
    join ci_cd_billing_package_options on (ci_cd_billing_package_options.id = ci_store_billing_item.billing_package_option_id)
    join ci_store on (ci_store.id = ci_store_billing_item.store_id)
    join ci_store_acq_channel on (ci_store.id = ci_store_acq_channel.id)
    join ci_cd_acq_channel on (ci_store_acq_channel.acq_channel_id=ci_cd_acq_channel.id)
    LEFT JOIN ci_store_product ON (ci_store.id = ci_store_product.store_id)
    WHERE ci_store_billing.payment_date BETWEEN '{start_date}' AND '{end_date}'
    and ci_store.status = "A"
    group by ci_store.id
            """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            df = pd.DataFrame((cur.fetchall()))
    
    logger.info(f" Total Data {len(df)}")
    def check_renewal(id):
        query = f"""
        SELECT ci_store_billing_item.*
        FROM ci_store_billing_item
        join ci_store on (ci_store.id = ci_store_billing_item.store_id)
        join ci_store_billing on (ci_store_billing.id = ci_store_billing_item.billing_id)
        where ci_store.id = {id}
        and ci_store_billing.is_paid  = 1
        limit 10
                """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
                return len(results)
        
    checks = df["store_id"].to_list()
    for i in checks:
        if check_renewal(i) != 1:
            df = df[df['store_id'] != i]

    df = df.sort_values("Payment Date")
    index = 0
    gclient.append_df(SPREADSHEET_ID,df,index)
    logger.info(f"🚀write_df index = {index} sheet = {SPREADSHEET_ID} number_row = {len(df)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--end_date",
        default=None,
        help="YYYY-MM-DD"
    )

    args = parser.parse_args()

    today = date.today()
    end_date = (
        datetime.strptime(args.end_date, "%Y-%m-%d").replace(day=1)
        if args.end_date
        else datetime.strptime(date.today().strftime("%Y-%m-01"), "%Y-%m-%d")
    )
    start_date = end_date - relativedelta(months=1)
    logger.info(f"📆Today {today} Date range: {start_date} → {end_date}")
    main(start_date,end_date, today)






