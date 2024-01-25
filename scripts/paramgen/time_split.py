import os
import pandas as pd
from datetime import datetime, timedelta


def process_csv(file_path):
    df = pd.read_csv(file_path, delimiter='|')
    return df

def timestamp_to_year_month(timestamp):
    return timestamp.strftime('%Y-%m')

account_folder_path = '../../out/raw/account'
transfer_folder_path = '../../out/raw/transfer'
withdraw_folder_path = '../../out/raw/withdraw'
output_folder = '../../out/factor_table'

account_files = [os.path.join(account_folder_path, file) for file in os.listdir(account_folder_path) if file.endswith('.csv')]
transfer_files = [os.path.join(transfer_folder_path, file) for file in os.listdir(transfer_folder_path) if file.endswith('.csv')]
withdraw_files = [os.path.join(withdraw_folder_path, file) for file in os.listdir(withdraw_folder_path) if file.endswith('.csv')]

account_df = pd.concat([process_csv(file) for file in account_files])
transfer_df = pd.concat([process_csv(file) for file in transfer_files])
withdraw_df = pd.concat([process_csv(file) for file in withdraw_files])

start_date = datetime(2020, 1, 1)
end_date = datetime(2023, 1, 1)

month_ranges = pd.date_range(start=start_date, end=end_date, freq='MS')

result_dict = {}

for account_id in account_df['id']:
    account_month_count = {timestamp_to_year_month(month): 0 for month in month_ranges}

    account_transfer_data = transfer_df[transfer_df['fromId'] == account_id]

    for _, row in account_transfer_data.iterrows():
        month = timestamp_to_year_month(pd.to_datetime(row['createTime'], unit='ms'))
        account_month_count[month] += 1

    account_withdraw_data = withdraw_df[withdraw_df['fromId'] == account_id]

    for _, row in account_withdraw_data.iterrows():
        month = timestamp_to_year_month(pd.to_datetime(row['createTime'], unit='ms'))
        account_month_count[month] += 1

    result_dict[account_id] = list(account_month_count.values())


result_df = pd.DataFrame.from_dict(result_dict, orient='index', columns=[timestamp_to_year_month(month) for month in month_ranges])
result_df.reset_index(inplace=True)
result_df.columns = ['account_id'] + [str(timestamp_to_year_month(month)) for month in month_ranges]


os.makedirs(output_folder, exist_ok=True)
result_df.to_csv('../../out/factor_table/month.csv', sep='|', index=False)
