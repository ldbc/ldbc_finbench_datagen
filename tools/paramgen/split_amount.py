import os
import pandas as pd


def process_csv(file_path):
    df = pd.read_csv(file_path, delimiter='|')
    return df

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

amount_buckets = [10000, 30000, 100000, 300000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000]

result_dict = {}

for account_id in account_df['id']:
    account_amount_count = {str(bucket): 0 for bucket in amount_buckets}

    account_transfer_data = transfer_df[transfer_df['fromId'] == account_id]

    for _, row in account_transfer_data.iterrows():
        amount = row['amount']
        for bucket in amount_buckets:
            if amount <= bucket:
                account_amount_count[str(bucket)] += 1
                break

    account_withdraw_data = withdraw_df[withdraw_df['fromId'] == account_id]

    for _, row in account_withdraw_data.iterrows():
        amount = row['amount']
        for bucket in amount_buckets:
            if amount <= bucket:
                account_amount_count[str(bucket)] += 1
                break

    result_dict[account_id] = list(account_amount_count.values())


result_df = pd.DataFrame.from_dict(result_dict, orient='index', columns=[str(bucket) for bucket in amount_buckets])
result_df.reset_index(inplace=True)
result_df.columns = ['account_id'] + [str(bucket) for bucket in amount_buckets]

os.makedirs(output_folder, exist_ok=True)
result_df.to_csv('../../out/factor_table/amount_bucket.csv', sep='|', index=False)
