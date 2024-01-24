import os
import pandas as pd


def process_csv(file_path):
    df = pd.read_csv(file_path, delimiter='|')
    return df


account_folder_path = '../../out/raw/account'
transfer_folder_path = '../../out/raw/transfer'
output_folder = '../../out/factor_table'
withdraw_folder_path = '../../out/raw/withdraw'

account_files = [os.path.join(account_folder_path, file) for file in os.listdir(account_folder_path) if file.endswith('.csv')]
transfer_files = [os.path.join(transfer_folder_path, file) for file in os.listdir(transfer_folder_path) if file.endswith('.csv')]
withdraw_files = [os.path.join(withdraw_folder_path, file) for file in os.listdir(withdraw_folder_path) if file.endswith('.csv')]

account_df = pd.concat([process_csv(file) for file in account_files])
transfer_df = pd.concat([process_csv(file) for file in transfer_files])
withdraw_df = pd.concat([process_csv(file) for file in withdraw_files])

merged_df = pd.merge(account_df, transfer_df, left_on='id', right_on='toId', how='left')

result_amount_df = merged_df.groupby('id')['amount'].sum().reset_index().fillna(0)

account_items = []

for account_id in account_df['id']:
    transfer_data = transfer_df[transfer_df['fromId'] == account_id].groupby('toId')['amount'].max().reset_index()
    withdraw_data = withdraw_df[withdraw_df['fromId'] == account_id].groupby('toId')['amount'].max().reset_index()
    
    max_amounts = pd.concat([transfer_data, withdraw_data], ignore_index=True).groupby('toId')['amount'].max().reset_index()
    
    items = [[to_id, max_amount] for to_id, max_amount in zip(max_amounts['toId'], max_amounts['amount'])]

    account_items.append([account_id, items])

os.makedirs(output_folder, exist_ok=True)

result_amount_df.to_csv(os.path.join(output_folder, 'amount.csv'), sep='|', index=False, header=['account_id', 'amount'])
result_df = pd.DataFrame(account_items, columns=['account_id', 'items'])
result_df.to_csv(os.path.join(output_folder, 'account_items.csv'), sep='|', index=False)
