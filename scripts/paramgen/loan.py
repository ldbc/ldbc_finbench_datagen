import os
import pandas as pd

def process_csv(file_path):
    df = pd.read_csv(file_path, delimiter='|')
    return df

loan_folder_path = '../../out/raw/loan'
deposit_folder_path = '../../out/raw/deposit'
output_folder = '../../out/factor_table'

loan_files = [os.path.join(loan_folder_path, file) for file in os.listdir(loan_folder_path) if file.endswith('.csv')]
deposit_files = [os.path.join(deposit_folder_path, file) for file in os.listdir(deposit_folder_path) if file.endswith('.csv')]

loan_df = pd.concat([process_csv(file) for file in loan_files])
deposit_df = pd.concat([process_csv(file) for file in deposit_files])

result_list = []

for loan_id in loan_df['id'].unique():
    account_list = deposit_df[deposit_df['loanId'] == loan_id]['accountId'].unique().tolist()
    result_list.append([loan_id, account_list])

result_df = pd.DataFrame(result_list, columns=['loan_id', 'account_list'])

os.makedirs(output_folder, exist_ok=True)
result_df.to_csv('../../out/factor_table/loan_account_list.csv', sep='|', index=False)
