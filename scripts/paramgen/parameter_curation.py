#!/usr/bin/env python3

from ast import literal_eval
from calendar import timegm
import pandas as pd
import numpy as np
import search_params
import time_select
import os
import codecs
from datetime import date
from glob import glob
import concurrent.futures

THRESH_HOLD = 0
TRUNCATION_LIMIT = 10000

def process_csv(file_path):
    all_files = glob(file_path + '/*.csv')
    
    df_list = []
    
    for filename in all_files:
        df = pd.read_csv(filename, delimiter='|')
        df_list.append(df)
    
    combined_df = pd.concat(df_list, ignore_index=True)
    
    return combined_df


class CSVSerializer:
    def __init__(self):
        self.handlers = []
        self.inputs = []

    def setOutputFile(self, outputFile):
        self.outputFile=outputFile

    def registerHandler(self, handler, inputParams, header):
        handler.header = header
        self.handlers.append(handler)
        self.inputs.append(inputParams)

    def writeCSV(self):
        dir_path = os.path.dirname(self.outputFile)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        output = codecs.open( self.outputFile, "w",encoding="utf-8")

        if len(self.inputs) == 0:
            return

        headers = [self.handlers[j].header for j in range(len(self.handlers))]
        output.write("|".join(headers))
        output.write("\n")

        for i in range(len(self.inputs[0])):
            # compile a single CSV line from multiple handlers
            csvLine = []
            for j in range(len(self.handlers)):
                handler = self.handlers[j]
                data = self.inputs[j][i]
                csvLine.append(handler(data))
            output.write('|'.join([s for s in csvLine]))
            output.write("\n")
        output.close()


def find_neighbors(account_list, account_account_df, account_amount_df, amount_bucket_df, num_list):
    temp = []
    result = set()

    # edge amount > upstream * threshold
    for item in account_list:
        rows_account_list = account_account_df.loc[item]
        ata_list = rows_account_list['items']
        rows_amount_bucket = amount_bucket_df.loc[item]
        transfer_in_amount = account_amount_df.loc[item]['amount']
        for ata in ata_list:
            if ata[1] > transfer_in_amount * THRESH_HOLD:
                temp.append(ata)
        # truncate at truncationLimit
        sum_num = 0
        header_at_limit = -1
        for col in reversed(num_list):
            sum_num += rows_amount_bucket[str(col)]
            if sum_num >= TRUNCATION_LIMIT:
                header_at_limit = col
                break
        for t in temp:
            if header_at_limit != -1 and t[1] < header_at_limit: 
                continue
            result.add(t[0])
    
    return list(result)


# def get_next_neighbor_list(neighbors_df, account_account_df, account_amount_df, amount_bucket_df):
#     next_neighbors_df = neighbors_df
#     num_list = [int(x) for x in amount_bucket_df.iloc[0].index.tolist()[1:]]
#     next_neighbors_df['account_list'] = next_neighbors_df['account_list'].apply(lambda x: find_neighbors(x, account_account_df, account_amount_df, amount_bucket_df, num_list))
#     return next_neighbors_df


def process_chunk(chunk, account_account_df, account_amount_df, amount_bucket_df, num_list):
    chunk['account_list'] = chunk['account_list'].apply(
        lambda x: find_neighbors(x, account_account_df, account_amount_df, amount_bucket_df, num_list)
    )
    return chunk


def get_next_neighbor_list(neighbors_df, account_account_df, account_amount_df, amount_bucket_df):
    num_list = [int(x) for x in amount_bucket_df.columns.tolist()]
    
    chunks = np.array_split(neighbors_df, 8)  
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, chunk, account_account_df, account_amount_df, amount_bucket_df, num_list) for chunk in chunks]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    next_neighbors_df = pd.concat(results)
    next_neighbors_df = next_neighbors_df.sort_index()
    return next_neighbors_df


def get_next_sum_table(neighbors_df, basic_sum_df):
    # result_data = []
    # for index, row in neighbors_df.iterrows():
    #     loan_id = row['loan_id']
    #     account_list = row['account_list']
    #     add_frame = basic_sum_df.loc[basic_sum_df.index.isin(account_list)]
    #     add_frame = add_frame.rename_axis('loan_id')
    #     sum_result = add_frame.sum(axis=0).astype(int)
    #     sum_result['loan_id'] = loan_id
    #     result_data.append(sum_result.to_dict())
    # return pd.DataFrame(result_data)
    
    neighbors_exploded = neighbors_df.explode('account_list')
    merged_df = neighbors_exploded.merge(basic_sum_df, left_on='account_list', right_index=True, how='left').drop(columns=['account_list'])
    result_df = merged_df.groupby('loan_id').sum().astype(int)

    return result_df


def handleLoanParam(loan):
    return str(loan)


def handleTimeDurationParam(timeParam):
    start = timegm(date(year=int(timeParam.year), month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000
    end = start + timeParam.duration * 3600 * 24 * 1000
    res = str(start) + "|" + str(end)
    return res


def main():

    loan_account_path = '../../out/factor_table/loan_account_list'
    account_account_path = '../../out/factor_table/trans_withdraw_items'
    account_amount_path = '../../out/factor_table/upstream_amount'
    amount_bucket_path = '../../out/factor_table/trans_withdraw_bucket'
    time_bucket_path = '../../out/factor_table/trans_withdraw_month'
    output_path = '../../out/substitute_parameters/'


    loan_account_df = process_csv(loan_account_path)
    account_account_df = process_csv(account_account_path)
    account_amount_df = process_csv(account_amount_path)
    amount_bucket_df = process_csv(amount_bucket_path)
    time_bucket_df = process_csv(time_bucket_path)
    account_account_df['items'] = account_account_df['items'].apply(literal_eval)
    loan_account_df['account_list'] = loan_account_df['account_list'].apply(literal_eval)

    account_account_df.set_index('account_id', inplace=True)
    amount_bucket_df.set_index('account_id', inplace=True)
    time_bucket_df.set_index('account_id', inplace=True)
    account_amount_df.set_index('account_id', inplace=True)
    
    steps = 3
    current_step = 0
    loan_neighbors_df = loan_account_df.sort_values(by='loan_id')
    loan_array = loan_neighbors_df['loan_id'].to_numpy()
    next_time_bucket = None

    while current_step < steps:

        next_loan_amount_bucket = get_next_sum_table(loan_neighbors_df, amount_bucket_df)
        # next_loan_amount_bucket.set_index('loan_id', inplace=True)
        temp_loan_array = next_loan_amount_bucket.to_numpy().sum(axis=1)
        loan_array = np.column_stack((loan_array, temp_loan_array))

        if current_step == steps - 1:
            next_time_bucket = get_next_sum_table(loan_neighbors_df, time_bucket_df)
            # next_time_bucket.set_index('loan_id', inplace=True)

            # print(neighbors_df)
            # print(next_amount_bucket)
            # print(next_time_bucket)
            # print(loan_array)
            
        else:
            loan_neighbors_df = get_next_neighbor_list(loan_neighbors_df, account_account_df, account_amount_df, amount_bucket_df)

        current_step += 1


    final_loan = search_params.generate(loan_array, 0.01)
    time_list = time_select.findTimeParams(final_loan, next_time_bucket)

    csvWriter = CSVSerializer()
    csvWriter.setOutputFile(output_path + "tcr8.txt")
    csvWriter.registerHandler(handleLoanParam, final_loan, "loanId")
    csvWriter.registerHandler(handleTimeDurationParam, time_list, "startDate|endDate")

    csvWriter.writeCSV()


if __name__ == "__main__":
    main()
