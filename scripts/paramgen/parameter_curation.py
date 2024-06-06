#!/usr/bin/env python3

from ast import literal_eval
from calendar import timegm
import multiprocessing
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


def find_neighbors(account_list, account_account_df, account_amount_df, amount_bucket_df, num_list, query_id):
    result = set()
    item_name = account_account_df.columns[0]

    if query_id == 8:
        transfer_in_amounts = account_amount_df.loc[account_list]['amount']
        for item in account_list:
            rows_account_list = account_account_df.loc[item][item_name]
            rows_amount_bucket = amount_bucket_df.loc[item]
            transfer_in_amount = transfer_in_amounts[item]
            result.update(neighbors_with_truncate_threshold(transfer_in_amount, rows_account_list, rows_amount_bucket, num_list))

    elif query_id in [1, 2, 5]:
        for item in account_list:
            rows_account_list = account_account_df.loc[item][item_name]
            rows_amount_bucket = amount_bucket_df.loc[item]
            result.update(neighbors_with_trancate(rows_account_list, rows_amount_bucket, num_list))

    return list(result)


def neighbors_with_truncate_threshold(transfer_in_amount, rows_account_list, rows_amount_bucket, num_list):
    threshold_value = transfer_in_amount * THRESH_HOLD
    temp = [ata for ata in rows_account_list if ata[1] > threshold_value]

    # truncate at truncationLimit
    sum_num = 0
    header_at_limit = -1
    for col in reversed(num_list):
        sum_num += rows_amount_bucket[str(col)]
        if sum_num >= TRUNCATION_LIMIT:
            header_at_limit = col
            break

    if header_at_limit != -1:
        return [t[0] for t in temp if t[1] >= header_at_limit]
    else:
        return [t[0] for t in temp]
    

def neighbors_with_trancate(rows_account_list, rows_amount_bucket, num_list):

    # truncate at truncationLimit
    sum_num = 0
    header_at_limit = -1
    for col in reversed(num_list):
        sum_num += rows_amount_bucket[str(col)]
        if sum_num >= TRUNCATION_LIMIT:
            header_at_limit = col
            break

    if header_at_limit != -1:
        return [t[0] for t in rows_account_list if t[1] >= header_at_limit]
    else:
        return [t[0] for t in rows_account_list]


def process_chunk(chunk, account_account_df, account_amount_df, amount_bucket_df, num_list, query_id):
    second_column_name = chunk.columns[1]
    chunk[second_column_name] = chunk[second_column_name].apply(
        lambda x: find_neighbors(x, account_account_df, account_amount_df, amount_bucket_df, num_list, query_id)
    )
    return chunk


def get_next_neighbor_list(neighbors_df, account_account_df, account_amount_df, amount_bucket_df, query_id):
    num_list = [int(x) for x in amount_bucket_df.columns.tolist()]

    query_parallelism = max(1, multiprocessing.cpu_count() // 4)
    print(f'query_parallelism: {query_parallelism}')
    chunks = np.array_split(neighbors_df, query_parallelism)

    with concurrent.futures.ProcessPoolExecutor(max_workers=query_parallelism) as executor:
        futures = [executor.submit(process_chunk, chunk, account_account_df, account_amount_df, amount_bucket_df, num_list, query_id) for chunk in chunks]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    next_neighbors_df = pd.concat(results)
    next_neighbors_df = next_neighbors_df.sort_index()
    return next_neighbors_df


def get_next_sum_table(neighbors_df, basic_sum_df):
    first_column_name = neighbors_df.columns[0]
    second_column_name = neighbors_df.columns[1]
    neighbors_exploded = neighbors_df.explode(second_column_name)
    merged_df = neighbors_exploded.merge(basic_sum_df, left_on=second_column_name, right_index=True, how='left').drop(columns=[second_column_name])
    result_df = merged_df.groupby(first_column_name).sum().astype(int)

    return result_df


def handleLoanParam(loan):
    return str(loan)


def handleTimeDurationParam(timeParam):
    start = timegm(date(year=int(timeParam.year), month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000
    end = start + timeParam.duration * 3600 * 24 * 1000
    res = str(start) + "|" + str(end)
    return res


def process_query(query_config):
    query_id = query_config['query_id']
    process_function = query_config['function']
    process_function(query_id)


def process_iter_queries(query_id):

    upstream_amount_df = None
    upstream_amount_path = None
    amount_bucket_path = None
    amount_bucket_df = None
    steps = None

    if query_id == 8:
        first_account_path = '../../out/factor_table/loan_account_list'
        account_account_path = '../../out/factor_table/trans_withdraw_items'
        upstream_amount_path = '../../out/factor_table/upstream_amount'
        amount_bucket_path = '../../out/factor_table/trans_withdraw_bucket'
        time_bucket_path = '../../out/factor_table/trans_withdraw_month'
        output_path = '../../out/substitute_parameters/tcr8.txt'
        steps = 3

    elif query_id == 1:
        first_account_path = '../../out/factor_table/account_transfer_out_list'
        account_account_path = '../../out/factor_table/account_transfer_out_items'
        amount_bucket_path = '../../out/factor_table/transfer_out_bucket'
        time_bucket_path = '../../out/factor_table/transfer_out_month'
        output_path = '../../out/substitute_parameters/tcr1.txt'
        steps = 2

    elif query_id == 5:
        first_account_path = '../../out/factor_table/person_account_list'
        account_account_path = '../../out/factor_table/account_transfer_out_items'
        amount_bucket_path = '../../out/factor_table/transfer_out_bucket'
        time_bucket_path = '../../out/factor_table/transfer_out_month'
        output_path = '../../out/substitute_parameters/'
        steps = 3
    
    elif query_id == 2:
        first_account_path = '../../out/factor_table/person_account_list'
        account_account_path = '../../out/factor_table/account_transfer_in_items'
        amount_bucket_path = '../../out/factor_table/transfer_in_bucket'
        time_bucket_path = '../../out/factor_table/transfer_in_month'
        output_path = '../../out/substitute_parameters/tcr2.txt'
        steps = 3

    first_account_df = process_csv(first_account_path)
    account_account_df = process_csv(account_account_path)
    if query_id == 8:
        upstream_amount_df = process_csv(upstream_amount_path)
        first_upstream_name = upstream_amount_df.columns[0]
        upstream_amount_df.set_index(first_upstream_name, inplace=True)

    amount_bucket_df = process_csv(amount_bucket_path)
    time_bucket_df = process_csv(time_bucket_path)
    first_column_name = first_account_df.columns[0]
    second_column_name = first_account_df.columns[1]
    first_account_name = account_account_df.columns[0]
    second_account_name = account_account_df.columns[1]
    first_amount_name = amount_bucket_df.columns[0]
    first_time_name = time_bucket_df.columns[0]  

    account_account_df[second_account_name] = account_account_df[second_account_name].apply(literal_eval)
    first_account_df[second_column_name] = first_account_df[second_column_name].apply(literal_eval)

    account_account_df.set_index(first_account_name, inplace=True)
    amount_bucket_df.set_index(first_amount_name, inplace=True)
    time_bucket_df.set_index(first_time_name, inplace=True)

    current_step = 0

    first_neighbors_df = first_account_df.sort_values(by=first_column_name)
    first_array = first_neighbors_df[first_column_name].to_numpy()

    next_time_bucket = None

    while current_step < steps:
        next_first_amount_bucket = get_next_sum_table(first_neighbors_df, amount_bucket_df)
        temp_first_array = next_first_amount_bucket.to_numpy().sum(axis=1)
        first_array = np.column_stack((first_array, temp_first_array))

        if current_step == steps - 1:
            next_time_bucket = get_next_sum_table(first_neighbors_df, time_bucket_df)
        else:
            first_neighbors_df = get_next_neighbor_list(first_neighbors_df, account_account_df, upstream_amount_df, amount_bucket_df, query_id)

        current_step += 1

    final_first_items = search_params.generate(first_array, 0.01)
    time_list = time_select.findTimeParams(final_first_items, next_time_bucket)

    if query_id == 8:
        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(handleLoanParam, final_first_items, "loanId")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startDate|endDate")
        csvWriter.writeCSV()

    elif query_id == 1:
        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(handleLoanParam, final_first_items, "account_id")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startDate|endDate")
        csvWriter.writeCSV()

    elif query_id == 5:
        csvWriter_5 = CSVSerializer()
        csvWriter_5.setOutputFile(output_path + 'tcr5.txt')
        csvWriter_5.registerHandler(handleLoanParam, final_first_items, "personId")
        csvWriter_5.registerHandler(handleTimeDurationParam, time_list, "startDate|endDate")

        csvWriter_12 = CSVSerializer()
        csvWriter_12.setOutputFile(output_path + 'tcr12.txt')
        csvWriter_12.registerHandler(handleLoanParam, final_first_items, "personId")
        csvWriter_12.registerHandler(handleTimeDurationParam, time_list, "startDate|endDate")

        csvWriter_5.writeCSV()
        csvWriter_12.writeCSV()

    elif query_id == 2:
        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(handleLoanParam, final_first_items, "personId")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startDate|endDate")
        csvWriter.writeCSV()


def process_iter_query_without_truncate(query_id):

    account_out_path = '../../out/factor_table/account_transfer_out_list'
    account_in_path = '../../out/factor_table/account_transfer_in_list'
    amount_bucket_out_path = '../../out/factor_table/transfer_out_bucket'
    amount_bucket_in_path = '../../out/factor_table/transfer_in_bucket'
    time_bucket_out_path = '../../out/factor_table/transfer_out_month'
    time_bucket_in_path = '../../out/factor_table/transfer_in_month'
    output_path = '../../out/substitute_parameters/'

    account_out_df = process_csv(account_out_path)
    account_in_df = process_csv(account_in_path)
    amount_bucket_out_df = process_csv(amount_bucket_out_path)
    amount_bucket_in_df = process_csv(amount_bucket_in_path)
    time_bucket_out_df = process_csv(time_bucket_out_path)
    time_bucket_in_df = process_csv(time_bucket_in_path)

    second_out_column_name = account_out_df.columns[1]
    second_in_column_name = account_in_df.columns[1]

    account_out_df[second_out_column_name] = account_out_df[second_out_column_name].apply(literal_eval)
    account_in_df[second_in_column_name] = account_in_df[second_in_column_name].apply(literal_eval)
    account_account_out_df = account_out_df.copy()
    account_account_in_df = account_in_df.copy()

    first_out_time_name = time_bucket_out_df.columns[0]
    first_in_time_name = time_bucket_in_df.columns[0]
    first_out_account_name = account_account_out_df.columns[0]
    first_in_account_name = account_account_in_df.columns[0]
    first_out_amount_name = amount_bucket_out_df.columns[0]
    first_in_amount_name = amount_bucket_in_df.columns[0]

    account_account_out_df.set_index(first_out_account_name, inplace=True)
    account_account_in_df.set_index(first_in_account_name, inplace=True)
    time_bucket_out_df.set_index(first_out_time_name, inplace=True)
    time_bucket_in_df.set_index(first_in_time_name, inplace=True)
    amount_bucket_out_df.set_index(first_out_amount_name, inplace=True)
    amount_bucket_in_df.set_index(first_in_amount_name, inplace=True)

    current_step = 0
    steps = 2

    first_out_neighbors_df = account_out_df.sort_values(by=first_out_account_name)
    first_out_array = first_out_neighbors_df[first_out_account_name].to_numpy()

    first_in_neighbors_df = account_in_df.sort_values(by=first_in_account_name)
    first_in_array = first_in_neighbors_df[first_in_account_name].to_numpy()

    next_out_time_bucket = None
    next_in_time_bucket = None

    while current_step < steps:

        next_first_out_amount_bucket = get_next_sum_table(first_out_neighbors_df, amount_bucket_out_df)
        temp_first_out_array = next_first_out_amount_bucket.to_numpy().sum(axis=1)
        first_out_array = np.column_stack((first_out_array, temp_first_out_array))

        next_first_in_amount_bucket = get_next_sum_table(first_in_neighbors_df, amount_bucket_in_df)
        temp_first_in_array = next_first_in_amount_bucket.to_numpy().sum(axis=1)
        first_in_array = np.column_stack((first_in_array, temp_first_in_array))

        if current_step == steps - 1:
            next_out_time_bucket = get_next_sum_table(first_out_neighbors_df, time_bucket_out_df)
            next_in_time_bucket = get_next_sum_table(first_in_neighbors_df, time_bucket_in_df)
        else:
            first_out_neighbors_df = neighbors_without_trancate(first_out_neighbors_df, account_account_out_df)
            first_in_neighbors_df = neighbors_without_trancate(first_in_neighbors_df, account_account_in_df)

        current_step += 1

    final_first_out_items = search_params.generate(first_out_array, 0.01)
    final_first_in_items = search_params.generate(first_in_array, 0.01)
    time_list_out = time_select.findTimeParams(final_first_out_items, next_out_time_bucket)
    time_list_in = time_select.findTimeParams(final_first_in_items, next_in_time_bucket)




def neighbors_without_trancate(first_neighbors_df, account_account_df):

    first_column_name = first_neighbors_df.columns[0]
    second_column_name = first_neighbors_df.columns[1]
    neighbors_exploded = first_neighbors_df.explode(second_column_name)
    merged_df = neighbors_exploded.merge(account_account_df, left_on=second_column_name, right_index=True, how='left').drop(columns=[second_column_name])
    result_df = merged_df.groupby(first_column_name).agg(lambda x: list(set(x)))

    return result_df


def main():
    queries = [
        {
            'query_id': 8,
            'function': process_iter_queries,
        },
        {
            'query_id': 1,
            'function': process_iter_queries,
        },
        {
            'query_id': 5,
            'function': process_iter_queries,
        },
        {
            'query_id': 2,
            'function': process_iter_queries,
        },
        # {
        #     'query_id': 3,
        #     'function': process_iter_query_without_truncate,
        # }
        # {
        #     'query_id': 1,
        #     'function': process_query_1_5_12,
        # }
        # {
        #     'query_id': 8,
        #     'function': process_query_8,
        # },
        # {
        #     'query_id': 8,
        #     'function': process_query_8,
        # },
        # {
        #     'query_id': 8,
        #     'function': process_query_8,
        # },
        # {
        #     'query_id': 8,
        #     'function': process_query_8,
        # }
    ]

    multiprocessing.set_start_method('spawn')
    processes = []

    for query_config in queries:
        p = multiprocessing.Process(target=process_query, args=(query_config,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    # with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, multiprocessing.cpu_count())) as executor:
    #     futures = []
    #     for query_config in queries:
    #         futures.append(executor.submit(process_query, query_config))



if __name__ == "__main__":
    main()
