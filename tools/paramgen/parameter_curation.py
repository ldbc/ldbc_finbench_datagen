#!/usr/bin/env python3
import warnings

# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)
from ast import literal_eval
from calendar import timegm
import multiprocessing
import random
import pandas as pd
import numpy as np
import search_params
import time_select
import os
import codecs
from datetime import date
from glob import glob
import concurrent.futures
from functools import partial

THRESH_HOLD = 0
THRESH_HOLD_6 = 0
TRUNCATION_LIMIT = 10000
BATCH_SIZE = 5000
TRUNCATION_ORDER = "TIMESTAMP_ASCENDING"
TIME_TRUNCATE = True

table_dir = '../../out/factor_table'
out_dir = '../../out/substitute_parameters/'

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
        # transfer_in_amounts = account_amount_df.loc[account_list]['amount']
        for item in account_list:
            try:
                rows_account_list = account_account_df.loc[item][item_name]
            except KeyError:
                rows_account_list = []

            try:
                rows_amount_bucket = amount_bucket_df.loc[item]
            except KeyError:
                rows_amount_bucket = None

            try:
                transfer_in_amount = account_amount_df.loc[item]['amount']
            except KeyError:
                transfer_in_amount = 0
            result.update(neighbors_with_truncate_threshold(transfer_in_amount, rows_account_list, rows_amount_bucket, num_list))

    elif query_id in [1, 2, 5]:
        for item in account_list:
            try:
                rows_account_list = account_account_df.loc[item][item_name]
            except KeyError:
                rows_account_list = []
            try:
                rows_amount_bucket = amount_bucket_df.loc[item]
            except KeyError:
                rows_amount_bucket = None
            result.update(neighbors_with_trancate(rows_account_list, rows_amount_bucket, num_list))

    elif query_id in [3, 11]:
        for item in account_list:
            try:
                rows_account_list = account_account_df.loc[item][item_name]
            except KeyError:
                rows_account_list = []
            result.update(rows_account_list)

    return list(result)


def filter_neighbors(account_list, amount_bucket_df, num_list, account_id):

    # print(f'account_list: {account_list}')

    rows_amount_bucket = amount_bucket_df.loc[account_id]

    sum_num = 0
    header_at_limit = -1
    for col in reversed(num_list):
        sum_num += rows_amount_bucket[col]
        if sum_num >= TRUNCATION_LIMIT:
            header_at_limit = col
            break

    partial_apply = partial(filter_neighbors_with_truncate_threshold, amount_bucket_df=amount_bucket_df, num_list=num_list, header_at_limit=header_at_limit)
    account_mapped = map(partial_apply, account_list)
    result = [x for x in account_mapped if x is not None]
    return result


def filter_neighbors_with_truncate_threshold(item, amount_bucket_df, num_list, header_at_limit):

    if header_at_limit != -1:
        if item[1] > THRESH_HOLD_6 and item[1] >= header_at_limit:
            return item[0]
    else:
        if item[1] > THRESH_HOLD_6:
            return item[0]
        
    return None


def neighbors_with_truncate_threshold(transfer_in_amount, rows_account_list, rows_amount_bucket, num_list):
    if rows_amount_bucket is None:
        return []
    threshold_value = transfer_in_amount * THRESH_HOLD
    temp = [ata for ata in rows_account_list if ata[1] > threshold_value]

    # truncate at truncationLimit
    sum_num = 0
    header_at_limit = -1
    for col in reversed(num_list):
        sum_num += rows_amount_bucket[col]
        if sum_num >= TRUNCATION_LIMIT:
            header_at_limit = col
            break

    if header_at_limit != -1:
        return [t[0] for t in temp if t[1] >= header_at_limit]
    else:
        return [t[0] for t in temp]
    

def neighbors_with_trancate(rows_account_list, rows_amount_bucket, num_list):
    if rows_amount_bucket is None:
        return []
    # truncate at truncationLimit
    sum_num = 0
    header_at_limit = -1
    for col in reversed(num_list):
        sum_num += rows_amount_bucket[col]
        if sum_num >= TRUNCATION_LIMIT:
            header_at_limit = col
            break

    if header_at_limit != -1:
        return [t[0] for t in rows_account_list if t[1] >= header_at_limit]
    else:
        return [t[0] for t in rows_account_list]


def process_get_neighbors(chunk, account_account_df, account_amount_df, amount_bucket_df, num_list, query_id):
    second_column_name = chunk.columns[1]
    chunk[second_column_name] = chunk[second_column_name].apply(
        lambda x: find_neighbors(x, account_account_df, account_amount_df, amount_bucket_df, num_list, query_id)
    )
    return chunk


def process_filter_neighbors(chunk, amount_bucket_df, num_list):

    first_column_name = chunk.columns[0]
    second_column_name = chunk.columns[1]

    chunk[second_column_name] = chunk.apply(
        lambda row: filter_neighbors(row[second_column_name], amount_bucket_df, num_list, row[first_column_name]),
        axis=1
    )
    return chunk


def get_next_neighbor_list(neighbors_df, account_account_df, account_amount_df, amount_bucket_df, query_id):

    num_list = []
    if query_id != 3 and query_id != 11:
        num_list = [x for x in amount_bucket_df.columns.tolist()]

    query_parallelism = max(1, multiprocessing.cpu_count() // 4)
    # print(f'query_id {query_id} query_parallelism {query_parallelism}')
    chunks = np.array_split(neighbors_df, query_parallelism)

    with concurrent.futures.ProcessPoolExecutor(max_workers=query_parallelism) as executor:
        futures = [executor.submit(process_get_neighbors, chunk, account_account_df, account_amount_df, amount_bucket_df, num_list, query_id) for chunk in chunks]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
        executor.shutdown(wait=True)
    
    next_neighbors_df = pd.concat(results)
    next_neighbors_df = next_neighbors_df.sort_index()
    return next_neighbors_df


def get_filter_neighbor_list(neighbors_df, amount_bucket_df):
    first_column_name = neighbors_df.columns[0]
    num_list = [x for x in amount_bucket_df.columns.tolist()]

    query_parallelism = max(1, multiprocessing.cpu_count() // 4)
    chunks = np.array_split(neighbors_df, query_parallelism)

    with concurrent.futures.ProcessPoolExecutor(max_workers=query_parallelism) as executor:
        futures = [executor.submit(process_filter_neighbors, chunk, amount_bucket_df, num_list) for chunk in chunks]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

        executor.shutdown(wait=True)

    next_neighbors_df = pd.concat(results)
    next_neighbors_df = next_neighbors_df.sort_values(by=first_column_name)
    return next_neighbors_df


def process_batch(batch, basic_sum_df, first_column_name, second_column_name):
    neighbors_exploded = batch.explode(second_column_name)
    merged_df = neighbors_exploded.merge(
        basic_sum_df, left_on=second_column_name, right_index=True, how='left'
    ).drop(columns=[second_column_name])
    return merged_df.groupby(first_column_name).sum()

def get_next_sum_table(neighbors_df, basic_sum_df, batch_size=BATCH_SIZE):
    first_column_name = neighbors_df.columns[0]
    second_column_name = neighbors_df.columns[1]

    batches = [neighbors_df.iloc[start:start + batch_size] for start in range(0, len(neighbors_df), batch_size)]
    
    result_list = []
    query_parallelism = max(1, multiprocessing.cpu_count() // 4)
    with concurrent.futures.ProcessPoolExecutor(max_workers=query_parallelism) as executor:
        futures = [executor.submit(process_batch, batch, basic_sum_df, first_column_name, second_column_name) for batch in batches]
        for future in futures:
            result_list.append(future.result())
        executor.shutdown(wait=True)

    result_df = pd.concat(result_list).groupby(first_column_name).sum().astype(int)

    return result_df


def handleThresholdParam(threshold):
    return str(threshold)

def handleThreshold2Param(threshold2):
    return str(threshold2)

def hendleIdParam(id):
    return str(id)

def handleTruncateLimitParam(truncateLimit):
    return str(truncateLimit)

def handleTruncateOrderParam(truncateOrder):
    return truncateOrder


def handleTimeDurationParam(timeParam):
    start = timegm(date(year=int(timeParam.year), month=int(timeParam.month), day=int(timeParam.day)).timetuple())*1000
    end = start + timeParam.duration * 3600 * 24 * 1000
    res = str(start) + "|" + str(end)
    return res


def process_query(query_id):

    if query_id in [7, 10]:
        process_1_hop_query(query_id)
    elif query_id in [1, 2, 3, 5, 8, 11]:
        process_iter_queries(query_id)
    elif query_id == 6:
        process_withdraw_query()


def process_iter_queries(query_id):

    upstream_amount_df = None
    upstream_amount_path = None
    amount_bucket_path = None
    amount_bucket_df = None
    steps = None

    if query_id == 8:
        first_account_path = os.path.join(table_dir, 'loan_account_list')
        account_account_path = os.path.join(table_dir, 'trans_withdraw_items')
        upstream_amount_path = os.path.join(table_dir, 'upstream_amount')
        if TIME_TRUNCATE:
            amount_bucket_path = os.path.join(table_dir, 'trans_withdraw_month')
        else:
            amount_bucket_path = os.path.join(table_dir, 'trans_withdraw_bucket')
        time_bucket_path = os.path.join(table_dir, 'trans_withdraw_month')
        output_path = os.path.join(out_dir, 'tcr8.txt')
        steps = 2

    elif query_id == 1:
        first_account_path = os.path.join(table_dir, 'account_transfer_out_list')
        account_account_path = os.path.join(table_dir, 'account_transfer_out_items')
        if TIME_TRUNCATE:
            amount_bucket_path = os.path.join(table_dir, 'transfer_out_month')
        else:
            amount_bucket_path = os.path.join(table_dir, 'transfer_out_bucket')
        time_bucket_path = os.path.join(table_dir, 'transfer_out_month')
        output_path = os.path.join(out_dir, 'tcr1.txt')
        steps = 2

    elif query_id == 5:
        first_account_path = os.path.join(table_dir, 'person_account_list')
        account_account_path = os.path.join(table_dir, 'account_transfer_out_items')
        if TIME_TRUNCATE:
            amount_bucket_path = os.path.join(table_dir, 'transfer_out_month')
        else:
            amount_bucket_path = os.path.join(table_dir, 'transfer_out_bucket')
        time_bucket_path = os.path.join(table_dir, 'transfer_out_month')
        output_path = out_dir
        steps = 3

    elif query_id == 2:
        first_account_path = os.path.join(table_dir, 'person_account_list')
        account_account_path = os.path.join(table_dir, 'account_transfer_in_items')
        if TIME_TRUNCATE:
            amount_bucket_path = os.path.join(table_dir, 'transfer_in_month')
        else:
            amount_bucket_path = os.path.join(table_dir, 'transfer_in_bucket')
        time_bucket_path = os.path.join(table_dir, 'transfer_in_month')
        output_path = os.path.join(out_dir, 'tcr2.txt')
        steps = 3

    elif query_id == 3:
        first_account_path = os.path.join(table_dir, 'account_in_out_list')
        account_account_path = os.path.join(table_dir, 'account_in_out_list')
        amount_bucket_path = os.path.join(table_dir, 'account_in_out_count')
        time_bucket_path = os.path.join(table_dir, 'account_in_out_month')
        output_path = out_dir
        steps = 1

    elif query_id == 11:
        first_account_path = os.path.join(table_dir, 'person_guarantee_list')
        account_account_path = os.path.join(table_dir, 'person_guarantee_list')
        amount_bucket_path = os.path.join(table_dir, 'person_guarantee_count')
        time_bucket_path = os.path.join(table_dir, 'person_guarantee_month')
        output_path = os.path.join(out_dir, 'tcr11.txt')
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
        if query_id in [3, 11]:
            temp_first_array = next_first_amount_bucket.to_numpy()
        else:
            temp_first_array = next_first_amount_bucket.to_numpy().sum(axis=1)
        first_array = np.column_stack((first_array, temp_first_array))

        if current_step == steps - 1:
            next_time_bucket = get_next_sum_table(first_neighbors_df, time_bucket_df)
        else:
            first_neighbors_df = get_next_neighbor_list(first_neighbors_df, account_account_df, upstream_amount_df, amount_bucket_df, query_id)

        current_step += 1

    final_first_items = search_params.generate(first_array, 0.01)
    time_list = time_select.findTimeParams(final_first_items, next_time_bucket)
    truncate_limit_list = [TRUNCATION_LIMIT] * len(final_first_items)
    truncate_order_list = [TRUNCATION_ORDER] * len(final_first_items)
    thresh_list = [THRESH_HOLD] * len(final_first_items)

    if query_id == 8:
        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter.registerHandler(handleThresholdParam, thresh_list, "threshold")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")
        csvWriter.writeCSV()

        print(f'query_id {query_id} finished')

    elif query_id == 1:
        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")
        csvWriter.writeCSV()

        print(f'query_id {query_id} finished')

    elif query_id == 5:
        csvWriter_5 = CSVSerializer()
        csvWriter_5.setOutputFile(output_path + 'tcr5.txt')
        csvWriter_5.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter_5.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter_5.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter_5.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")

        csvWriter_12 = CSVSerializer()
        csvWriter_12.setOutputFile(output_path + 'tcr12.txt')
        csvWriter_12.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter_12.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter_12.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter_12.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")

        csvWriter_5.writeCSV()
        csvWriter_12.writeCSV()

        print(f'query_id 5 and 12 finished')

    elif query_id == 2:
        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")
        csvWriter.writeCSV()

        print(f'query_id {query_id} finished')

    elif query_id == 3:
        final_second_items_3 = []
        final_second_items_4 = []

        for account_id in final_first_items:
            j = 0
            k = 0
            while True:
                j = random.randint(0, len(final_first_items)-1)
                k = random.randint(0, len(final_first_items)-1)
                if final_first_items[j] != account_id and final_first_items[k] != account_id:
                    break
            final_second_items_3.append(final_first_items[j])
            final_second_items_4.append(final_first_items[k])
        
        csvWriter_3 = CSVSerializer()
        csvWriter_3.setOutputFile(output_path + 'tcr3.txt')
        csvWriter_3.registerHandler(hendleIdParam, final_first_items, "id1")
        csvWriter_3.registerHandler(hendleIdParam, final_second_items_3, "id2")
        csvWriter_3.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter_3.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter_3.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")
        
        csvWriter_4 = CSVSerializer()
        csvWriter_4.setOutputFile(output_path + 'tcr4.txt')
        csvWriter_4.registerHandler(hendleIdParam, final_first_items, "id1")
        csvWriter_4.registerHandler(hendleIdParam, final_second_items_4, "id2")
        csvWriter_4.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter_4.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter_4.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")

        csvWriter_3.writeCSV()
        csvWriter_4.writeCSV()

        print(f'query_id 3 and 4 finished')

    elif query_id == 11:
        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")
        csvWriter.writeCSV()

        print(f'query_id {query_id} finished')


def process_1_hop_query(query_id):

    if query_id == 7:
        first_count_path = os.path.join(table_dir, 'account_in_out_count')
        time_bucket_path = os.path.join(table_dir, 'account_in_out_month')
        output_path = out_dir
    elif query_id == 10:
        first_count_path = os.path.join(table_dir, 'person_invest_company')
        time_bucket_path = os.path.join(table_dir, 'invest_month')
        output_path = os.path.join(out_dir, 'tcr10.txt')

    first_count_df = process_csv(first_count_path)
    time_bucket_df = process_csv(time_bucket_path)

    first_time_name = time_bucket_df.columns[0]

    time_bucket_df.set_index(first_time_name, inplace=True)

    first_array = first_count_df.to_numpy()
    final_first_items = search_params.generate(first_array, 0.01)
    time_list = time_select.findTimeParams(final_first_items, time_bucket_df)

    truncate_limit_list = [TRUNCATION_LIMIT] * len(final_first_items)
    truncate_order_list = [TRUNCATION_ORDER] * len(final_first_items)
    thresh_list = [THRESH_HOLD] * len(final_first_items)

    if query_id == 7:

        csvWriter_7 = CSVSerializer()
        csvWriter_7.setOutputFile(output_path + 'tcr7.txt')
        csvWriter_7.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter_7.registerHandler(handleThresholdParam, thresh_list, "threshold")
        csvWriter_7.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter_7.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter_7.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")

        csvWriter_9 = CSVSerializer()
        csvWriter_9.setOutputFile(output_path + 'tcr9.txt')
        csvWriter_9.registerHandler(hendleIdParam, final_first_items, "id")
        csvWriter_9.registerHandler(handleThresholdParam, thresh_list, "threshold")
        csvWriter_9.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter_9.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
        csvWriter_9.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")

        csvWriter_7.writeCSV()
        csvWriter_9.writeCSV()

        print(f'query_id 7 and 9 finished')
    
    elif query_id == 10:
        final_second_items = []
        for person in final_first_items:
            j = 0
            while True:
                j = random.randint(0, len(final_first_items)-1)
                if final_first_items[j] != person:
                    break
            final_second_items.append(final_first_items[j])

        csvWriter = CSVSerializer()
        csvWriter.setOutputFile(output_path)
        csvWriter.registerHandler(hendleIdParam, final_first_items, "pid1")
        csvWriter.registerHandler(hendleIdParam, final_second_items, "pid2")
        csvWriter.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
        csvWriter.writeCSV()

        print(f'query_id {query_id} finished')


def process_withdraw_query():

    first_account_path = os.path.join(table_dir, 'account_withdraw_in_items')
    time_bucket_path = os.path.join(table_dir, 'transfer_in_month')
    if TIME_TRUNCATE:
        withdraw_bucket_path = os.path.join(table_dir, 'withdraw_in_month')
    else:
        withdraw_bucket_path = os.path.join(table_dir, 'withdraw_in_bucket')
    transfer_bucket_path = os.path.join(table_dir, 'transfer_in_bucket')
    output_path = os.path.join(out_dir, 'tcr6.txt')

    first_account_df = process_csv(first_account_path)
    time_bucket_df = process_csv(time_bucket_path)
    transfer_bucket_df = process_csv(transfer_bucket_path)
    withdraw_bucket_df = process_csv(withdraw_bucket_path)

    first_column_name = first_account_df.columns[0]
    second_column_name = first_account_df.columns[1]
    withdraw_first_name = withdraw_bucket_df.columns[0]
    transfer_first_name = transfer_bucket_df.columns[0]
    time_first_name = time_bucket_df.columns[0]

    withdraw_bucket_df.set_index(withdraw_first_name, inplace=True)
    transfer_bucket_df.set_index(transfer_first_name, inplace=True)
    time_bucket_df.set_index(time_first_name, inplace=True)
    
    first_account_df[second_column_name] = first_account_df[second_column_name].apply(literal_eval)
    first_neighbors_df = first_account_df.sort_values(by=first_column_name)

    first_neighbors_df = get_filter_neighbor_list(first_neighbors_df, withdraw_bucket_df)

    first_array = first_neighbors_df[first_column_name].to_numpy()
    next_first_amount_bucket = get_next_sum_table(first_neighbors_df, transfer_bucket_df)
    temp_first_array = next_first_amount_bucket.to_numpy().sum(axis=1)
    first_array = np.column_stack((first_array, temp_first_array))
    next_time_bucket = get_next_sum_table(first_neighbors_df, time_bucket_df)

    final_first_items = search_params.generate(first_array, 0.01)
    time_list = time_select.findTimeParams(final_first_items, next_time_bucket)

    truncate_limit_list = [TRUNCATION_LIMIT] * len(final_first_items)
    truncate_order_list = [TRUNCATION_ORDER] * len(final_first_items)
    thresh_list = [THRESH_HOLD_6] * len(final_first_items)

    csvWriter = CSVSerializer()
    csvWriter.setOutputFile(output_path)
    csvWriter.registerHandler(hendleIdParam, final_first_items, "id")
    csvWriter.registerHandler(handleThresholdParam, thresh_list, "threshold1")
    csvWriter.registerHandler(handleThreshold2Param, thresh_list, "threshold2")
    csvWriter.registerHandler(handleTimeDurationParam, time_list, "startTime|endTime")
    csvWriter.registerHandler(handleTruncateLimitParam, truncate_limit_list, "truncationLimit")
    csvWriter.registerHandler(handleTruncateOrderParam, truncate_order_list, "truncationOrder")
    csvWriter.writeCSV()

    print(f'query_id 6 finished')


def main():
    queries = [3, 1, 8, 7, 10, 11, 2, 5, 6]
    # queries = [3]

    multiprocessing.set_start_method('forkserver')
    processes = []

    for query_id in queries:
        p = multiprocessing.Process(target=process_query, args=(query_id,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
