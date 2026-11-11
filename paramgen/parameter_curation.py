#!/usr/bin/env python3
#
# Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import codecs
import multiprocessing
import os
import random
import sys
from ast import literal_eval
from calendar import timegm
from collections import defaultdict
from datetime import date, datetime
from functools import partial
from glob import glob
import concurrent.futures

import numpy as np
import pandas as pd
import search_params
import time_select

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TABLE_DIR = sys.argv[1]
OUT_DIR   = sys.argv[2]
random.seed(42)

TRUNCATION_LIMIT = 500
THRESH_HOLD      = 0
THRESH_HOLD_6    = 0
TIME_TRUNCATE    = True
TRUNCATION_ORDER = "TIMESTAMP_DESCENDING" if TIME_TRUNCATE else "AMOUNT_DESCENDING"
BATCH_SIZE       = 5000


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def factor_path(*parts):
    return os.path.join(TABLE_DIR, *parts)

def output_path(filename):
    return os.path.join(OUT_DIR, filename)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def read_csv(file_path):
    if os.path.isfile(file_path):
        return pd.read_csv(file_path, delimiter='|')

    all_files = sorted(glob(os.path.join(file_path, '*.csv')))
    if not all_files:
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Factor table path does not exist: {file_path}. "
                "Please regenerate factor_table outputs before running paramgen."
            )
        available = sorted(os.listdir(file_path))
        raise FileNotFoundError(
            f"No CSV files found under factor table path: {file_path}. "
            f"Available entries: {available}. "
            "Please check the factor generation output format and rerun if needed."
        )

    return pd.concat([pd.read_csv(f, delimiter='|') for f in all_files], ignore_index=True)


def load_indexed_list_df(file_path):
    df = read_csv(file_path)
    key_col, val_col = df.columns[0], df.columns[1]
    df[val_col] = df[val_col].apply(literal_eval)
    df.set_index(key_col, inplace=True)
    return df


def load_person_account_df(file_path):
    df = read_csv(file_path)
    list_col = df.columns[1]
    df[list_col] = df[list_col].apply(literal_eval)
    return df


def load_loan_month_account_map(file_path):
    df = read_csv(file_path)
    if len(df.columns) < 3:
        return {}

    loan_col, month_col, account_col = df.columns[0], df.columns[1], df.columns[2]
    df[account_col] = df[account_col].apply(literal_eval)

    result = defaultdict(dict)
    for loan_id, month_start, account_list in df[[loan_col, month_col, account_col]].itertuples(index=False, name=None):
        accounts = [int(a) for a in account_list]
        if accounts:
            result[int(loan_id)][int(month_start)] = accounts
    return dict(result)


# ---------------------------------------------------------------------------
# Graph traversal helpers
# ---------------------------------------------------------------------------

def neighbor_id(item):
    return int(item[0]) if isinstance(item, (list, tuple)) else int(item)

def neighbor_time(item):
    return int(item[2]) if isinstance(item, (list, tuple)) and len(item) >= 3 else None

def month_start_ms(timestamp_ms):
    dt = datetime.utcfromtimestamp(int(timestamp_ms) / 1000)
    return timegm(date(dt.year, dt.month, 1).timetuple()) * 1000

def get_neighbors(indexed_df, key):
    try:
        row = indexed_df.loc[key]
    except KeyError:
        return []
    col = indexed_df.columns[0]
    if isinstance(row, pd.DataFrame):
        values = row[col].tolist()
        flat = []
        for v in values:
            flat.extend(v) if isinstance(v, list) else flat.append(v)
        return flat
    values = row[col]
    if isinstance(values, list):
        return values
    if isinstance(values, pd.Series):
        return values.tolist()
    return []


def truncate_neighbors(neighbors, limit=TRUNCATION_LIMIT):
    if len(neighbors) <= limit:
        return neighbors
    if TIME_TRUNCATE:
        neighbors = sorted(neighbors,
                           key=lambda item: neighbor_time(item) or 0,
                           reverse=True)
    else:
        neighbors = sorted(neighbors,
                           key=lambda item: (item[1] if isinstance(item, (list, tuple))
                                                        and len(item) >= 2 else 0),
                           reverse=True)
    return neighbors[:limit]


def collect_path_stats(indexed_df, current_id, prev_ts, visited, depth,
                       max_depth=3, truncation_limit=TRUNCATION_LIMIT,
                       target_month=None, months_out=None):
    reachable, count = set(), 0
    if depth >= max_depth:
        return reachable, count
    neighbors = truncate_neighbors(get_neighbors(indexed_df, current_id),
                                   truncation_limit)
    for item in neighbors:
        dst = neighbor_id(item)
        ts  = neighbor_time(item)
        if ts is None or dst in visited or ts <= prev_ts:
            continue
        if target_month is not None and month_start_ms(ts) != target_month:
            continue
        if months_out is not None:
            months_out.add(month_start_ms(ts))
        reachable.add(dst)
        count += 1
        sub_r, sub_c = collect_path_stats(
            indexed_df, dst, ts, visited | {dst}, depth + 1,
            max_depth=max_depth, truncation_limit=truncation_limit,
            target_month=target_month, months_out=months_out,
                                 )
        reachable.update(sub_r)
        count += sub_c
    return reachable, count


def collect_month_matches_increasing(indexed_df, current_id, prev_ts, visited, depth,
                                     qualifying_df, match_ids, hit_counts, traversed_counts,
                                     target_month=None, max_depth=3,
                                     truncation_limit=TRUNCATION_LIMIT):
    traversed = 0
    if depth >= max_depth:
        return traversed
    neighbors = truncate_neighbors(get_neighbors(indexed_df, current_id),
                                   truncation_limit)
    for item in neighbors:
        dst = neighbor_id(item)
        ts  = neighbor_time(item)
        if ts is None or dst in visited or ts <= prev_ts:
            continue
        month = month_start_ms(ts)
        if target_month is not None and month != target_month:
            continue
        traversed += 1
        traversed_counts[month] += 1
        if _has_month_activity(qualifying_df, dst, month):
            match_ids[month].add(dst)
            hit_counts[month] += 1
        sub = collect_month_matches_increasing(
            indexed_df, dst, ts, visited | {dst}, depth + 1,
            qualifying_df, match_ids, hit_counts, traversed_counts,
            target_month=target_month, max_depth=max_depth,
            truncation_limit=truncation_limit,
                                 )
        traversed += sub
    return traversed


def collect_month_matches_decreasing(indexed_df, current_id, prev_ts, visited, depth,
                                     qualifying_df, match_ids, hit_counts,
                                     match_months=None, traversed_counts=None,
                                     max_depth=3, truncation_limit=TRUNCATION_LIMIT):
    traversed = 0
    if depth >= max_depth:
        return traversed
    neighbors = truncate_neighbors(get_neighbors(indexed_df, current_id),
                                   truncation_limit)
    for item in neighbors:
        src = neighbor_id(item)
        ts  = neighbor_time(item)
        if ts is None or src in visited or ts >= prev_ts:
            continue
        traversed += 1
        month = month_start_ms(ts)
        if traversed_counts is not None:
            traversed_counts[month] += 1
        if _has_month_activity(qualifying_df, src, month):
            match_ids[month].add(src)
            hit_counts[month] += 1
            if match_months is not None:
                match_months[month].add(month)
        sub = collect_month_matches_decreasing(
            indexed_df, src, ts, visited | {src}, depth + 1,
            qualifying_df, match_ids, hit_counts,
            match_months=match_months, traversed_counts=traversed_counts,
            max_depth=max_depth, truncation_limit=truncation_limit,
                                 )
        traversed += sub
    return traversed


def _has_month_activity(month_df, item_id, month_start):
    try:
        row = month_df.loc[item_id]
    except KeyError:
        return False
    return _get_month_count(row, month_start) > 0


def _get_month_count(row, month_start):
    key = str(int(month_start))
    if isinstance(row, pd.DataFrame):
        row = row.iloc[0]
    for k in (key, int(month_start)):
        if k in row.index:
            try:
                return float(row[k])
            except (TypeError, ValueError):
                return 0
    return 0


# ---------------------------------------------------------------------------
# Candidate selection
# ---------------------------------------------------------------------------

def select_candidates(first_array, portion=0.01, min_size=1):
    if len(first_array) == 0:
        return []
    if len(first_array) == 1:
        return [first_array[0][0]]
    sample_size = min(max(min_size, int(len(first_array) * portion)), len(first_array))
    if sample_size == len(first_array):
        return [row[0] for row in first_array]
    return search_params.generate(first_array, sample_size / len(first_array))


def random_distinct(current_id, pool):
    candidates = [c for c in pool if c != current_id]
    return random.choice(candidates) if candidates else current_id


# ---------------------------------------------------------------------------
# Unified CSV writer
# ---------------------------------------------------------------------------

def write_params(path, ids, time_list, *, threshold=None, threshold2=None,
                 id_col="id", id2_list=None, id2_col="id2",
                 truncate_limit=True, truncate_order=True):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    rows = []
    for i, (item_id, tp) in enumerate(zip(ids, time_list)):
        row = [str(item_id)]
        if id2_list is not None:
            row.append(str(id2_list[i]))
        if threshold is not False:
            row.append(str(THRESH_HOLD if threshold is None else threshold))
        if threshold2 is not False and threshold2 is not None:
            row.append(str(threshold2))
        row.append(_format_time(tp))
        if truncate_limit:
            row.append(str(TRUNCATION_LIMIT))
        if truncate_order:
            row.append(TRUNCATION_ORDER)
        rows.append(row)

    header = [id_col]
    if id2_list is not None:
        header.append(id2_col)
    if threshold is not False:
        header.append("threshold" if threshold2 is None else "threshold1")
    if threshold2 is not None and threshold2 is not False:
        header.append("threshold2")
    header.append("startTime|endTime")
    if truncate_limit:
        header.append("truncationLimit")
    if truncate_order:
        header.append("truncationOrder")

    with codecs.open(path, "w", encoding="utf-8") as f:
        f.write("|".join(header) + "\n")
        for row in rows:
            f.write("|".join(row) + "\n")


def _format_time(tp):
    if hasattr(tp, 'start_ms') and hasattr(tp, 'end_ms'):
        return f"{int(tp.start_ms)}|{int(tp.end_ms)}"
    start = timegm(date(int(tp.year), int(tp.month), int(tp.day)).timetuple()) * 1000
    return f"{start}|{start + tp.duration * 3600 * 24 * 1000}"


# ---------------------------------------------------------------------------
# Per-query candidate builders  (pure logic, no I/O)
# ---------------------------------------------------------------------------

def _candidates_query1(transfer_out_df, blocked_signin_month_df):
    candidate_rows = []
    for src_id in transfer_out_df.index.unique():
        src_id = int(src_id)
        match_ids = defaultdict(set)
        match_ranges = {}
        hit_counts = defaultdict(int)
        traversed_counts = defaultdict(int)
        _collect_q1_with_range(
            transfer_out_df, src_id, -1, {src_id}, 0,
            blocked_signin_month_df, match_ids, hit_counts, traversed_counts,
            match_ranges, first_month=None, path_min=None, path_max=None,
        )
        for month, ids in match_ids.items():
            mn, mx = match_ranges.get(month, (month, month))
            candidate_rows.append([(src_id, int(month), int(mn), int(mx)),
                                   traversed_counts[month], hit_counts[month], len(ids)])
    if not candidate_rows:
        return [], []
    first_array = np.array(candidate_rows, dtype=object)
    first_array = _filter_first_array_for_sr6(
        first_array, lambda r: r[0][0], lambda r: r[0][1])
    selected = select_candidates(first_array, 0.05)
    ids = [int(s[0]) for s in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(s[2]), int(s[3])) for s in selected])
    return ids, time_list


def _collect_q1_with_range(indexed_df, current_id, prev_ts, visited, depth,
                           qualifying_df, match_ids, hit_counts, traversed_counts,
                           match_ranges, first_month, path_min, path_max,
                           max_depth=3, truncation_limit=TRUNCATION_LIMIT):
    if depth >= max_depth:
        return
    neighbors = truncate_neighbors(get_neighbors(indexed_df, current_id),
                                   truncation_limit)
    for item in neighbors:
        dst = neighbor_id(item)
        ts  = neighbor_time(item)
        if ts is None or dst in visited or ts <= prev_ts:
            continue
        month = month_start_ms(ts)
        fm = month if first_month is None else first_month
        pmin = month if path_min is None else min(path_min, month)
        pmax = month if path_max is None else max(path_max, month)

        traversed_counts[fm] += 1
        if _has_month_activity(qualifying_df, dst, month):
            match_ids[fm].add(dst)
            hit_counts[fm] += 1
            old = match_ranges.get(fm)
            if old is None:
                match_ranges[fm] = (pmin, pmax)
            else:
                match_ranges[fm] = (min(old[0], pmin), max(old[1], pmax))

        _collect_q1_with_range(
            indexed_df, dst, ts, visited | {dst}, depth + 1,
            qualifying_df, match_ids, hit_counts, traversed_counts,
            match_ranges, first_month=fm, path_min=pmin, path_max=pmax,
            max_depth=max_depth, truncation_limit=truncation_limit,
                                 )


def _candidates_query2(person_account_df, transfer_in_df, loan_deposit_month_df):
    person_col, account_col = person_account_df.columns[0], person_account_df.columns[1]
    stats = {}
    for _, row in person_account_df.iterrows():
        person_id = int(row[person_col])
        for account_id in row[account_col]:
            account_id = int(account_id)
            match_ids, hit_counts = defaultdict(set), defaultdict(int)
            match_months = defaultdict(set)
            traversed = collect_month_matches_decreasing(
                transfer_in_df, account_id, sys.maxsize, {account_id}, 0,
                loan_deposit_month_df, match_ids, hit_counts,
                match_months=match_months,
            )
            for month, ids in match_ids.items():
                key = (person_id, int(month))
                if key not in stats:
                    stats[key] = dict(other_ids=set(), hit_count=0,
                                      account_hits=set(), traversed=0,
                                      months=set())
                stats[key]['other_ids'].update(int(i) for i in ids)
                stats[key]['hit_count']    += hit_counts[month]
                stats[key]['account_hits'].add(account_id)
                stats[key]['traversed']    += traversed
                stats[key]['months'].update(match_months.get(month, {month}))
    if not stats:
        return [], []
    candidate_rows = []
    for (pid, month), s in stats.items():
        months = s['months'] or {month}
        candidate_rows.append([(pid, min(months), max(months)),
                               s['traversed'], len(s['other_ids']),
                               s['hit_count'], len(s['account_hits'])])
    first_array = np.array(candidate_rows, dtype=object)
    selected = select_candidates(first_array, 0.01)
    ids = [int(s[0]) for s in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(s[1]), int(s[2])) for s in selected])
    return ids, time_list


def _candidates_query3(transfer_out_df, pool_ids, pool_times,
                       friendly_accounts, scan_cap=10_000):
    transfer_col = transfer_out_df.columns[0]
    adjacency = {
        int(src): [(neighbor_id(item), neighbor_time(item)) for item in items
                   if isinstance(item, (list, tuple)) and len(item) >= 3]
        for src, items in transfer_out_df[transfer_col].items()
    }
    time_by_id = {int(account_id): tp
                  for account_id, tp in zip(pool_ids, pool_times)}
    candidate_rows = []

    for src_id in map(int, pool_ids):
        tp = time_by_id[src_id]
        frontier = [src_id]
        visited = {src_id}
        layers = defaultdict(list)
        scanned = 0

        for depth in range(1, 4):
            next_frontier = []
            for account_id in frontier:
                for dst_id, ts in adjacency.get(account_id, []):
                    scanned += 1
                    if scanned > scan_cap:
                        break
                    if ts is None or not (tp.start_ms < ts < tp.end_ms):
                        continue
                    if dst_id not in visited:
                        visited.add(dst_id)
                        next_frontier.append(dst_id)
                        layers[depth].append((dst_id, scanned))
                if scanned > scan_cap:
                    break
            if scanned > scan_cap:
                break
            frontier = next_frontier

        if scanned > scan_cap:
            continue

        reachable = layers[2] + layers[3]
        candidates = ([item for item in reachable
                       if item[0] in friendly_accounts] or reachable)
        if not candidates:
            continue
        dst_id, cost = candidates[0]
        candidate_rows.append([(src_id, dst_id), cost])

    if len(candidate_rows) < 4:
        selected = [row[0] for row in candidate_rows]
    else:
        selected = search_params.generate(
            np.array(candidate_rows, dtype=object), 0.10)
    ids = [int(src_id) for src_id, _ in selected]
    id2_list = [int(dst_id) for _, dst_id in selected]
    time_list = [time_by_id[src_id] for src_id in ids]
    return ids, id2_list, time_list


def _candidates_query4(transfer_out_df, transfer_in_df):
    out_by_src      = defaultdict(set)
    in_by_dst       = defaultdict(set)
    pair_count      = defaultdict(int)
    pair_months     = defaultdict(list)

    for src_id in transfer_out_df.index.unique():
        src_id = int(src_id)
        for item in get_neighbors(transfer_out_df, src_id):
            dst = neighbor_id(item); ts = neighbor_time(item)
            if ts is None or dst == src_id: continue
            out_by_src[src_id].add(dst)
            pair_count[(src_id, dst)] += 1
            pair_months[(src_id, dst)].append(month_start_ms(ts))

    for dst_id in transfer_in_df.index.unique():
        dst_id = int(dst_id)
        for item in get_neighbors(transfer_in_df, dst_id):
            src = neighbor_id(item); ts = neighbor_time(item)
            if ts is None or src == dst_id: continue
            in_by_dst[dst_id].add(src)

    candidate_rows = []
    for src_id, direct_dsts in out_by_src.items():
        incoming = in_by_dst.get(src_id, set())
        if not incoming: continue
        for dst_id in direct_dsts:
            outgoing_from_dst = out_by_src.get(dst_id, set())
            cycles = outgoing_from_dst & incoming - {src_id, dst_id}
            if not cycles: continue
            e1 = pair_count.get((src_id, dst_id), 0)
            e2 = sum(pair_count.get((o, src_id), 0) for o in cycles)
            e3 = sum(pair_count.get((dst_id, o), 0) for o in cycles)

            all_months = list(pair_months.get((src_id, dst_id), []))
            for o in cycles:
                all_months.extend(pair_months.get((o, src_id), []))
                all_months.extend(pair_months.get((dst_id, o), []))
            if not all_months:
                continue
            min_month = min(all_months)
            max_month = max(all_months)
            candidate_rows.append([(src_id, dst_id, min_month, max_month),
                                   len(cycles), e1+e2+e3, e1])

    if not candidate_rows:
        return [], [], []

    first_array = np.array(candidate_rows, dtype=object)
    selected = select_candidates(first_array, 0.30)
    src_ids   = [int(s) for s, _, _, _ in selected]
    dst_ids   = [int(d) for _, d, _, _ in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(mn), int(mx)) for _, _, mn, mx in selected])
    return src_ids, dst_ids, time_list


def _candidates_query5(person_account_df, transfer_out_df):
    person_col, account_col = person_account_df.columns[0], person_account_df.columns[1]
    stats = {}
    for _, row in person_account_df.iterrows():
        person_id = int(row[person_col])
        for account_id in row[account_col]:
            account_id = int(account_id)
            for item in get_neighbors(transfer_out_df, account_id):
                dst = neighbor_id(item); ts = neighbor_time(item)
                if ts is None or dst == account_id: continue
                month = month_start_ms(ts)
                key = (person_id, month)
                if key not in stats:
                    stats[key] = dict(dst_ids=set(), path_count=0,
                                      account_hits=set(), months=set())
                stats[key]['dst_ids'].add(dst)
                stats[key]['path_count'] += 1
                stats[key]['account_hits'].add(account_id)
                stats[key]['months'].add(month)
                sub_months = set()
                sub_r, sub_c = collect_path_stats(
                    transfer_out_df, dst, ts, {account_id, dst}, 1,
                    months_out=sub_months)
                stats[key]['dst_ids'].update(sub_r)
                stats[key]['path_count'] += sub_c
                stats[key]['months'].update(sub_months)
    if not stats:
        return [], []
    candidate_rows = []
    for (pid, month), s in stats.items():
        months = s['months'] or {month}
        candidate_rows.append([(pid, min(months), max(months)),
                               s['path_count'], len(s['dst_ids']),
                               len(s['account_hits'])])
    first_array = np.array(candidate_rows, dtype=object)
    selected = select_candidates(first_array, 0.01)
    ids = [int(s[0]) for s in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(s[1]), int(s[2])) for s in selected])
    return ids, time_list


def _candidates_query6(withdraw_in_df, transfer_in_month_df):
    card_account_ids = _load_card_account_ids()
    if not card_account_ids:
        return [], []
    candidate_rows = []

    mid_total_transfers = {}
    for mid_id in transfer_in_month_df.index:
        row = transfer_in_month_df.loc[mid_id]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        total = sum(float(v) for v in row.values if isinstance(v, (int, float, np.integer, np.floating)))
        mid_total_transfers[int(mid_id)] = total

    for card_id in card_account_ids:
        month_stats = defaultdict(lambda: dict(mid_ids=set(), transfer_count=0,
                                               withdraw_count=0, withdraw_amount=0.0,
                                               months=set()))
        for item in get_neighbors(withdraw_in_df, card_id):
            mid = neighbor_id(item); ts = neighbor_time(item)
            if ts is None: continue
            try:
                month_row = transfer_in_month_df.loc[mid]
            except KeyError:
                continue
            if mid_total_transfers.get(mid, 0) <= 3:
                continue
            month = month_start_ms(ts)
            s = month_stats[month]
            s['mid_ids'].add(mid)
            s['transfer_count'] += int(mid_total_transfers.get(mid, 0))
            s['withdraw_count'] += 1
            s['months'].add(month)
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                s['withdraw_amount'] += float(item[1])
        for month, s in month_stats.items():
            if s['mid_ids']:
                months = s['months'] or {month}
                candidate_rows.append([(card_id, min(months), max(months)),
                                       len(s['mid_ids']), s['transfer_count'],
                                       s['withdraw_count'], s['withdraw_amount']])
    if not candidate_rows:
        return [], []
    first_array = np.array(candidate_rows, dtype=object)
    first_array = _filter_first_array_for_sr6(
        first_array, lambda r: r[0][0])
    selected = select_candidates(first_array, 0.01)
    ids = [int(s[0]) for s in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(s[1]), int(s[2])) for s in selected])
    return ids, time_list


def _bfs_window_cost(adj, seeds, start_ms, end_ms, max_depth=3,
                     per_node_scan=2 * TRUNCATION_LIMIT, cap=100000):
    frontier = set(seeds)
    inwin = 0
    scanned = 0
    for _ in range(max_depth):
        nxt = set()
        for v in frontier:
            edges = adj.get(v)
            if not edges:
                continue
            scanned += min(len(edges), per_node_scan)
            for dst, ts in edges[:per_node_scan]:
                if start_ms < ts < end_ms:
                    inwin += 1
                    nxt.add(dst)
        if scanned >= cap:
            return max(inwin, cap)
        frontier = nxt
    return inwin


def _candidates_query8(loan_month_account_map, trans_withdraw_df):
    adj = defaultdict(list)
    items_col = trans_withdraw_df.columns[0]
    for key, items in trans_withdraw_df[items_col].items():
        if isinstance(items, list):
            adj[int(key)].extend((int(i[0]), int(i[2])) for i in items
                                 if isinstance(i, (list, tuple)) and len(i) >= 3)

    keys = []
    for loan_id, month_accounts in loan_month_account_map.items():
        for month_start, account_ids in month_accounts.items():
            months = set()
            for account_id in account_ids:
                account_id = int(account_id)
                for item in get_neighbors(trans_withdraw_df, account_id):
                    dst = neighbor_id(item); ts = neighbor_time(item)
                    if ts is None or dst == account_id: continue
                    months.add(month_start_ms(ts))
                    collect_path_stats(
                        trans_withdraw_df, dst, ts, {account_id, dst}, 1,
                        months_out=months,
                    )
            win_min = int(month_start)
            win_max = max(max(months), win_min) if months else win_min
            keys.append((int(loan_id), win_min, win_max))
    if not keys:
        return [], []

    time_params = time_select.findTimeParamsForMonthRanges(
        [(win_min, win_max) for _, win_min, win_max in keys])
    candidate_rows = []
    for (loan_id, win_min, win_max), tp in zip(keys, time_params):
        seeds = {int(a) for m, accs in loan_month_account_map[loan_id].items()
                 if tp.start_ms <= int(m) < tp.end_ms for a in accs}
        cost = _bfs_window_cost(adj, seeds, tp.start_ms, tp.end_ms)
        candidate_rows.append([(loan_id, win_min, win_max), cost])

    target = max(1, int(len(candidate_rows) * 0.01))
    filtered = [r for r in candidate_rows if r[1] >= 100]
    if len(filtered) < target:
        filtered = sorted(candidate_rows, key=lambda r: -r[1])[:2 * target]
    first_array = np.array(filtered, dtype=object)
    selected = select_candidates(first_array, target / len(filtered))
    ids = [int(s[0]) for s in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(s[1]), int(s[2])) for s in selected])
    return ids, time_list


def _candidates_query12(person_account_df, transfer_out_df):
    company_ids = _load_company_account_ids()
    if not company_ids:
        return [], []
    person_col, account_col = person_account_df.columns[0], person_account_df.columns[1]
    factor_rows, time_counts = [], defaultdict(lambda: defaultdict(int))

    for _, row in person_account_df.iterrows():
        person_id = row[person_col]
        company_hits, edge_count = set(), 0
        for account_id in row[account_col]:
            for item in get_neighbors(transfer_out_df, account_id):
                dst = neighbor_id(item)
                if dst not in company_ids: continue
                edge_count += 1
                company_hits.add(dst)
                ts = neighbor_time(item)
                if ts is not None:
                    time_counts[person_id][month_start_ms(ts)] += 1
        if edge_count > 0:
            factor_rows.append([person_id, edge_count, len(company_hits)])

    if not factor_rows or not time_counts:
        return [], []

    first_array = np.array(factor_rows, dtype=object)
    ids = select_candidates(first_array, 0.01)
    time_bucket_df = _build_time_bucket_df(time_counts)
    time_list = time_select.findTimeParams(ids, time_bucket_df)
    return ids, time_list


# ---------------------------------------------------------------------------
# Helpers for iter-based queries (3/5/7/10/11)
# ---------------------------------------------------------------------------

def _iter_query_setup(query_id):
    if query_id == 5:
        return (
            factor_path('person_account_list'),
            factor_path('account_transfer_out_items'),
            factor_path('transfer_out_month' if TIME_TRUNCATE else 'transfer_out_bucket'),
            factor_path('transfer_out_month'),
            3,
        )
    if query_id == 3:
        return (
            factor_path('account_in_out_list'),
            factor_path('account_in_out_list'),
            factor_path('account_in_out_count'),
            factor_path('account_in_out_month'),
            1,
        )
    if query_id == 11:
        return (
            factor_path('person_guarantee_list'),
            factor_path('person_guarantee_list'),
            factor_path('person_guarantee_count'),
            factor_path('person_guarantee_month'),
            3,
        )
    raise ValueError(f"No iter setup for query_id={query_id}")


def _run_iter_pipeline(query_id, portion=0.01):
    first_path, acct_path, amount_path, time_path, steps = _iter_query_setup(query_id)

    first_df   = load_person_account_df(first_path)
    account_df = read_csv(acct_path)
    amount_df  = read_csv(amount_path)
    time_df    = read_csv(time_path)

    acct_col1, acct_col2 = account_df.columns[0], account_df.columns[1]
    first_col,  list_col = first_df.columns[0],   first_df.columns[1]
    amt_col  = amount_df.columns[0]
    time_col = time_df.columns[0]

    account_df[acct_col2] = account_df[acct_col2].apply(literal_eval)
    account_df.set_index(acct_col1, inplace=True)
    amount_df.set_index(amt_col, inplace=True)
    time_df.set_index(time_col, inplace=True)

    neighbors_df = first_df.sort_values(by=first_col)
    first_array  = neighbors_df[first_col].to_numpy()
    next_time_bucket = None

    for step in range(steps):
        next_amount = _get_next_sum_table(neighbors_df, amount_df)
        col = next_amount.to_numpy() if query_id in (3, 11) else next_amount.to_numpy().sum(axis=1)
        first_array = np.column_stack((first_array, col))
        if step == steps - 1:
            next_time_bucket = _get_next_sum_table(neighbors_df, time_df)
        else:
            neighbors_df = _get_next_neighbor_list(neighbors_df, account_df, None, amount_df, query_id)

    if query_id == 3:
        first_array = _filter_first_array_for_sr6(first_array, lambda r: r[0])
        next_time_bucket = _mask_time_bucket_to_sr6_months(next_time_bucket)
    ids = select_candidates(first_array, portion)
    time_list = time_select.findTimeParams(ids, next_time_bucket)
    return ids, time_list, first_df, account_df


# ---------------------------------------------------------------------------
# Per-query generate functions  (each one: load → build candidates → write)
# ---------------------------------------------------------------------------

def generate_query1():
    try:
        transfer_out_df       = load_indexed_list_df(factor_path('account_transfer_out_items'))
        blocked_signin_df     = read_csv(factor_path('blocked_signin_month'))
        blocked_signin_df.set_index(blocked_signin_df.columns[0], inplace=True)
        ids, time_list = _candidates_query1(transfer_out_df, blocked_signin_df)
        if ids:
            write_params(output_path('complex_1_param.csv'), ids, time_list,
                         threshold=False)
    except (FileNotFoundError, ValueError, KeyError, IndexError):
        pass


def generate_query2():
    try:
        person_account_df  = load_person_account_df(factor_path('person_account_list'))
        transfer_in_df     = load_indexed_list_df(factor_path('account_transfer_in_items'))
        loan_deposit_df    = read_csv(factor_path('account_loan_deposit_month'))
        loan_deposit_df.set_index(loan_deposit_df.columns[0], inplace=True)
        ids, time_list = _candidates_query2(person_account_df, transfer_in_df, loan_deposit_df)
        if ids:
            write_params(output_path('complex_2_param.csv'), ids, time_list,
                         threshold=False)
    except (FileNotFoundError, ValueError, KeyError, IndexError):
        pass


def generate_query3():
    ids, times, *_ = _run_iter_pipeline(3, portion=0.20)
    transfer_out_df = load_indexed_list_df(factor_path('account_transfer_out_items'))
    friendly_accounts = set(_get_sr6_friendly_months())
    ids, id2_list, time_list = _candidates_query3(
        transfer_out_df, ids, times, friendly_accounts)
    if ids:
        write_params(output_path('complex_3_param.csv'), ids, time_list,
                     threshold=False, id2_list=id2_list, id_col="id1", id2_col="id2",
                     truncate_limit=False, truncate_order=False)


def generate_query4():
    try:
        transfer_out_df  = load_indexed_list_df(factor_path('account_transfer_out_items'))
        transfer_in_df   = load_indexed_list_df(factor_path('account_transfer_in_items'))
        ids, dst_ids, time_list = _candidates_query4(transfer_out_df, transfer_in_df)
    except (FileNotFoundError, ValueError, KeyError, IndexError):
        ids, time_list, *_ = _run_iter_pipeline(3)
        dst_ids = _build_query4_pairs(ids)

    write_params(output_path('complex_4_param.csv'), ids, time_list,
                 threshold=False, id2_list=dst_ids, id_col="id1", id2_col="id2")


def generate_query5_and_12():
    person_account_df = load_person_account_df(factor_path('person_account_list'))
    transfer_out_df   = load_indexed_list_df(factor_path('account_transfer_out_items'))

    ids5, time_list5 = _candidates_query5(person_account_df, transfer_out_df)
    write_params(output_path('complex_5_param.csv'), ids5, time_list5,
                 threshold=False)

    ids12, time_list12 = _candidates_query12(person_account_df, transfer_out_df)
    write_params(output_path('complex_12_param.csv'), ids12, time_list12,
                 threshold=False)


def generate_query6():
    withdraw_in_df       = load_indexed_list_df(factor_path('account_withdraw_in_items'))
    transfer_in_month_df = read_csv(factor_path('transfer_in_month'))
    transfer_in_month_df.set_index(transfer_in_month_df.columns[0], inplace=True)
    ids, time_list = _candidates_query6(withdraw_in_df, transfer_in_month_df)
    write_params(output_path('complex_6_param.csv'), ids, time_list,
                 threshold=THRESH_HOLD_6, threshold2=THRESH_HOLD_6)


def generate_query7_and_9():
    ids, time_list = _run_1hop_pipeline('account_in_out_count', 'account_in_out_month',
                                        sr6_filter=True)
    for qid in (7, 9):
        write_params(output_path(f'complex_{qid}_param.csv'), ids, time_list)


def generate_query8():
    loan_map        = load_loan_month_account_map(factor_path('loan_deposit_account_month_list'))
    trans_withdraw  = load_indexed_list_df(factor_path('trans_withdraw_items'))
    ids, time_list  = _candidates_query8(loan_map, trans_withdraw)
    write_params(output_path('complex_8_param.csv'), ids, time_list)


def generate_query10():
    ids, time_list = _run_1hop_pipeline('person_invest_company', 'invest_month')
    id2_list = [_random_pair(ids, i) for i in range(len(ids))]
    write_params(output_path('complex_10_param.csv'), ids, time_list,
                 threshold=False, truncate_limit=False, truncate_order=False,
                 id2_list=id2_list, id_col="pid1", id2_col="pid2")


def generate_query11():
    ids, time_list, *_ = _run_iter_pipeline(11)
    write_params(output_path('complex_11_param.csv'), ids, time_list,
                 threshold=False)


# ---------------------------------------------------------------------------
# Supporting helpers
# ---------------------------------------------------------------------------

def _run_1hop_pipeline(count_table, month_table, sr6_filter=False):
    count_df = read_csv(factor_path(count_table))
    time_df  = read_csv(factor_path(month_table))
    time_df.set_index(time_df.columns[0], inplace=True)
    first_array = count_df.to_numpy()
    if sr6_filter:
        first_array = _filter_first_array_for_sr6(first_array, lambda r: r[0])
        time_df = _mask_time_bucket_to_sr6_months(time_df)
    ids = select_candidates(first_array, 0.01)
    time_list = time_select.findTimeParams(ids, time_df)
    return ids, time_list


def _random_pair(id_list, i):
    while True:
        j = random.randint(0, len(id_list) - 1)
        if id_list[j] != id_list[i]:
            return id_list[j]


def _build_query4_pairs(ids):
    transfer_out_df = load_indexed_list_df(factor_path('account_transfer_out_items'))
    transfer_in_df  = load_indexed_list_df(factor_path('account_transfer_in_items'))
    pool = list(ids)
    pairs = []
    for src_id in ids:
        incoming = {neighbor_id(i) for i in get_neighbors(transfer_in_df, src_id)}
        out_items = get_neighbors(transfer_out_df, src_id)
        scored = []
        for item in out_items:
            dst = neighbor_id(item)
            if dst == src_id: continue
            shared = incoming & {neighbor_id(i) for i in get_neighbors(transfer_out_df, dst)}
            if shared:
                scored.append((len(shared), dst))
        if scored:
            scored.sort(key=lambda x: (-x[0], x[1]))
            pairs.append(scored[0][1])
            continue
        out_ids = [neighbor_id(i) for i in out_items if neighbor_id(i) != src_id]
        pairs.append(random.choice(out_ids) if out_ids else random_distinct(src_id, pool))
    return pairs


def _load_card_account_ids():
    try:
        df = read_csv(factor_path('card_account_ids'))
    except (FileNotFoundError, ValueError):
        return set()
    if len(df) > 0:
        return set(df[df.columns[0]].dropna().astype(np.int64).tolist())
    return set()


# ---------------------------------------------------------------------------
# SR6 affordance filter
# ---------------------------------------------------------------------------

_SR6_FRIENDLY_MONTHS = None


def _load_sr6_friendly_account_months():
    try:
        transfer_in_df  = load_indexed_list_df(factor_path('account_transfer_in_items'))
        transfer_out_df = load_indexed_list_df(factor_path('account_transfer_out_items'))
        blocked_df      = read_csv(factor_path('blocked_signin_month'))
    except (FileNotFoundError, ValueError, KeyError):
        return {}

    blocked_ids = set(int(x) for x in blocked_df[blocked_df.columns[0]].dropna().tolist())
    if not blocked_ids:
        return {}

    mid_blocked_months = defaultdict(set)
    for mid in transfer_out_df.index.unique():
        mid_int = int(mid)
        for item in get_neighbors(transfer_out_df, mid):
            dst = neighbor_id(item)
            ts  = neighbor_time(item)
            if ts is None or dst == mid_int or dst not in blocked_ids:
                continue
            mid_blocked_months[mid_int].add(month_start_ms(ts))
    if not mid_blocked_months:
        return {}

    friendly = defaultdict(set)
    for src in transfer_in_df.index.unique():
        src_int = int(src)
        for item in get_neighbors(transfer_in_df, src):
            mid = neighbor_id(item)
            ts  = neighbor_time(item)
            if ts is None or mid == src_int:
                continue
            months = mid_blocked_months.get(mid)
            if not months:
                continue
            m = month_start_ms(ts)
            if m in months:
                friendly[src_int].add(m)
    return dict(friendly)


def _get_sr6_friendly_months():
    global _SR6_FRIENDLY_MONTHS
    if _SR6_FRIENDLY_MONTHS is None:
        _SR6_FRIENDLY_MONTHS = _load_sr6_friendly_account_months()
    return _SR6_FRIENDLY_MONTHS


def _filter_first_array_for_sr6(first_array, account_getter, month_getter=None):
    friendly = _get_sr6_friendly_months()
    if not friendly:
        return first_array
    kept = []
    for row in first_array:
        acc = int(account_getter(row))
        months = friendly.get(acc)
        if not months:
            continue
        if month_getter is not None and int(month_getter(row)) not in months:
            continue
        kept.append(row)
    if not kept:
        return first_array
    return np.array(kept, dtype=object)


def _mask_time_bucket_to_sr6_months(time_bucket_df):
    friendly = _get_sr6_friendly_months()
    if not friendly or time_bucket_df is None or len(time_bucket_df) == 0:
        return time_bucket_df
    month_cols = {}
    for col in time_bucket_df.columns:
        try:
            ts = int(str(col))
        except (TypeError, ValueError):
            continue
        if ts > 10**11:
            month_cols[col] = ts
    if not month_cols:
        return time_bucket_df
    df = time_bucket_df.copy()
    for idx in df.index:
        try:
            acc_int = int(idx)
        except (TypeError, ValueError):
            continue
        acc_friendly = friendly.get(acc_int)
        if not acc_friendly:
            continue
        for col, ms in month_cols.items():
            if ms not in acc_friendly:
                df.at[idx, col] = 0
    return df


def _load_company_account_ids():
    try:
        df = read_csv(factor_path('company_account_list'))
    except (FileNotFoundError, ValueError):
        return set()
    if len(df) >= 2:
        acct_col = df.columns[1]
        df[acct_col] = df[acct_col].apply(literal_eval)
        ids = set()
        for lst in df[acct_col]:
            ids.update(int(i) for i in lst)
        return ids
    return set()


def _build_time_bucket_df(time_counts_by_id):
    if not time_counts_by_id:
        return pd.DataFrame()
    normalized = {iid: {str(m): c for m, c in counts.items()}
                  for iid, counts in time_counts_by_id.items()}
    all_months = sorted({m for counts in normalized.values() for m in counts}, key=int)
    rows = {iid: {'__padding__': 0, **{m: counts.get(m, 0) for m in all_months}}
            for iid, counts in normalized.items()}
    return pd.DataFrame.from_dict(rows, orient='index').fillna(0).astype(int)


# ---------------------------------------------------------------------------
# Neighbor expansion (parallelized, unchanged logic)
# ---------------------------------------------------------------------------

def _find_neighbors(account_list, account_df, account_amount_df, amount_bucket_df, num_list, query_id):
    result = set()
    item_name = account_df.columns[0]
    if query_id == 8:
        for item in account_list:
            rows_list   = _safe_loc(account_df, item, item_name, [])
            rows_bucket = _safe_loc_row(amount_bucket_df, item)
            amount      = _safe_loc_scalar(account_amount_df, item, 'amount', 0)
            result.update(_neighbors_threshold(amount, rows_list, rows_bucket, num_list))
    elif query_id in (1, 2, 5):
        for item in account_list:
            rows_list   = _safe_loc(account_df, item, item_name, [])
            rows_bucket = _safe_loc_row(amount_bucket_df, item)
            result.update(_neighbors_truncate(rows_list, rows_bucket, num_list))
    elif query_id in (3, 11):
        for item in account_list:
            result.update(_safe_loc(account_df, item, item_name, []))
    return list(result)


def _safe_loc(df, key, col, default):
    try:
        row = df.loc[key]
        return row[col] if not isinstance(row, pd.DataFrame) else row[col].tolist()
    except KeyError:
        return default

def _safe_loc_row(df, key):
    try:
        return df.loc[key]
    except KeyError:
        return None

def _safe_loc_scalar(df, key, col, default):
    try:
        return df.loc[key][col]
    except KeyError:
        return default


def _neighbors_threshold(transfer_in_amount, rows_list, rows_bucket, num_list):
    if rows_bucket is None:
        return []
    threshold = transfer_in_amount * THRESH_HOLD
    temp = [r for r in rows_list if r[1] > threshold]
    return _apply_truncation(temp, rows_bucket, num_list)

def _neighbors_truncate(rows_list, rows_bucket, num_list):
    if rows_bucket is None:
        return []
    return _apply_truncation(rows_list, rows_bucket, num_list)

def _apply_truncation(items, bucket_row, num_list):
    total, header = 0, -1
    for col in reversed(num_list):
        total += bucket_row[col]
        if total >= TRUNCATION_LIMIT:
            header = int(col)
            break
    if header == -1:
        return [t[0] for t in items]
    if TIME_TRUNCATE:
        return [t[0] for t in items if t[2] >= header]
    return [t[0] for t in items if t[1] >= header]


def _process_get_neighbors(chunk, account_df, account_amount_df, amount_bucket_df, num_list, query_id):
    col = chunk.columns[1]
    chunk[col] = chunk[col].apply(
        lambda x: _find_neighbors(x, account_df, account_amount_df, amount_bucket_df, num_list, query_id)
    )
    return chunk


def _get_next_neighbor_list(neighbors_df, account_df, account_amount_df, amount_bucket_df, query_id):
    num_list = [] if query_id in (3, 11) else list(amount_bucket_df.columns)
    parallelism = max(1, multiprocessing.cpu_count() // 4)
    chunks = np.array_split(neighbors_df, parallelism)
    with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as ex:
        futures = [ex.submit(_process_get_neighbors, c, account_df, account_amount_df,
                             amount_bucket_df, num_list, query_id) for c in chunks]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    return pd.concat(results).sort_index()


def _process_batch(batch, basic_sum_df, first_col, second_col):
    exploded = batch.explode(second_col)
    merged = exploded.merge(basic_sum_df, left_on=second_col, right_index=True, how='left'
                            ).drop(columns=[second_col])
    return merged.groupby(first_col).sum()


def _get_next_sum_table(neighbors_df, basic_sum_df):
    first_col  = neighbors_df.columns[0]
    second_col = neighbors_df.columns[1]
    batches    = [neighbors_df.iloc[i:i+BATCH_SIZE] for i in range(0, len(neighbors_df), BATCH_SIZE)]
    parallelism = max(1, multiprocessing.cpu_count() // 4)
    with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as ex:
        results = list(ex.map(partial(_process_batch, basic_sum_df=basic_sum_df,
                                      first_col=first_col, second_col=second_col), batches))
    return pd.concat(results).groupby(first_col).sum().astype(int)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# Map each logical task to its generator function.
# Tasks in the same inner list run in the same process batch.
QUERY_TASKS = [
    generate_query6,
    generate_query2,
    generate_query3,
    generate_query4,
    generate_query5_and_12,
    generate_query7_and_9,
    generate_query11,
    generate_query8,
    generate_query10,
    generate_query1,
]


def main():
    multiprocessing.set_start_method('forkserver')
    batch_size = 5
    for i in range(0, len(QUERY_TASKS), batch_size):
        procs = []
        for task in QUERY_TASKS[i:i+batch_size]:
            p = multiprocessing.Process(target=task)
            p.start()
            procs.append((p, task.__name__))
        for p, name in procs:
            p.join()
            print(f"{name} finished")


if __name__ == "__main__":
    main()
