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
import multiprocessing.connection
import os
import random
import sys
from array import array
from ast import literal_eval
from calendar import timegm
from collections import defaultdict
from datetime import date
from glob import glob
import concurrent.futures

import numpy as np
import pandas as pd
import search_params
import time_select

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TABLE_DIR = None
OUT_DIR   = None
random.seed(42)

TRUNCATION_LIMIT = 500
THRESH_HOLD      = 0
THRESH_HOLD_6    = 0
TIME_TRUNCATE    = True
TRUNCATION_ORDER = "TIMESTAMP_DESCENDING" if TIME_TRUNCATE else "AMOUNT_DESCENDING"
BATCH_SIZE       = 5000


MAX_CONCURRENT_TASKS = 1
INNER_WORKERS = 3


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def factor_path(*parts):
    """Path inside TABLE_DIR."""
    return os.path.join(TABLE_DIR or os.environ['PARAMGEN_TABLE_DIR'], *parts)

def output_path(filename):
    """Path inside OUT_DIR."""
    return os.path.join(OUT_DIR or os.environ['PARAMGEN_OUT_DIR'], filename)


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


def _parse_literal_column(col_series):
    return col_series.apply(literal_eval)


def _apply_literal_eval(df, list_col):
    parallelism = INNER_WORKERS
    if parallelism <= 1 or len(df) < _PARALLEL_MIN_ITEMS:
        df[list_col] = df[list_col].apply(literal_eval)
        return df
    step = -(-len(df) // parallelism)
    chunks = [df[list_col].iloc[i:i + step]
              for i in range(0, len(df), step)]
    with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as ex:
        futures = [ex.submit(_parse_literal_column, c) for c in chunks]
        parsed = [f.result() for f in futures]
    df[list_col] = pd.concat(parsed)
    return df


def load_indexed_list_df(file_path):
    df = load_list_df(file_path)
    df.set_index(df.columns[0], inplace=True)
    return df


def load_list_df(file_path):
    df = read_csv(file_path)
    _apply_literal_eval(df, df.columns[1])
    return df


# ---------------------------------------------------------------------------
# Graph traversal helpers
# ---------------------------------------------------------------------------

def neighbor_id(item):
    return int(item[0]) if isinstance(item, (list, tuple)) else int(item)

def neighbor_time(item):
    return int(item[2]) if isinstance(item, (list, tuple)) and len(item) >= 3 else None

def month_start_ms(timestamp_ms):
    days = int(timestamp_ms) // 86_400_000
    z = days + 719_468
    doe = z - (z // 146_097) * 146_097
    yoe = (doe - doe // 1460 + doe // 36_524 - doe // 146_096) // 365
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    return (days - doy + (153 * mp + 2) // 5) * 86_400_000


def _month_start_ms_vec(ts_arr):
    """month_start_ms over an int64 ndarray (identical values; never feed it
    the _TS_NONE sentinel — filter first)."""
    days = ts_arr // 86_400_000
    z = days + 719_468
    doe = z - (z // 146_097) * 146_097
    yoe = (doe - doe // 1460 + doe // 36_524 - doe // 146_096) // 365
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    return (days - doy + (153 * mp + 2) // 5) * 86_400_000

def get_neighbors(indexed_df, key):
    if isinstance(indexed_df, CsrAdjacency):
        return indexed_df.get(key) or []
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


_TS_NONE = np.iinfo(np.int64).min


class CsrAdjacency:
    __slots__ = ('node_ids', 'indptr', 'dst', 'amount', 'ts',
                 'pairs', 'per_node_limit')

    def __init__(self, node_ids, indptr, dst, amount, ts,
                 pairs=False, per_node_limit=None):
        self.node_ids = node_ids
        self.indptr = indptr
        self.dst = dst
        self.amount = amount
        self.ts = ts
        self.pairs = pairs
        self.per_node_limit = per_node_limit

    def get(self, key, default=None):
        node_ids = self.node_ids
        pos = int(np.searchsorted(node_ids, key))
        if pos >= node_ids.shape[0] or node_ids[pos] != key:
            return default
        s = int(self.indptr[pos])
        e = int(self.indptr[pos + 1])
        if s == e:
            return default
        if self.per_node_limit is not None and e - s > self.per_node_limit:
            e = s + self.per_node_limit
        ts = self.ts[s:e]
        if self.pairs:
            return [(int(d), None if t == _TS_NONE else int(t))
                    for d, t in zip(self.dst[s:e], ts)]
        return [[int(d), float(a), None if t == _TS_NONE else int(t)]
                for d, a, t in zip(self.dst[s:e], self.amount[s:e], ts)]


def _pair_adjacency(table, per_node_limit=None):
    """(destination, timestamp) view sharing the graph arrays."""
    base = table.adjacency
    return CsrAdjacency(base.node_ids, base.indptr, base.dst, base.amount,
                        base.ts, pairs=True, per_node_limit=per_node_limit)


# ---------------------------------------------------------------------------
# Graph loading
# ---------------------------------------------------------------------------

_CSR_SHARD_BYTES = 8 << 20


def _parse_graph_part(text):
    row_keys = array('q')
    keys = array('q')
    dst = array('q')
    amt = array('d')
    ts = array('q')
    for line in text.split('\n'):
        if not line or line.endswith('|items'):
            continue
        pipe = line.find('|')
        if pipe < 0:
            raise RuntimeError(f'malformed factor row: {line[:80]!r}')
        body = line[pipe + 1:]
        if not body.startswith('['):
            raise RuntimeError(f'unexpected items cell: {body[:80]!r}')
        key = int(line[:pipe])
        row_keys.append(key)
        toks = body[1:-1].replace('[', '').replace(']', '').split(',')
        if not toks or toks == ['']:
            continue
        if len(toks) % 3:
            raise RuntimeError(f'non-triplet items cell: {line[pipe:pipe + 80]!r}')
        for i in range(0, len(toks), 3):
            keys.append(key)
            dst.append(int(toks[i]))
            amt.append(float(toks[i + 1]))
            t = toks[i + 2]
            ts.append(_TS_NONE if t == 'None' else int(t))
    return row_keys, keys, dst, amt, ts


def _unique_in_order(rows):
    n = rows.shape[0]
    if n == 0:
        return rows
    order = np.argsort(rows, kind='stable')
    sorted_rows = rows[order]
    mask = np.empty(n, dtype=bool)
    mask[0] = True
    np.not_equal(sorted_rows[1:], sorted_rows[:-1], out=mask[1:])
    return rows[np.sort(order[mask])]


class GraphTable:
    __slots__ = ('adjacency', 'nodes')

    def __init__(self, adjacency, nodes):
        self.adjacency = adjacency
        self.nodes = nodes


def _read_shards(paths):
    for path in paths:
        with open(path, 'rb') as f:
            first = True
            buf = b''
            while True:
                chunk = f.read(_CSR_SHARD_BYTES)
                if not chunk:
                    break
                buf += chunk
                nl = buf.rfind(b'\n')
                if nl < 0:
                    continue
                text = buf[:nl].decode('utf-8')
                buf = buf[nl + 1:]
                if first:
                    text = text.partition('\n')[2]
                    first = False
                if text:
                    yield text
            if buf.strip():
                text = buf.decode('utf-8')
                if first:
                    text = text.partition('\n')[2]
                if text:
                    yield text


def _load_parts(file_path, parse_fn):
    if os.path.isfile(file_path):
        paths = [file_path]
    else:
        paths = sorted(glob(os.path.join(file_path, '*.csv')))
        if not paths:
            raise FileNotFoundError(
                f"Factor table path does not exist or has no CSVs: {file_path}. "
                "Please regenerate factor_table outputs before running paramgen."
            )
    parallelism = INNER_WORKERS
    parts = []
    if parallelism <= 1:
        parts = [parse_fn(t) for t in _read_shards(paths)]
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as ex:
            in_flight = []
            for text in _read_shards(paths):
                in_flight.append(ex.submit(parse_fn, text))
                while len(in_flight) >= 4 * parallelism:
                    parts.append(in_flight.pop(0).result())
            while in_flight:
                parts.append(in_flight.pop(0).result())
    return parts


def load_graph(file_path):
    parts = _load_parts(file_path, _parse_graph_part)
    row_keys = array('q')
    keys = array('q')
    dst = array('q')
    amt = array('d')
    ts = array('q')
    for rk, k, d, a, t in parts:
        row_keys.extend(rk)
        keys.extend(k)
        dst.extend(d)
        amt.extend(a)
        ts.extend(t)
    del parts
    src = np.frombuffer(keys, dtype=np.int64)
    n = src.shape[0]
    if n == 0:
        empty = np.empty(0, dtype=np.int64)
        return GraphTable(CsrAdjacency(empty, np.zeros(1, dtype=np.int64),
                                       empty, np.empty(0), empty), empty)
    order = np.argsort(src, kind='stable')
    src_sorted = src[order]
    starts = np.empty(n, dtype=bool)
    starts[0] = True
    np.not_equal(src_sorted[1:], src_sorted[:-1], out=starts[1:])
    node_ids = src_sorted[starts]
    indptr = np.append(np.flatnonzero(starts), n)
    del src_sorted, starts, src, keys
    dst_sorted = np.frombuffer(dst, dtype=np.int64)[order]
    amt_sorted = np.frombuffer(amt, dtype=np.float64)[order]
    ts_sorted = np.frombuffer(ts, dtype=np.int64)[order]
    del order, dst, amt, ts
    adjacency = CsrAdjacency(node_ids, indptr, dst_sorted, amt_sorted, ts_sorted)
    nodes = _unique_in_order(np.frombuffer(row_keys, dtype=np.int64))
    return GraphTable(adjacency, nodes)


# ---------------------------------------------------------------------------
# Loan loading
# ---------------------------------------------------------------------------

class LoanAccountMap:
    __slots__ = ('loan_ids', 'loan_ptr', 'row_month', 'acc_ptr', 'acc_flat',
                 'sorted_ids', 'sorted_pos')

    def __init__(self, loan_ids, loan_ptr, row_month, acc_ptr, acc_flat):
        self.loan_ids = loan_ids
        self.loan_ptr = loan_ptr
        self.row_month = row_month
        self.acc_ptr = acc_ptr
        self.acc_flat = acc_flat
        self.sorted_pos = np.argsort(loan_ids, kind='stable')
        self.sorted_ids = loan_ids[self.sorted_pos]

    def loan_rows(self, loan_id):
        pos = int(np.searchsorted(self.sorted_ids, loan_id))
        if pos >= self.sorted_ids.shape[0] or self.sorted_ids[pos] != loan_id:
            return -1, -1
        row = int(self.sorted_pos[pos])
        return int(self.loan_ptr[row]), int(self.loan_ptr[row + 1])


def _parse_loan_part(text):
    loans = array('q')
    months = array('q')
    counts = array('q')
    accs = array('q')
    for line in text.split('\n'):
        if not line:
            continue
        p1 = line.find('|')
        p2 = line.find('|', p1 + 1) if p1 >= 0 else -1
        if p2 < 0:
            raise RuntimeError(f'malformed loan row: {line[:80]!r}')
        body = line[p2 + 1:]
        if not body.startswith('['):
            raise RuntimeError(f'unexpected account cell: {body[:80]!r}')
        flat = body[1:-1].replace('[', '').replace(']', '')
        toks = flat.split(',') if flat else []
        loans.append(int(line[:p1]))
        months.append(int(line[p1 + 1:p2]))
        counts.append(len(toks))
        for t in toks:
            accs.append(int(t))
    return loans, months, counts, accs


def load_loan_accounts(file_path):
    """LoanAccountMap parsed directly from the 3-column pipe CSVs."""
    parts = _load_parts(file_path, _parse_loan_part)
    loans_a = array('q')
    months_a = array('q')
    counts_a = array('q')
    accs_a = array('q')
    for lo, mo, c, ac in parts:
        loans_a.extend(lo)
        months_a.extend(mo)
        counts_a.extend(c)
        accs_a.extend(ac)
    del parts
    L = np.frombuffer(loans_a, dtype=np.int64)
    M = np.frombuffer(months_a, dtype=np.int64)
    C = np.frombuffer(counts_a, dtype=np.int64)
    A = np.frombuffer(accs_a, dtype=np.int64)
    row_ptr = np.append(0, np.cumsum(C))
    keep = C > 0
    row_lo = row_ptr[:-1][keep]
    row_hi = row_ptr[1:][keep]
    L, M = L[keep], M[keep]
    n = L.shape[0]
    empty = np.empty(0, dtype=np.int64)
    if n == 0:
        return LoanAccountMap(empty, np.zeros(1, dtype=np.int64),
                              empty, np.zeros(1, dtype=np.int64), empty)
    idx = np.arange(n)
    order = np.lexsort((idx, M, L))
    gL, gM = L[order], M[order]
    gstart = np.empty(n, dtype=bool)
    gstart[0] = True
    np.logical_or(gL[1:] != gL[:-1], gM[1:] != gM[:-1], out=gstart[1:])
    grp_first = order[gstart]
    grp_last = order[np.append(np.flatnonzero(gstart)[1:], n) - 1]
    grp_loan, grp_month = gL[gstart], gM[gstart]
    appear = _unique_in_order(L)
    ord2 = np.argsort(appear, kind='stable')
    grp_rank = ord2[np.searchsorted(appear[ord2], grp_loan)]
    final = np.lexsort((grp_first, grp_rank))
    fl, fm = grp_loan[final], grp_month[final]
    lstart = np.empty(n, dtype=bool)
    lstart[0] = True
    lstart[1:] = fl[1:] != fl[:-1]
    loan_ids = fl[lstart]
    loan_ptr = np.append(np.flatnonzero(lstart), n)
    last_lo = row_lo[grp_last[final]]
    last_hi = row_hi[grp_last[final]]
    lens = last_hi - last_lo
    acc_ptr = np.append(0, np.cumsum(lens))
    total = int(lens.sum())
    if total:
        grp_idx = np.repeat(np.arange(n), lens)
        within = np.arange(total) - np.repeat(acc_ptr[:-1], lens)
        acc_flat = A[last_lo[grp_idx] + within]
    else:
        acc_flat = empty
    return LoanAccountMap(loan_ids, loan_ptr, fm, acc_ptr, acc_flat)


def to_month_counts_map(month_df):
    col_months = {}
    for c in month_df.columns:
        try:
            col_months[c] = int(str(c))
        except (TypeError, ValueError):
            continue
    if not col_months:
        return {}
    if not month_df.index.is_unique:
        month_df = month_df[~month_df.index.duplicated(keep='first')]
    cols = [c for c in month_df.columns if c in col_months]
    months = [col_months[c] for c in cols]
    out = {}
    for idx, row in zip(month_df.index, month_df[cols].to_numpy()):
        out[int(idx)] = dict(zip(months, row))
    return out


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
                       months_out=None):
    reachable, count = set(), 0
    if depth >= max_depth:
        return reachable, count
    neighbors = truncate_neighbors(get_neighbors(indexed_df, current_id),
                                   truncation_limit)
    for item in neighbors:
        if item.__class__ is list and len(item) >= 3:
            dst = int(item[0])
            ts = item[2]
            if ts is not None:
                ts = int(ts)
        else:
            dst = neighbor_id(item)
            ts = neighbor_time(item)
        if ts is None or dst in visited or ts <= prev_ts:
            continue
        if months_out is not None:
            months_out.add(month_start_ms(ts))
        reachable.add(dst)
        count += 1
        sub_r, sub_c = collect_path_stats(
            indexed_df, dst, ts, visited | {dst}, depth + 1,
            max_depth=max_depth, truncation_limit=truncation_limit,
            months_out=months_out,
                                 )
        reachable.update(sub_r)
        count += sub_c
    return reachable, count


def collect_month_matches_decreasing(indexed_df, current_id, prev_ts, visited, depth,
                                     qualifying_df, match_ids, hit_counts,
                                     max_depth=3, truncation_limit=TRUNCATION_LIMIT):
    traversed = 0
    if depth >= max_depth:
        return traversed
    neighbors = truncate_neighbors(get_neighbors(indexed_df, current_id),
                                   truncation_limit)
    for item in neighbors:
        if item.__class__ is list and len(item) >= 3:
            src = int(item[0])
            ts = item[2]
            if ts is not None:
                ts = int(ts)
        else:
            src = neighbor_id(item)
            ts = neighbor_time(item)
        if ts is None or src in visited or ts >= prev_ts:
            continue
        traversed += 1
        month = month_start_ms(ts)
        if _has_month_activity(qualifying_df, src, month):
            match_ids[month].add(src)
            hit_counts[month] += 1
        sub = collect_month_matches_decreasing(
            indexed_df, src, ts, visited | {src}, depth + 1,
            qualifying_df, match_ids, hit_counts,
            max_depth=max_depth, truncation_limit=truncation_limit,
                                 )
        traversed += sub
    return traversed


_EMPTY_MONTH_COUNTS = {}


def _has_month_activity(month_counts, item_id, month_start):
    if isinstance(month_counts, dict):
        return month_counts.get(item_id, _EMPTY_MONTH_COUNTS).get(month_start, 0) > 0
    try:
        row = month_counts.loc[item_id]
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
# Parallel scan
# ---------------------------------------------------------------------------

_PARALLEL_MIN_ITEMS = 256


def _parallel_scan(scan_fn, items, *shared):
    items = list(items)
    if not items:
        return []
    parallelism = INNER_WORKERS
    if parallelism <= 1 or len(items) < _PARALLEL_MIN_ITEMS:
        return scan_fn(items, *shared)
    step = -(-len(items) // parallelism)
    chunks = [items[i:i + step] for i in range(0, len(items), step)]
    return [row for rows in _fork_map(scan_fn, chunks, shared) for row in rows]


_FORK_JOB = None


def _fork_send(rank, conn):
    fn, chunks, shared = _FORK_JOB
    try:
        conn.send(('OK', fn(chunks[rank], *shared)))
    except BaseException as e:
        try:
            conn.send(('ERR', e))
        except BaseException:
            pass
    finally:
        conn.close()


def _fork_map(fn, chunks, shared):
    global _FORK_JOB
    if len(chunks) == 1:
        return [fn(chunks[0], *shared)]
    _FORK_JOB = (fn, chunks, shared)
    ctx = multiprocessing.get_context('fork')
    procs, conns = [], []
    results, err = [], None
    try:
        for rank in range(len(chunks)):
            recv, send = ctx.Pipe(duplex=False)
            p = ctx.Process(target=_fork_send, args=(rank, send))
            p.start()
            send.close()  # children hold their own end; the parent only reads
            procs.append(p)
            conns.append(recv)
        for conn in conns:
            try:
                tag, val = conn.recv()
            except EOFError:
                err = err or RuntimeError('fork scan worker died')
                break
            if tag == 'ERR':
                err = val
                break
            results.append(val)
    finally:
        for conn in conns:
            conn.close()
        for p in procs:
            p.join()
        _FORK_JOB = None
    if err is not None:
        raise err
    return results


# ---------------------------------------------------------------------------
# Per-query candidate builders  (pure logic, no I/O)
# ---------------------------------------------------------------------------

def _scan_query1(src_ids, transfer_out_df, blocked_signin_month_df):
    rows = []
    for src_id in src_ids:
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
            rows.append([(src_id, int(month), int(mn), int(mx)),
                         traversed_counts[month], hit_counts[month], len(ids)])
    return rows


def _candidates_query1(transfer_out_df, blocked_signin_month_df):
    candidate_rows = _parallel_scan(
        _scan_query1, list(transfer_out_df.nodes),
        transfer_out_df.adjacency, to_month_counts_map(blocked_signin_month_df))
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
        if item.__class__ is list and len(item) >= 3:
            dst = int(item[0])
            ts = item[2]
            if ts is not None:
                ts = int(ts)
        else:
            dst = neighbor_id(item)
            ts = neighbor_time(item)
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


def _scan_query2(person_entries, transfer_in_df, loan_deposit_month_df):
    rows = []
    for person_id, account_ids in person_entries:
        person_id = int(person_id)
        stats = {}
        for account_id in account_ids:
            account_id = int(account_id)
            match_ids, hit_counts = defaultdict(set), defaultdict(int)
            traversed = collect_month_matches_decreasing(
                transfer_in_df, account_id, sys.maxsize, {account_id}, 0,
                loan_deposit_month_df, match_ids, hit_counts,
            )
            for month, ids in match_ids.items():
                key = (person_id, int(month))
                if key not in stats:
                    stats[key] = dict(other_ids=set(), hit_count=0,
                                      account_hits=set(), traversed=0)
                stats[key]['other_ids'].update(int(i) for i in ids)
                stats[key]['hit_count']    += hit_counts[month]
                stats[key]['account_hits'].add(account_id)
                stats[key]['traversed']    += traversed
        for (pid, month), s in stats.items():
            rows.append([(pid, month, month),
                         s['traversed'], len(s['other_ids']),
                         s['hit_count'], len(s['account_hits'])])
    return rows


def _candidates_query2(person_account_df, transfer_in_df, loan_deposit_month_df):
    person_col, account_col = person_account_df.columns[0], person_account_df.columns[1]
    person_entries = [(row[person_col], row[account_col])
                      for _, row in person_account_df.iterrows()]
    candidate_rows = _parallel_scan(
        _scan_query2, person_entries, transfer_in_df.adjacency,
        loan_deposit_month_df)
    if not candidate_rows:
        return [], []
    first_array = np.array(candidate_rows, dtype=object)
    selected = select_candidates(first_array, 0.01)
    ids = [int(s[0]) for s in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(s[1]), int(s[2])) for s in selected])
    return ids, time_list


def _scan_query3(src_ids, adjacency, time_by_id, friendly_accounts, scan_cap):
    candidate_rows = []

    for src_id in src_ids:
        src_id = int(src_id)
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

    return candidate_rows


def _candidates_query3(transfer_out_df, pool_ids, pool_times,
                       friendly_accounts, scan_cap=10_000):
    adjacency = _pair_adjacency(transfer_out_df)
    time_by_id = {int(account_id): tp
                  for account_id, tp in zip(pool_ids, pool_times)}
    candidate_rows = _parallel_scan(
        _scan_query3, list(pool_ids), adjacency, time_by_id,
        friendly_accounts, scan_cap)

    if len(candidate_rows) < 4:
        selected = [row[0] for row in candidate_rows]
    else:
        selected = search_params.generate(
            np.array(candidate_rows, dtype=object), 0.10)
    ids = [int(src_id) for src_id, _ in selected]
    id2_list = [int(dst_id) for _, dst_id in selected]
    time_list = [time_by_id[src_id] for src_id in ids]
    return ids, id2_list, time_list


def _q4_pair_index(usrc, udst, src, dst):
    """Position of (src, dst) in the lexsorted pair arrays, -1 if absent."""
    lo = int(np.searchsorted(usrc, src, 'left'))
    hi = int(np.searchsorted(usrc, src, 'right'))
    if lo == hi:
        return -1
    p = lo + int(np.searchsorted(udst[lo:hi], dst))
    return p if p < hi and udst[p] == dst else -1


def _scan_query4(src_dst_items, payload):
    (F_node, F_ptr, F_dst, IN_node, IN_ptr, IN_src,
     usrc, udst, pptr, mts) = payload
    rows = []
    for src_id, direct_dsts in src_dst_items:
        ipos = int(np.searchsorted(IN_node, src_id))
        if ipos >= IN_node.shape[0] or IN_node[ipos] != src_id:
            continue
        s = int(IN_ptr[ipos]); e = int(IN_ptr[ipos + 1])
        incoming = set(IN_src[s:e].tolist())
        if not incoming: continue
        for dst_id in direct_dsts:
            opos = int(np.searchsorted(F_node, dst_id))
            if opos >= F_node.shape[0] or F_node[opos] != dst_id:
                continue
            s = int(F_ptr[opos]); e = int(F_ptr[opos + 1])
            outgoing = set(F_dst[s:e].tolist())
            cycles = outgoing & incoming - {src_id, dst_id}
            if not cycles: continue
            e1 = e2 = e3 = 0
            all_months = []
            pi = _q4_pair_index(usrc, udst, src_id, dst_id)
            if pi >= 0:
                m0, m1 = int(pptr[pi]), int(pptr[pi + 1])
                e1 = m1 - m0
                all_months = mts[m0:m1].tolist()
            for o in cycles:
                pi = _q4_pair_index(usrc, udst, o, src_id)
                if pi >= 0:
                    m0, m1 = int(pptr[pi]), int(pptr[pi + 1])
                    e2 += m1 - m0
                    all_months.extend(mts[m0:m1].tolist())
                pi = _q4_pair_index(usrc, udst, dst_id, o)
                if pi >= 0:
                    m0, m1 = int(pptr[pi]), int(pptr[pi + 1])
                    e3 += m1 - m0
                    all_months.extend(mts[m0:m1].tolist())
            if not all_months:
                continue
            rows.append([(src_id, dst_id, min(all_months), max(all_months)),
                         len(cycles), e1 + e2 + e3, e1])
    return rows


def _q4_filtered_csr(adj):
    src = np.repeat(adj.node_ids, np.diff(adj.indptr))
    keep = (adj.ts != _TS_NONE) & (adj.dst != src)
    fsrc, fdst, fts = src[keep], adj.dst[keep], adj.ts[keep]
    del src, keep
    n = fsrc.shape[0]
    empty = np.empty(0, dtype=np.int64)
    if n == 0:
        return empty, np.zeros(1, dtype=np.int64), empty, empty, empty, empty
    order = np.argsort(fsrc, kind='stable')
    ssorted = fsrc[order]
    starts = np.empty(n, dtype=bool)
    starts[0] = True
    np.not_equal(ssorted[1:], ssorted[:-1], out=starts[1:])
    node = ssorted[starts]
    ptr = np.append(np.flatnonzero(starts), n)
    return node, ptr, fdst[order], fsrc, fdst, fts


def _candidates_query4(transfer_out_df, transfer_in_df):
    out_adj = transfer_out_df.adjacency
    in_adj  = transfer_in_df.adjacency

    F_node, F_ptr, F_dst, fsrc, fdst, fts = _q4_filtered_csr(out_adj)
    IN_node, IN_ptr, IN_src, _, _, _ = _q4_filtered_csr(in_adj)

    n = fsrc.shape[0]
    empty = np.empty(0, dtype=np.int64)
    if n == 0:
        usrc = udst = mts = empty
        pptr = np.zeros(1, dtype=np.int64)
    else:
        lex = np.lexsort((fdst, fsrc))
        lsrc, ldst = fsrc[lex], fdst[lex]
        pstarts = np.empty(n, dtype=bool)
        pstarts[0] = True
        np.logical_or(lsrc[1:] != lsrc[:-1], ldst[1:] != ldst[:-1],
                      out=pstarts[1:])
        usrc, udst = lsrc[pstarts], ldst[pstarts]
        pptr = np.append(np.flatnonzero(pstarts), n)
        mts = _month_start_ms_vec(fts[lex])
        del lex, lsrc, ldst, pstarts
    nodes = np.asarray(transfer_out_df.nodes)
    if F_node.shape[0]:
        vpos = np.searchsorted(F_node, nodes)
        vhit = vpos < F_node.shape[0]
        src_first = nodes[vhit & (F_node[np.minimum(vpos, F_node.shape[0] - 1)] == nodes)]
    else:
        src_first = nodes[:0]
    pos = np.searchsorted(F_node, src_first)
    src_dst_items = []
    for i, src in enumerate(src_first):
        s = int(pos[i])
        lo, hi = int(F_ptr[s]), int(F_ptr[s + 1])
        src_dst_items.append((int(src), list(set(F_dst[lo:hi].tolist()))))
    payload = (F_node, F_ptr, F_dst, IN_node, IN_ptr, IN_src,
               usrc, udst, pptr, mts)
    candidate_rows = _parallel_scan(_scan_query4, src_dst_items, payload)

    if not candidate_rows:
        return [], [], []

    first_array = np.array(candidate_rows, dtype=object)
    selected = select_candidates(first_array, 0.30)
    src_ids   = [int(s) for s, _, _, _ in selected]
    dst_ids   = [int(d) for _, d, _, _ in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(mn), int(mx)) for _, _, mn, mx in selected])
    return src_ids, dst_ids, time_list


def _scan_query5(person_entries, transfer_out_df):
    rows = []
    for person_id, account_ids in person_entries:
        person_id = int(person_id)
        stats = {}
        for account_id in account_ids:
            account_id = int(account_id)
            for item in get_neighbors(transfer_out_df, account_id):
                if item.__class__ is list and len(item) >= 3:
                    # fast path: parsed factor rows are [dst, amount, ts] lists
                    dst = int(item[0])
                    ts = item[2]
                    if ts is not None:
                        ts = int(ts)
                else:
                    dst = neighbor_id(item)
                    ts = neighbor_time(item)
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
        for (pid, month), s in stats.items():
            months = s['months'] or {month}
            rows.append([(pid, min(months), max(months)),
                         s['path_count'], len(s['dst_ids']),
                         len(s['account_hits'])])
    return rows


def _candidates_query5(person_account_df, transfer_out_df):
    person_col, account_col = person_account_df.columns[0], person_account_df.columns[1]
    person_entries = [(row[person_col], row[account_col])
                      for _, row in person_account_df.iterrows()]
    candidate_rows = _parallel_scan(
        _scan_query5, person_entries, transfer_out_df.adjacency)
    if not candidate_rows:
        return [], []
    first_array = np.array(candidate_rows, dtype=object)
    selected = select_candidates(first_array, 0.01)
    ids = [int(s[0]) for s in selected]
    time_list = time_select.findTimeParamsForMonthRanges(
        [(int(s[1]), int(s[2])) for s in selected])
    return ids, time_list


def _scan_query6(card_ids, withdraw_in_df, mid_total_transfers):
    rows = []
    for card_id in card_ids:
        card_id = int(card_id)
        month_stats = defaultdict(lambda: dict(mid_ids=set(), transfer_count=0,
                                               withdraw_count=0, withdraw_amount=0.0))
        for item in get_neighbors(withdraw_in_df, card_id):
            mid = neighbor_id(item); ts = neighbor_time(item)
            if ts is None: continue
            # mid_total_transfers covers every transfer_in_month index row;
            # a missing mid scores 0 and is filtered by the threshold below
            if mid_total_transfers.get(mid, 0) <= 3:
                continue
            month = month_start_ms(ts)
            s = month_stats[month]
            s['mid_ids'].add(mid)
            s['transfer_count'] += int(mid_total_transfers.get(mid, 0))
            s['withdraw_count'] += 1
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                s['withdraw_amount'] += float(item[1])
        for month, s in month_stats.items():
            if s['mid_ids']:
                rows.append([(card_id, month, month),
                             len(s['mid_ids']), s['transfer_count'],
                             s['withdraw_count'], s['withdraw_amount']])
    return rows


def _candidates_query6(withdraw_in_df, transfer_in_month_df):
    card_account_ids = _load_card_account_ids()
    if not card_account_ids:
        return [], []

    mid_total_transfers = {int(mid_id): total
                           for mid_id, total in transfer_in_month_df.sum(axis=1).items()}

    candidate_rows = _parallel_scan(
        _scan_query6, list(card_account_ids), withdraw_in_df.adjacency,
        mid_total_transfers)
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


def _q8_account_months(account_ids, trans_withdraw_map):
    results = []
    for account_id in account_ids:
        account_id = int(account_id)
        months = set()
        for item in get_neighbors(trans_withdraw_map, account_id):
            if item.__class__ is list and len(item) >= 3:
                # fast path: parsed factor rows are [dst, amount, ts] lists
                dst = int(item[0])
                ts = item[2]
                if ts is not None:
                    ts = int(ts)
            else:
                dst = neighbor_id(item)
                ts = neighbor_time(item)
            if ts is None or dst == account_id: continue
            months.add(month_start_ms(ts))
            collect_path_stats(
                trans_withdraw_map, dst, ts, {account_id, dst}, 1,
                months_out=months,
            )
        results.append((account_id, months))
    return results


def _q8_window_keys(lmap, trans_withdraw_map):
    seed_accounts = sorted(set(lmap.acc_flat.tolist()))
    account_months = dict(_parallel_scan(
        _q8_account_months, seed_accounts, trans_withdraw_map))

    keys = []
    loan_ids, loan_ptr = lmap.loan_ids, lmap.loan_ptr
    row_month, acc_ptr, acc_flat = lmap.row_month, lmap.acc_ptr, lmap.acc_flat
    for li in range(loan_ids.shape[0]):
        loan_id = int(loan_ids[li])
        for ri in range(int(loan_ptr[li]), int(loan_ptr[li + 1])):
            month_start = int(row_month[ri])
            months = set()
            for a in acc_flat[int(acc_ptr[ri]):int(acc_ptr[ri + 1])]:
                acc_months = account_months.get(int(a))
                if acc_months:
                    months |= acc_months
            win_max = max(max(months), month_start) if months else month_start
            keys.append((loan_id, month_start, win_max))
    return keys


def _scan_query8(key_pairs, adj, lmap):
    rows = []
    for (loan_id, win_min, win_max), tp in key_pairs:
        lo, hi = lmap.loan_rows(loan_id)
        seeds = set()
        if lo >= 0:
            row_month, acc_ptr, acc_flat = lmap.row_month, lmap.acc_ptr, lmap.acc_flat
            for ri in range(lo, hi):
                m = int(row_month[ri])
                if tp.start_ms <= m < tp.end_ms:
                    seeds.update(acc_flat[int(acc_ptr[ri]):int(acc_ptr[ri + 1])].tolist())
        cost = _bfs_window_cost(adj, seeds, tp.start_ms, tp.end_ms)
        rows.append([(loan_id, win_min, win_max), cost])
    return rows


def _candidates_query8(loan_month_account_map, trans_withdraw_df):
    adj = _pair_adjacency(trans_withdraw_df,
                          per_node_limit=2 * TRUNCATION_LIMIT)

    keys = _q8_window_keys(loan_month_account_map, trans_withdraw_df.adjacency)
    if not keys:
        return [], []

    time_params = time_select.findTimeParamsForMonthRanges(
        [(win_min, win_max) for _, win_min, win_max in keys])
    candidate_rows = _parallel_scan(
        _scan_query8, list(zip(keys, time_params)), adj,
        loan_month_account_map)

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


def _scan_query12(person_entries, transfer_out_df, company_ids):
    results = []
    for person_id, account_ids in person_entries:
        company_hits, edge_count = set(), 0
        person_time_counts = defaultdict(int)
        for account_id in account_ids:
            for item in get_neighbors(transfer_out_df, account_id):
                dst = neighbor_id(item)
                if dst not in company_ids: continue
                edge_count += 1
                company_hits.add(dst)
                ts = neighbor_time(item)
                if ts is not None:
                    person_time_counts[month_start_ms(ts)] += 1
        if edge_count > 0:
            results.append(([person_id, edge_count, len(company_hits)],
                            dict(person_time_counts)))
    return results


def _candidates_query12(person_account_df, transfer_out_df):
    company_ids = _load_company_account_ids()
    if not company_ids:
        return [], []
    person_col, account_col = person_account_df.columns[0], person_account_df.columns[1]
    person_entries = [(row[person_col], row[account_col])
                      for _, row in person_account_df.iterrows()]
    scan_results = _parallel_scan(
        _scan_query12, person_entries, transfer_out_df.adjacency,
        company_ids)

    factor_rows = [factor_row for factor_row, _ in scan_results]
    time_counts = defaultdict(lambda: defaultdict(int))
    for factor_row, person_time_counts in scan_results:
        for month, count in person_time_counts.items():
            time_counts[factor_row[0]][month] += count

    if not factor_rows or not time_counts:
        return [], []

    first_array = np.array(factor_rows, dtype=object)
    ids = select_candidates(first_array, 0.01)
    time_bucket_df = _build_time_bucket_df(time_counts)
    time_list = time_select.findTimeParams(ids, time_bucket_df)
    return ids, time_list


# ---------------------------------------------------------------------------
# Helpers for iter-based queries (3/11)
# ---------------------------------------------------------------------------

def _iter_query_setup(query_id):
    if query_id == 3:
        return (
            factor_path('account_in_out_list'),
            factor_path('account_in_out_count'),
            factor_path('account_in_out_month'),
            1,
        )
    if query_id == 11:
        return (
            factor_path('person_guarantee_list'),
            factor_path('person_guarantee_count'),
            factor_path('person_guarantee_month'),
            3,
        )
    raise ValueError(f"No iter setup for query_id={query_id}")


def _run_iter_pipeline(query_id, portion=0.01):
    first_path, amount_path, time_path, steps = _iter_query_setup(query_id)

    first_df   = load_list_df(first_path)
    amount_df  = read_csv(amount_path)
    time_df    = read_csv(time_path)

    first_col = first_df.columns[0]
    if steps > 1:
        account_df = first_df.set_index(first_col)
    amt_col  = amount_df.columns[0]
    time_col = time_df.columns[0]

    amount_df.set_index(amt_col, inplace=True)
    time_df.set_index(time_col, inplace=True)

    neighbors_df = first_df.sort_values(by=first_col)
    first_array  = neighbors_df[first_col].to_numpy()
    next_time_bucket = None

    for step in range(steps):
        next_amount = _get_next_sum_table(neighbors_df, amount_df)
        first_array = np.column_stack((first_array, next_amount.to_numpy()))
        if step == steps - 1:
            next_time_bucket = _get_next_sum_table(neighbors_df, time_df)
        else:
            neighbors_df = _get_next_neighbor_list(neighbors_df, account_df)

    if query_id == 3:
        first_array = _filter_first_array_for_sr6(first_array, lambda r: r[0])
        next_time_bucket = _mask_time_bucket_to_sr6_months(next_time_bucket)
    ids = select_candidates(first_array, portion)
    time_list = time_select.findTimeParams(ids, next_time_bucket)
    return ids, time_list


# ---------------------------------------------------------------------------
# Per-query generate functions  (each one: load → build candidates → write)
# ---------------------------------------------------------------------------

def generate_query1():
    try:
        transfer_out_df       = load_graph(factor_path('account_transfer_out_items'))
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
        person_account_df  = load_list_df(factor_path('person_account_list'))
        transfer_in_df     = load_graph(factor_path('account_transfer_in_items'))
        loan_deposit_df    = read_csv(factor_path('account_loan_deposit_month'))
        loan_deposit_df.set_index(loan_deposit_df.columns[0], inplace=True)
        ids, time_list = _candidates_query2(person_account_df, transfer_in_df, loan_deposit_df)
        if ids:
            write_params(output_path('complex_2_param.csv'), ids, time_list,
                         threshold=False)
    except (FileNotFoundError, ValueError, KeyError, IndexError):
        pass


def generate_query3():
    ids, times = _run_iter_pipeline(3, portion=0.20)
    transfer_out_df = load_graph(factor_path('account_transfer_out_items'))
    friendly_accounts = set(_get_sr6_friendly_months())
    ids, id2_list, time_list = _candidates_query3(
        transfer_out_df, ids, times, friendly_accounts)
    if ids:
        write_params(output_path('complex_3_param.csv'), ids, time_list,
                     threshold=False, id2_list=id2_list, id_col="id1", id2_col="id2",
                     truncate_limit=False, truncate_order=False)


def generate_query4():
    try:
        transfer_out_df  = load_graph(factor_path('account_transfer_out_items'))
        transfer_in_df   = load_graph(factor_path('account_transfer_in_items'))
        ids, dst_ids, time_list = _candidates_query4(transfer_out_df, transfer_in_df)
    except (FileNotFoundError, ValueError, KeyError, IndexError):
        ids, time_list = _run_iter_pipeline(3)
        dst_ids = _build_query4_pairs(ids)

    write_params(output_path('complex_4_param.csv'), ids, time_list,
                 threshold=False, id2_list=dst_ids, id_col="id1", id2_col="id2")


def generate_query5_and_12():
    person_account_df = load_list_df(factor_path('person_account_list'))
    transfer_out_df   = load_graph(factor_path('account_transfer_out_items'))

    ids5, time_list5 = _candidates_query5(person_account_df, transfer_out_df)
    write_params(output_path('complex_5_param.csv'), ids5, time_list5,
                 threshold=False)

    ids12, time_list12 = _candidates_query12(person_account_df, transfer_out_df)
    write_params(output_path('complex_12_param.csv'), ids12, time_list12,
                 threshold=False)


def generate_query6():
    withdraw_in_df       = load_graph(factor_path('account_withdraw_in_items'))
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
    loan_map = load_loan_accounts(factor_path('loan_deposit_account_month_list'))
    trans_withdraw  = load_graph(factor_path('trans_withdraw_items'))
    ids, time_list  = _candidates_query8(loan_map, trans_withdraw)
    write_params(output_path('complex_8_param.csv'), ids, time_list)


def generate_query10():
    ids, time_list = _run_1hop_pipeline('person_invest_company', 'invest_month')
    rng = random.Random(42)
    id2_list = [_random_pair(ids, i, rng) for i in range(len(ids))]
    write_params(output_path('complex_10_param.csv'), ids, time_list,
                 threshold=False, truncate_limit=False, truncate_order=False,
                 id2_list=id2_list, id_col="pid1", id2_col="pid2")


def generate_query11():
    ids, time_list = _run_iter_pipeline(11)
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


def _random_pair(id_list, i, rng):
    while True:
        j = rng.randint(0, len(id_list) - 1)
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


def _sr6_scan_blocked_mids(mid_ids, transfer_out_df, blocked_ids):
    pairs = []
    for mid in mid_ids:
        mid_int = int(mid)
        months = set()
        for item in get_neighbors(transfer_out_df, mid_int):
            dst = neighbor_id(item)
            ts  = neighbor_time(item)
            if ts is None or dst == mid_int or dst not in blocked_ids:
                continue
            months.add(month_start_ms(ts))
        if months:
            pairs.append((mid_int, months))
    return pairs


def _sr6_scan_friendly(src_ids, transfer_in_df, mid_blocked_months):
    pairs = []
    for src in src_ids:
        src_int = int(src)
        months = set()
        for item in get_neighbors(transfer_in_df, src):
            mid = neighbor_id(item)
            ts  = neighbor_time(item)
            if ts is None or mid == src_int:
                continue
            mid_months = mid_blocked_months.get(mid)
            if not mid_months:
                continue
            m = month_start_ms(ts)
            if m in mid_months:
                months.add(m)
        if months:
            pairs.append((src_int, months))
    return pairs


def _load_sr6_friendly_account_months():
    try:
        transfer_in_df  = load_graph(factor_path('account_transfer_in_items'))
        transfer_out_df = load_graph(factor_path('account_transfer_out_items'))
        blocked_df      = read_csv(factor_path('blocked_signin_month'))
    except (FileNotFoundError, ValueError, KeyError):
        return {}

    blocked_ids = set(int(x) for x in blocked_df[blocked_df.columns[0]].dropna().tolist())
    if not blocked_ids:
        return {}

    mid_blocked_months = dict(_parallel_scan(
        _sr6_scan_blocked_mids, list(transfer_out_df.nodes),
        transfer_out_df.adjacency, blocked_ids))
    if not mid_blocked_months:
        return {}

    friendly = dict(_parallel_scan(
        _sr6_scan_friendly, list(transfer_in_df.nodes),
        transfer_in_df.adjacency, mid_blocked_months))
    return friendly


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
    cols = [c for c in df.columns if c in month_cols]
    arr = df[cols].to_numpy(copy=True)

    rows_by_months = defaultdict(list)
    for row_i, idx in enumerate(df.index):
        try:
            acc_int = int(idx)
        except (TypeError, ValueError):
            continue
        acc_friendly = friendly.get(acc_int)
        if acc_friendly:
            rows_by_months[frozenset(acc_friendly)].append(row_i)
    for acc_months, rows in rows_by_months.items():
        zero = np.array([month_cols[c] not in acc_months for c in cols])
        arr[np.ix_(rows, zero)] = 0
    df[cols] = arr
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

def _find_neighbors(account_list, account_df):
    result = set()
    item_name = account_df.columns[0]
    for item in account_list:
        result.update(_safe_loc(account_df, item, item_name, []))
    return list(result)


def _safe_loc(df, key, col, default):
    try:
        row = df.loc[key]
        return row[col] if not isinstance(row, pd.DataFrame) else row[col].tolist()
    except KeyError:
        return default


def _process_get_neighbors(chunk, account_df):
    if not isinstance(chunk, pd.DataFrame):
        chunk = pd.DataFrame(chunk)
    if chunk.empty:
        return chunk
    col = chunk.columns[1]
    chunk[col] = chunk[col].apply(
        lambda x: _find_neighbors(x, account_df)
    )
    return chunk


def _get_next_neighbor_list(neighbors_df, account_df):
    parallelism = INNER_WORKERS
    step = -(-len(neighbors_df) // parallelism)
    chunks = [neighbors_df.iloc[i:i + step]
              for i in range(0, len(neighbors_df), step)]
    results = _fork_map(_process_get_neighbors, chunks,
                        (account_df,))
    return pd.concat(results).sort_index()


def _process_batch(batch, basic_sum_df, first_col, second_col):
    exploded = batch.explode(second_col)
    merged = exploded.merge(basic_sum_df, left_on=second_col, right_index=True, how='left'
                            ).drop(columns=[second_col])
    return merged.groupby(first_col).sum()


def _process_sum_groups(batch_group, basic_sum_df, first_col, second_col):
    return pd.concat([_process_batch(b, basic_sum_df, first_col, second_col)
                      for b in batch_group])


def _get_next_sum_table(neighbors_df, basic_sum_df):
    first_col  = neighbors_df.columns[0]
    second_col = neighbors_df.columns[1]
    batches    = [neighbors_df.iloc[i:i+BATCH_SIZE] for i in range(0, len(neighbors_df), BATCH_SIZE)]
    parallelism = INNER_WORKERS
    group = -(-len(batches) // parallelism)
    results = _fork_map(
        _process_sum_groups,
        [batches[i:i + group] for i in range(0, len(batches), group)],
        (basic_sum_df, first_col, second_col))
    return pd.concat(results).groupby(first_col).sum().astype(int)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

TASK_ORDER = [
    generate_query8,
    generate_query10,
    generate_query11,
    generate_query5_and_12,
    generate_query2,
    generate_query4,
    generate_query6,
    generate_query3,
    generate_query1,
    generate_query7_and_9,
]


def main():
    global TABLE_DIR, OUT_DIR
    TABLE_DIR = sys.argv[1]
    OUT_DIR   = sys.argv[2]
    os.environ['PARAMGEN_TABLE_DIR'] = TABLE_DIR
    os.environ['PARAMGEN_OUT_DIR']   = OUT_DIR

    multiprocessing.set_start_method('forkserver')
    max_tasks = MAX_CONCURRENT_TASKS
    pending = list(TASK_ORDER)
    running = []
    while pending or running:
        while pending and len(running) < max_tasks:
            task = pending.pop(0)
            p = multiprocessing.Process(target=task)
            p.start()
            running.append((p, task.__name__))
        ready = multiprocessing.connection.wait([p.sentinel for p, _ in running])
        still = []
        for p, name in running:
            if p.sentinel in ready:
                p.join()
                print(f"{name} finished")
            else:
                still.append((p, name))
        running = still


if __name__ == "__main__":
    main()
