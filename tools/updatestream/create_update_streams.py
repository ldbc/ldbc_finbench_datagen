#!/usr/bin/env python3
"""
FILE: create_update_streams.py
DESC: Creates the update streams for LDBC FinBench Transaction using the raw parquet files
"""
import argparse
import glob
from multiprocessing.sharedctypes import Value
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import duckdb
import re
import time
import copy


class UpdateStreamCreator:

    def __init__(self, raw_format, raw_dir, output_dir, start_date, end_date, batch_size_in_days):
        """
        Args:
            - raw_format (str): The format of the raw files (e.g. 'parquet', 'raw')
            - raw_dir  (str): The root input dir (e.g. '/data/out-sf1')
            - output_dir (str): The output directory for the inserts and deletes folder and files
            - start_date (datetime): Start date of the update streams
            - end_date (datetime): End date of the update streams
        """
        self.raw_format = raw_format
        self.raw_dir = raw_dir
        if not Path(f"{self.raw_dir}/raw").exists():
            raise ValueError(f"Provided directory does not contain expected folder. Got: {self.raw_dir}")

        self.output_dir = output_dir
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size_in_days = batch_size_in_days
        self.database_name = 'finbench.stream.duckdb'
        self.input_extension = '**/*.snappy.parquet'
        self.input_path_folder = 'parquet'

        Path(self.database_name).unlink(missing_ok=True)  # Remove original file
        self.cursor = duckdb.connect(database=self.database_name)

        self.load_raw_data()

    def load_raw_data(self):
        # Get folders
        for folder in glob.glob(f"{self.raw_dir}/raw/*"):
            if (os.path.isdir(folder)):
                entity = folder.split('/')[-1]
                if self.raw_format == 'parquet':
                    loader_function = f"read_parquet('{folder}/*.csv')"
                elif self.raw_format == 'csv':
                    loader_function = f"read_csv_auto('{folder}/*.csv', delim='|', header=TRUE)"
                else:
                    raise ValueError(f"Unknown raw format: {args.raw_format}")
                self.cursor.execute(f"CREATE OR REPLACE VIEW {entity} AS SELECT * FROM {loader_function};")
                print(f"VIEW FOR {entity} CREATED")

    def create_snapshot(self):
        print(f"===== Creating snapshot =====")
        Path(f"{self.output_dir}/snapshot").mkdir(parents=True, exist_ok=True)

        start_date_long = self.start_date.timestamp() * 1000
        with open("snapshot.sql", "r") as f:
            queries_file = f.read()
            queries_file = queries_file.replace(':start_date_long', str(int(start_date_long)))
            queries_file = queries_file.replace(':output_dir', self.output_dir)
            queries_file = re.sub(r"\n--.*", "", queries_file)
            queries = queries_file.split(';\n')  # split on semicolon-newline sequences

            for query in queries:
                if not query or query.isspace():
                    continue

                print(query)
                start = time.time()
                self.cursor.execute(query)
                end = time.time()
                duration = end - start
                print(f"-> {duration:.4f} seconds")

    def create_upstream(self):
        print(f"===== Creating update streams =====")
        Path(f"{self.output_dir}/inserts").mkdir(parents=True, exist_ok=True)
        Path(f"{self.output_dir}/deletes").mkdir(parents=True, exist_ok=True)

        start_date_long = self.start_date.timestamp() * 1000

        with open("updateStream.sql", "r") as f:
            queries_file = f.read()
            queries_file = queries_file.replace(':start_date_long', str(int(start_date_long)))
            queries_file = queries_file.replace(':output_dir', self.output_dir)

            # strip comments
            queries_file = re.sub(r"\n--.*", "", queries_file)
            queries = queries_file.split(';\n')  # split on semicolon-newline sequences
            for query in queries:
                if not query or query.isspace():
                    continue

                print(query)
                start = time.time()
                self.cursor.execute(query)
                end = time.time()
                duration = end - start
                print(f"-> {duration:.4f} seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--raw_format',
        help="raw_format: the format of the raw data e.g. 'parquet', 'csv'",
        type=str,
        default="csv",
        required=False
    )
    parser.add_argument(
        '--raw_dir',
        help="raw_dir: directory containing the raw data e.g. 'graphs/parquet/raw/'",
        type=str,
        required=True
    )
    parser.add_argument(
        '--output_dir',
        help="output_dir: folder to output the data",
        type=str,
        required=True
    )
    parser.add_argument(
        '--batch_size_in_days',
        help="batch_size_in_days: The amount of days in a batch",
        type=int,
        default=1,
        required=False
    )
    args = parser.parse_args()

    # Determine date boundaries
    start_date = datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0, tzinfo=ZoneInfo('GMT')).timestamp()
    end_date = datetime(year=2023, month=1, day=1, hour=0, minute=0, second=0, tzinfo=ZoneInfo('GMT'))
    bulk_load_portion = 0.97

    threshold = datetime.fromtimestamp(start_date + ((end_date.timestamp() - start_date) * bulk_load_portion),
                                       tz=ZoneInfo('GMT'))

    if not Path(args.raw_dir).exists():
        raise ValueError(f"raw data dir does not exist. Got: {args.raw_dir}")

    start = time.time()
    USC = UpdateStreamCreator(args.raw_format, args.raw_dir, args.output_dir, threshold, end_date,
                              args.batch_size_in_days)
    USC.create_snapshot()
    # USC.create_upstream()
    end = time.time()
    duration = end - start
    print(f"Total duration: {duration:.4f} seconds")
    print("Update streams created")
