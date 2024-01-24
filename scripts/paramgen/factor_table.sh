#!/bin/bash

rm -rf ../../out/factor_table

python3 generate_account.py &

python3 time_split.py &

python3 split_amount.py &

python3 loan.py &

wait

echo "All factors have been generated."
