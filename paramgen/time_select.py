#
# Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import math
from calendar import timegm
from datetime import date, datetime


LAST_MONTHS = 3
START_YEAR = 2020
DAY_MS = 24 * 60 * 60 * 1000


class MonthYearCount:
    def __init__(self, month, year, count):
        self.month = month
        self.year = year
        self.count = count


class TimeParameter:
    def __init__(self, start_ms, end_ms):
        self.start_ms = int(start_ms)
        self.end_ms = int(max(end_ms, start_ms))

        start_dt = datetime.utcfromtimestamp(self.start_ms / 1000.0)
        self.year = start_dt.year
        self.month = start_dt.month
        self.day = start_dt.day

        duration_ms = max(self.end_ms - self.start_ms, 0)
        self.duration = int(math.ceil(duration_ms / float(DAY_MS))) if duration_ms > 0 else 0


def _month_start_ms(year, month):
    return timegm(date(year, month, 1).timetuple()) * 1000


def _add_months(year, month, delta):
    month_index = (year * 12 + (month - 1)) + delta
    target_year = month_index // 12
    target_month = month_index % 12 + 1
    return target_year, target_month


def _build_month_window(year, month, span_months=1):
    span_months = max(1, int(span_months))
    start_ms = _month_start_ms(year, month)
    end_year, end_month = _add_months(year, month, span_months)
    end_ms = _month_start_ms(end_year, end_month)
    return TimeParameter(start_ms, end_ms)


def _duration_days_to_month_span(duration_days):
    return max(1, int(math.ceil(max(duration_days, 1) / 28.0)))


def _parse_month_column(column_name):
    column_str = str(column_name)
    if column_str == "__padding__":
        return None

    # Factor tables produced by Spark store month buckets as month-start
    # timestamps in milliseconds.
    try:
        timestamp_ms = int(column_str)
        if timestamp_ms > 10**11:
            total_months = (
                timestamp_ms // 1000 // 60 // 60 // 24 // 28
            )  # fast reject for obvious non-date values
            if total_months >= 0:
                dt = datetime.utcfromtimestamp(timestamp_ms / 1000.0)
                return dt.year, dt.month
    except ValueError:
        pass

    if len(column_str) == 7 and column_str[4] == "-":
        try:
            return int(column_str[:4]), int(column_str[5:7])
        except ValueError:
            return None

    if len(column_str) == 6 and column_str.isdigit():
        try:
            return int(column_str[:4]), int(column_str[4:6])
        except ValueError:
            return None

    return None


def _extract_month_columns(time_bucket_df):
    month_columns = []
    for column_name in time_bucket_df.columns.tolist():
        parsed = _parse_month_column(column_name)
        if parsed is None:
            continue
        year, month = parsed
        month_columns.append((column_name, year, month))
    return month_columns


def getMedian(data, sort_key, getEntireTuple=False):
    if len(data) == 0:
        if getEntireTuple:
            return MonthYearCount(0, 0, 0)
        return 0

    if len(data) == 1:
        if getEntireTuple:
            return data[0]
        return sort_key(data[0])

    srtd = sorted(data, key=sort_key)
    mid = int(len(data) / 2)

    if len(data) % 2 == 0:
        if getEntireTuple:
            return srtd[mid]
        return (sort_key(srtd[mid - 1]) + sort_key(srtd[mid])) / 2.0

    if getEntireTuple:
        return srtd[mid]
    return sort_key(srtd[mid])


def computeTimeMedians(factors, lastmonthcount=LAST_MONTHS):
    mediantimes = []
    lastmonths = []
    firstmonths = []
    for values in factors:
        values.sort(key=lambda myc: (myc.year, myc.month))

        l = len(values)
        lastmonthsum = sum(myc.count for myc in values[max(l - lastmonthcount, 0):l])
        lastmonths.append(lastmonthsum)
        cutoff_max = l - lastmonthcount
        if cutoff_max < 0:
            cutoff_max = l
        firstmonthsum = sum(myc.count for myc in values[0:cutoff_max])
        firstmonths.append(firstmonthsum)
        mediantimes.append(getMedian(values, lambda myc: myc.count))

    median = getMedian(mediantimes, lambda x: x)
    medianLastMonth = getMedian(lastmonths, lambda x: x)
    medianFirstMonth = getMedian(firstmonths, lambda x: x)

    return medianFirstMonth, medianLastMonth, median


def getTimeParamsWithMedian(factors, medianFirstMonth, medianLastMonth, median):
    # strategy: find the median of the given distribution, then increase the time interval until it matches the given parameter
    res = []
    for values in factors:
        currentMedian = getMedian(values, lambda myc: myc.count, True)
        if int(median) == 0 or int(currentMedian.count) == 0 or int(currentMedian.year) == 0:
            empty_start = _month_start_ms(START_YEAR, 1)
            res.append(TimeParameter(empty_start, empty_start))
            continue
        if currentMedian.count > median:
            duration = int(28 * currentMedian.count / median)
        else:
            duration = int(28 * median / currentMedian.count)
        res.append(
            _build_month_window(
                currentMedian.year,
                currentMedian.month,
                _duration_days_to_month_span(duration)
            )
        )
    return res


def findTimeParameters(factors):
    medianFirstMonth, medianLastMonth, median = computeTimeMedians(factors)
    timeParams = getTimeParamsWithMedian(factors, medianFirstMonth, medianLastMonth, median)

    return timeParams


def findTimeParams(input_loan_list, time_bucket_df):
    month_columns = _extract_month_columns(time_bucket_df)
    factors = []
    for loan in input_loan_list:
        temp_factors = []
        loan_month_list = time_bucket_df.loc[loan]
        for month_key, year, month in month_columns:
            count = loan_month_list[month_key]
            if count == 0:
                continue
            temp_factors.append(MonthYearCount(month, year, count))
        factors.append(temp_factors)

    return findTimeParameters(factors)


def findTimeParamsForMonthStarts(month_starts):
    params = []
    for month_start in month_starts:
        timestamp_ms = int(month_start)
        dt = datetime.utcfromtimestamp(timestamp_ms / 1000.0)
        params.append(_build_month_window(dt.year, dt.month))
    return params


def findTimeParamsForSelectedMonths(selected_candidates):
    month_starts = []
    for candidate in selected_candidates:
        if not isinstance(candidate, (list, tuple)) or len(candidate) == 0:
            continue
        month_starts.append(int(candidate[-1]))
    return findTimeParamsForMonthStarts(month_starts)
