LAST_MONTHS = 3
START_YEAR = 2020

class MonthYearCount:
    def __init__(self, month, year, count):
        self.month=month
        self.year=year
        self.count=count


class TimeParameter:
    def __init__(self, year, month, day, duration):
        self.month=month
        self.year=year
        self.day=day
        self.duration=duration
        

def getMedian(data, sort_key, getEntireTuple = False):
    if len(data) == 0:
        if getEntireTuple:
            return MonthYearCount(0,0,0)
        return 0

    if len(data) == 1:
        if getEntireTuple:
            return data[0]
        return data[0].count

    srtd = sorted(data,key=sort_key)
    mid = int(len(data)/2)

    if len(data) % 2 == 0:
        if getEntireTuple:
            return srtd[mid]
        return (sort_key(srtd[mid-1]) + sort_key(srtd[mid])) / 2.0

    if getEntireTuple:
        return srtd[mid]
    return sort_key(srtd[mid])


def computeTimeMedians(factors, lastmonthcount = LAST_MONTHS):
    mediantimes = []
    lastmonths = []
    firstmonths = []
    for values in factors:
        values.sort(key=lambda myc: (myc.year, myc.month))

        l = len(values)
        lastmonthsum = sum(myc.count for myc in values[max(l-lastmonthcount,0):l])
        lastmonths.append(lastmonthsum)
        cutoff_max = l-lastmonthcount
        if cutoff_max < 0:
            cutoff_max = l
        firstmonthsum = sum(myc.count for myc in values[0:cutoff_max])
        firstmonths.append(firstmonthsum)
        mediantimes.append(getMedian(values,lambda myc: myc.count))

    median = getMedian(mediantimes, lambda x: x)
    medianLastMonth = getMedian(lastmonths, lambda x: x)
    medianFirstMonth = getMedian(firstmonths, lambda x: x)

    return medianFirstMonth, medianLastMonth, median
        

def getTimeParamsWithMedian(factors, medianFirstMonth, medianLastMonth, median):
    # strategy: find the median of the given distribution, then increase the time interval until it matches the given parameter
    res = []
    for values in factors:
        input = sorted(values,key=lambda myc: (myc.year, myc.month))
        currentMedian = getMedian(values,lambda myc: myc.count, True)
        if int(median) == 0 or int(currentMedian.count) == 0 or int(currentMedian.year) == 0:
            res.append(TimeParameter(START_YEAR,1,1,0))
            continue
        if currentMedian.count > median:
            duration = int(28*currentMedian.count/median)
            res.append(TimeParameter(currentMedian.year, currentMedian.month, 1, duration))
        else:
            duration = int(28*median/currentMedian.count)
            res.append(TimeParameter(currentMedian.year, currentMedian.month, 1, duration))
    return res
        

def findTimeParameters(factors):
    
    medianFirstMonth, medianLastMonth, median = computeTimeMedians(factors)
    timeParams = getTimeParamsWithMedian(factors, medianFirstMonth, medianLastMonth, median)

    return timeParams

    
def findTimeParams(input_loan_list, time_bucket_df):
    time_list = [x for x in time_bucket_df.iloc[0].index.tolist()[1:]]
    factors = []
    for loan in input_loan_list:
        temp_factors = []
        loan_month_list = time_bucket_df.loc[loan]
        for month, month_str in enumerate(time_list):
            count = loan_month_list[month_str]
            if count == 0:
                continue
            year = START_YEAR + month / 12
            temp_factors.append(MonthYearCount(month % 12 + 1, int(year), count))
        factors.append(temp_factors)
    
    return findTimeParameters(factors)


