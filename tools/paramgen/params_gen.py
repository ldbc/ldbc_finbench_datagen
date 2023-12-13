from os import system
from sys import argv
import json
import random


data_prefix = './finbench/sf1/snapshot/'
params_input_prefix = './input_data/read_params/'
params_output_prefix = './output_data/read_params/'
create_validation_cmd = 'java -cp ../target/tugraph-0.2.0-SNAPSHOT.jar org.ldbcouncil.finbench.driver.driver.Driver -P params_gen.properties -p "ldbc.finbench.transaction.queries.ComplexRead%d_enable|true" -p "validation_parameters_size|%d" -p "validate_database|validation_params.csv.%d"'


def get_ids(file):
    ids = []
    with open(file) as f:
        for line in f:
            fields = line.strip('\n').split('|')
            ids.append(fields[0])
    return ids

def get_accounts(data_prefix):
    return get_ids(data_prefix + 'Account.csv')

def get_person(data_prefix):
    return get_ids(data_prefix + 'Person.csv')

def get_loans(data_prefix):
    return get_ids(data_prefix + 'Loan.csv')

def write_params_tcr1(file, tcr):
    with open(file, 'w') as f:
        f.write('...\n')
        for param in tcr:
            f.write('%s|1627020616747|1669690342640|500|TIMESTAMP_DESCENDING\n' % param)

def write_params_tcr6(file, tcr):
    with open(file, 'w') as f:
        f.write('...\n')
        for param in tcr:
            f.write('%s|0.0|0.0|1627020616747|1669690342640|500|TIMESTAMP_DESCENDING\n' % param)

def write_params_tcr7(file, tcr):
    with open(file, 'w') as f:
        f.write('...\n')
        for param in tcr:
            f.write('%s|0.0|1627020616747|1669690342640|500|TIMESTAMP_DESCENDING\n' % param)

def write_params_tcr3(file, tcr):
    with open(file, 'w') as f:
        # f.write('...\n')
        # f.write('accountId,id2,startTime,endTime\n')
        for param in tcr:
            f.write('%s|%s|1627020616747|1669690342640\n' % param)

def write_params_tcr10(file, tcr):
    with open(file, 'w') as f:
        f.write('...\n')
        for param in tcr:
            f.write('%s|%s|1627020616747|1669690342640\n' % param)

def get_single(nodes, tcr_num):
    return nodes[:tcr_num]

def get_random_pair(nodes, tcr_num):
    size = len(nodes)
    res = []
    for i in range(tcr_num):
        lef = random.randint(0, size - 1)
        rig = lef
        while rig == lef:
            rig = random.randint(0, size - 1)
        res.append((nodes[lef], nodes[rig]))
    return res

def gen_tcr(nodes, tcr_id, tcr_num, tcr_res_num, write_params_tcr, get_parmas=get_single):
    tcr = get_parmas(nodes, tcr_num)
    write_params_tcr(params_input_prefix + 'complex_%d_param.csv' % tcr_id, tcr)
    system(create_validation_cmd % (tcr_id, tcr_num, tcr_id))
    tcr_res = []
    with open('./validation_params.csv.%d' % tcr_id, 'r') as f:
        content = f.read()
        content = json.loads(content)
        for item in content:
            res = item['operationResult']
            if len(res) > 0:
                # print(item['operationResult'])
                res_item = item["operation"]["ComplexRead%d" % tcr_id]
                if "id" in res_item:
                    tcr_res.append(res_item["id"])
                elif ("pid1" in res_item) and (res[0]['jaccardSimilarity'] != 0.0):
                    tcr_res.append((res_item["pid1"], res_item["pid2"]))
                elif "id1" in res_item:
                    if tcr_id == 3 and res[0]['shortestPathLength'] == -1:
                        continue
                    tcr_res.append((res_item["id1"], res_item["id2"]))
    write_params_tcr(params_output_prefix + 'complex_%d_param.csv' % tcr_id, tcr_res[:tcr_res_num])


def main():
    accounts = get_accounts(data_prefix)
    person = get_person(data_prefix)
    loans = get_loans(data_prefix)
    tcr_id = int(argv[1])

    if tcr_id == 1:
        gen_tcr(accounts, 1, 3000, 1000, write_params_tcr1)
    elif tcr_id == 2:
        gen_tcr(person, 2, 1200, 1000, write_params_tcr1)
    elif tcr_id == 5:
        gen_tcr(person, 5, 1200, 1000, write_params_tcr1)
    elif tcr_id == 6:
        gen_tcr(accounts, 6, 12000, 1000, write_params_tcr6)
    elif tcr_id == 7:
        gen_tcr(accounts, 7, 1200, 1000, write_params_tcr7)
    elif tcr_id == 8:
        gen_tcr(loans, 8, 1500, 1000, write_params_tcr7)
    elif tcr_id == 9:
        gen_tcr(accounts, 9, 1200, 1000, write_params_tcr7)
    elif tcr_id == 11:
        gen_tcr(person, 11, 4000, 1000, write_params_tcr1)
    elif tcr_id == 12:
        gen_tcr(person, 12, 2400, 1000, write_params_tcr1)
    elif tcr_id == 3:
        gen_tcr(accounts, 3, 6000, 1000, write_params_tcr3, get_random_pair)
    elif tcr_id == 4:
        gen_tcr(accounts, 4, 10000, 1000, write_params_tcr3, get_random_pair)
    elif tcr_id == 10:
        gen_tcr(person, 10, 1000, 1000, write_params_tcr10, get_random_pair)


if __name__ == '__main__':
    main()
