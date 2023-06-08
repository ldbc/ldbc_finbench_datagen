## ParamsGen

`params_gen.py` uses the CREATE_VALIDATION feature to generate parameters. 

The specific steps are as follows:

1. Select vertices of type Account, Person, and Loan from the dataset, and generate a parameter file that meets the input specifications for ldbc_finbench_driver.
2. Execute CREATE_VALIDATION to generate validation_params.csv.
3. Select non-empty results from validation_params.csv.

Example:

```bash
python3 params_gen.py 1 # gen tcr1 params
```

Other notes:

1. The generated start_timestamp and end_timestamp in the current version are fixed values.
2. For tcr4 and tcr10, this method is not efficient enough. Use the following Cypher query to search for parameters:

```Cypher
// tcr4
MATCH
    (n1:Account)-[:transfer]->
    (n2:Account)-[:transfer]->
    (n3:Account)-[:transfer]->(n4:Account)
WHERE
    n1.id = n4.id AND n1.id > n2.id AND n2.id > n3.id
WITH
	  n1.id as n1id,
    n2.id as n2id,
    n3.id as n3id,
    n4.id as n4id
LIMIT 1000
RETURN DISTINCT toString(n1id)+"|"+toString(n2id)

// tcr10
MATCH
    (c:Company)<-[:invest]-(p:Person)
WITH
	  c.id as cid,
    count(p.id) as num,
		collect(p.id) as person
WHERE num >= 2
RETURN
    tostring(person[0])+"|"+tostring(person[1])
LIMIT 1000
```
