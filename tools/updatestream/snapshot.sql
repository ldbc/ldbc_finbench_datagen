--- person
COPY (
SELECT *
FROM Person
WHERE Person.createTime <= :start_date_long
ORDER BY Person.createTime
)
TO ':output_dir/snapshot/Person.parquet' (FORMAT 'parquet');

--- company
COPY (
SELECT *
FROM Company
WHERE Company.createTime <= :start_date_long
ORDER BY Company.createTime
)
TO ':output_dir/snapshot/Company.parquet' (FORMAT 'parquet');

-- account