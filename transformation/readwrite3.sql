-- Read Write 1 & 2

-- Load raw data
CREATE
OR REPLACE VIEW PersonGuarantee AS
SELECT *
FROM read_csv_auto(':output_dir/inserts/AddPersonGuaranteePerson.csv', delim = '|', header = TRUE);

-- Sample for read write 3
CREATE
OR REPLACE TABLE PGPReadWrite3 AS (SELECT * FROM PersonGuarantee USING SAMPLE 20 PERCENT (bernoulli, 23));

-- PGP Left for insert
COPY
(
SELECT *
FROM PersonGuarantee
WHERE NOT EXISTS(SELECT *
                 FROM PGPReadWrite3
                 WHERE PersonGuarantee.createTime = PGPReadWrite3.createTime
                   AND PersonGuarantee.fromId = PGPReadWrite3.fromId
                   AND PersonGuarantee.toId = PGPReadWrite3.toId)
ORDER BY PersonGuarantee.createTime )
TO ':output_dir/inserts/AddPersonGuaranteePersonInserts.csv' (DELIMITER '|', HEADER);


-- Read Write 3 output
COPY
(
SELECT PGPReadWrite3.*,
       (SELECT min(createTime) FROM PGPReadWrite3) AS startTime,
       (SELECT max(createTime) FROM PGPReadWrite3) AS endTime,
       :truncation_limit                           AS TRUNCATION_LIMIT,
       ':truncation_order'                         AS TRUNCATION_ORDER
FROM PGPReadWrite3 )
TO ':output_dir/readwrites/AddPersonGuaranteePersonReadWrite3.csv' (DELIMITER '|', HEADER);
