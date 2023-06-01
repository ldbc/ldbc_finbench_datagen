-- Read Write 1 & 2

-- Load raw data
CREATE
OR REPLACE VIEW TransferInsertsAll AS
SELECT *
FROM read_csv_auto(':output_dir/incremental/AddAccountTransferAccountAll.csv', delim = '|', header = TRUE);

-- sample for read writes
CREATE
OR REPLACE TABLE TransferReadWrites AS (SELECT * FROM TransferInsertsAll USING SAMPLE 20 PERCENT (bernoulli, 23));

-- Transfer Left for insert
COPY
(
SELECT *
FROM TransferInsertsAll
WHERE NOT EXISTS(SELECT *
                 FROM TransferReadWrites
                 WHERE TransferInsertsAll.createTime = TransferReadWrites.createTime
                   AND TransferInsertsAll.fromId = TransferReadWrites.fromId
                   AND TransferInsertsAll.toId = TransferReadWrites.toId)
ORDER BY TransferInsertsAll.createTime )
TO ':output_dir/incremental/AddAccountTransferAccountWrite12.csv' (DELIMITER '|', HEADER);


-- Read Write 1 Raw
CREATE
OR REPLACE TABLE TransferReadWrite1 AS (SELECT * FROM TransferReadWrites USING SAMPLE 50 PERCENT (bernoulli, 23));

-- Read Write 2 Raw
CREATE
OR REPLACE TABLE TransferReadWrite2 AS (
    SELECT *
    FROM TransferReadWrites
    WHERE NOT EXISTS(
        SELECT *
        FROM TransferReadWrite1
        WHERE TransferReadWrites.createTime = TransferReadWrite1.createTime
          AND TransferReadWrites.fromId = TransferReadWrite1.fromId
          AND TransferReadWrites.toId = TransferReadWrite1.toId
    )
    ORDER BY TransferReadWrites.createTime
);

-- Read Write 1 output
COPY
(
SELECT TransferReadWrite1.*,
       (SELECT min(createTime) FROM TransferReadWrite1) AS startTime,
       (SELECT max(createTime) FROM TransferReadWrite1) AS endTime
FROM TransferReadWrite1 )
TO ':output_dir/incremental/AddAccountTransferAccountReadWrite1.csv' (DELIMITER '|', HEADER);

-- Read Write 2 output
COPY
(
SELECT TransferReadWrite2.*,
       (SELECT min(createTime) FROM TransferReadWrite2) AS startTime,
       (SELECT max(createTime) FROM TransferReadWrite2) AS endTime,
       :truncation_limit                                AS truncation_limit,
       ':truncation_order'                              AS truncation_order,
       :rw2_amount_threshold                            AS amount_threshold,
       :rw2_ratio_threshold                             AS ratio_threshold
FROM TransferReadWrite2 )
TO ':output_dir/incremental/AddAccountTransferAccountReadWrite2.csv' (DELIMITER '|', HEADER);

-- Read Write 3

-- Load raw data
CREATE
OR REPLACE VIEW PersonGuarantee AS
SELECT *
FROM read_csv_auto(':output_dir/incremental/AddPersonGuaranteePersonAll.csv', delim = '|', header = TRUE);

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
TO ':output_dir/incremental/AddPersonGuaranteePersonWrite10.csv' (DELIMITER '|', HEADER);


-- Read Write 3 output
COPY
(
SELECT PGPReadWrite3.*,
       (SELECT min(createTime) FROM PGPReadWrite3) AS startTime,
       (SELECT max(createTime) FROM PGPReadWrite3) AS endTime,
       :truncation_limit                           AS truncation_limit,
       ':truncation_order'                         AS truncation_order,
       :rw3_amount_threshold                       AS amount_threshold,
FROM PGPReadWrite3 )
TO ':output_dir/incremental/AddPersonGuaranteePersonReadWrite3.csv' (DELIMITER '|', HEADER);

