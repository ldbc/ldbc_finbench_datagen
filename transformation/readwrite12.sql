-- Read Write 1 & 2

-- Load raw data
CREATE
OR REPLACE VIEW TransferInsertsAll AS
SELECT *
FROM read_csv_auto(':output_dir/inserts/Transfer.csv', delim = '|', header = TRUE);

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
TO ':output_dir/inserts/TransferInserts.csv' (DELIMITER '|', HEADER);


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
       (SELECT max(createTime) FROM TransferReadWrite1) AS endTime,
       :truncation_limit                                AS TRUNCATION_LIMIT,
       ':truncation_order'                              AS TRUNCATION_ORDER
FROM TransferReadWrite1 )
TO ':output_dir/readwrites/AccountTransferAccountReadWrite1.csv' (DELIMITER '|', HEADER);

-- Read Write 2 output
COPY
(
SELECT TransferReadWrite2.*,
       (SELECT min(createTime) FROM TransferReadWrite2) AS startTime,
       (SELECT max(createTime) FROM TransferReadWrite2) AS endTime,
       :truncation_limit                                AS TRUNCATION_LIMIT,
       ':truncation_order'                              AS TRUNCATION_ORDER
FROM TransferReadWrite2 )
TO ':output_dir/readwrites/AccountTransferAccountReadWrite2.csv' (DELIMITER '|', HEADER);
