/* ==========================================================
   LIBNAME CONFIGURATION
   ========================================================== */
libname duck2 sasioduk
    file_path="/<BASE_PATH>/<USER_HOME>/ducklib2";

/* ==========================================================
   INITIAL SETUP (DUCKLAKE + AWS + S3)
   ========================================================== */
proc sql;
    connect using duck2;

    execute (
        SET extension_directory='/<TMP_PATH>/.extensions';

        INSTALL ducklake;
        LOAD ducklake;

        ATTACH 'ducklake:<METADATA_FILE>.ducklake' AS metadata2
        (DATA_PATH '/<BASE_PATH>/<USER_HOME>/ducklib2');

        USE metadata2;
    ) by duck2;

    execute(
        SET extension_directory = '/<TMP_PATH>/.extensions';

        INSTALL httpfs;
        LOAD httpfs;

        INSTALL aws;
        LOAD aws;

        /* Masked AWS credentials */
        CREATE OR REPLACE SECRET secret2 (
            TYPE s3,
            PROVIDER config,
            KEY_ID '<AWS_ACCESS_KEY>',
            SECRET '<AWS_SECRET_KEY>',
            REGION '<AWS_REGION>'
        );
    ) BY duck2;

quit;


/* ==========================================================
   CREATE TABLE FROM S3 (FULL DATA SAMPLE)
   ========================================================== */
proc sql;
    connect using duck2;

    execute (
        CREATE TABLE if not exists metadata2.UPI1 AS
        SELECT *
        FROM read_parquet('s3://<BUCKET_NAME>/UPI/<SOURCE_FILE>.parquet')
        LIMIT 1000;
    ) by duck2;

quit;


/* ==========================================================
   COPY TO SAS WORK
   ========================================================== */
proc sql;
    connect using duck2;

    create table work.UPI1 as
    select *
    from connection to duck2
    (
        select * from metadata2.UPI1
    );
quit;


/* ==========================================================
   TABLE CREATION (SELECTED COLUMNS)
   ========================================================== */
proc sql;
    connect using duck2;

    execute (
        CREATE TABLE if not exists metadata2.UPI2 AS
        SELECT txid, timestamp, amount, payer_state, payer_business_cat
        FROM read_parquet('s3://<BUCKET_NAME>/UPI/<SOURCE_FILE>.parquet')
        LIMIT 5;
    ) by duck2;

quit;


/* ==========================================================
   RUN TABLE IN SAS WORK
   ========================================================== */
proc sql;
    connect using duck2;

    create table work.UPI2 as
    select *
    from connection to duck2
    (
        select * from metadata2.UPI2
    );
quit;

proc print data=work.UPI2;
run;


/* ==========================================================
   INSERT
   ========================================================== */
proc sql;
    connect using duck2;

    execute (
        INSERT INTO metadata2.UPI2
        (txid, timestamp, amount, payer_state, payer_business_cat)
        VALUES
        ('1111aaaa-bbbb-cccc-dddd-222233334444',
         TIMESTAMP '2023-04-01 10:15:30',
         2500.75,
         '<STATE_CODE>',
         '<BUSINESS_CATEGORY>'),

        ('5555eeee-ffff-gggg-hhhh-666677778888',
         TIMESTAMP '2023-04-01 12:45:10',
         8900.00,
         '<STATE_CODE>',
         '<BUSINESS_CATEGORY>')
    ) by duck2;

quit;


/* ==========================================================
   UPDATE
   ========================================================== */
proc sql;
    connect using duck2;

    execute (
        UPDATE metadata2.UPI2
        SET amount = 7800.50
        WHERE txid = '<TXID_TO_UPDATE>'
    ) by duck2;

quit;


/* ==========================================================
   DELETE
   ========================================================== */
proc sql;
    connect using duck2;

    execute (
        DELETE FROM metadata2.UPI2
        WHERE txid = '<TXID_TO_DELETE>'
    ) by duck2;

quit;


/* ==========================================================
   COPY UPDATED DATA TO SAS WORK
   ========================================================== */
proc sql;
    connect using duck2;

    create table work.UPI2 as
    select *
    from connection to duck2
    (
        select * from metadata2.UPI2
    );

quit;


/* ==========================================================
   SNAPSHOTS
   ========================================================== */
proc sql;
    connect using duck2;

    create table work.UPI2_snapshots as
    select *
    from connection to duck2
    (
        SELECT *
        FROM metadata2.snapshots()
    );

quit;


/* ==========================================================
   TIME TRAVEL
   ========================================================== */
proc sql;
    connect using duck2;

    create table work.UPI2_v2 as
    select *
    from connection to duck2
    (
        SELECT *
        FROM metadata2.UPI2 AT (VERSION => 2)
    );

quit;


/* ==========================================================
   INFORMATION SCHEMA
   ========================================================== */
proc sql;
    connect using duck2;

    select *
    from connection to duck2
    (
        SELECT *
        FROM information_schema.tables
        ORDER BY table_schema, table_name
    );

    disconnect from duck2;
quit;


/* ==========================================================
   DUCKLAKE SNAPSHOT METADATA
   ========================================================== */
proc sql;
    connect using duck2;

    create table work.ducklake_snapshot as
    select *
    from connection to duck2
    (
        select *
        from "__ducklake_metadata_metadata2".main.ducklake_snapshot
    );

quit;


/* ==========================================================
   DATA CHANGE FEED (CDC)
   ========================================================== */
proc sql;
    connect using duck2;

    create table work.UPI2_changes as
    select *
    from connection to duck2
    (
        SELECT *
        FROM metadata2.table_changes('UPI2', 2, 2)
    );

quit;


/* ==========================================================
   EXPORT BACK TO S3
   ========================================================== */
proc sql;
    connect using duck2;

    execute (
        COPY metadata2.UPI2
        TO 's3://<BUCKET_NAME>/UPI/processed/<OUTPUT_FILE>.parquet'
        (FORMAT PARQUET)
    ) by duck2;

quit;


/* ==========================================================
   DUCKLAKE FEATURES DEMO
   ========================================================== */

/* TRANSACTION */
proc sql;
    connect using duck2;

    execute (
        BEGIN TRANSACTION;

        INSERT INTO metadata2.UPI2
        VALUES
        ('2222xxxx-yyyy-zzzz-aaaa-777788889999',
         TIMESTAMP '2023-04-02 14:00:00',
         5400.25,
         '<STATE_CODE>',
         '<BUSINESS_CATEGORY>');

        DELETE FROM metadata2.UPI2
        WHERE amount < 500;

        COMMIT;
    ) by duck2;

quit;


/* MERGE / UPSERT */
proc sql;
    connect using duck2;

    execute (
        MERGE INTO metadata2.UPI2 AS target
        USING (VALUES
            ('<TXID_EXISTING>', 9999.99, '<STATE_CODE>', '<UPDATED_CATEGORY>'),
            ('<TXID_NEW>', 1234.56, '<STATE_CODE>', '<NEW_CATEGORY>')
        ) AS source (txid, amount, payer_state, payer_business_cat)
        ON target.txid = source.txid
        WHEN MATCHED THEN
            UPDATE SET amount = source.amount,
                       payer_business_cat = source.payer_business_cat
        WHEN NOT MATCHED THEN
            INSERT (txid, amount, payer_state, payer_business_cat)
            VALUES (source.txid, source.amount, source.payer_state, source.payer_business_cat);
    ) by duck2;

quit;


/* PARTITIONING */
proc sql;
    connect using duck2;

    execute (
        ALTER TABLE metadata2.UPI2
        SET PARTITIONED BY (payer_state);
    ) by duck2;

quit;


/* VIEW CREATION */
proc sql;
    connect using duck2;

    execute (
        CREATE VIEW if not exists metadata2.UPI2_high_value AS
        SELECT txid, amount, payer_state
        FROM metadata2.UPI2
        WHERE amount > 5000;
    ) by duck2;

    create table work.UPI2_high as
    select *
    from connection to duck2
    (
        select * from metadata2.UPI2_high_value
    );

quit;


/* ENCRYPTED TABLE */
proc sql;
    connect using duck2;

    execute (
        CREATE TABLE metadata2.UPI2_secure (
            txid VARCHAR,
            timestamp TIMESTAMP,
            amount DOUBLE,
            payer_state VARCHAR,
            payer_business_cat VARCHAR
        )
        WITH (encrypted = true)
    ) by duck2;

quit;


/* SCHEMA EVOLUTION */
proc sql;
    connect using duck2;

    execute (
        ALTER TABLE metadata2.UPI2
        ADD COLUMN remarks VARCHAR
    ) by duck2;

quit;


/* FINAL TABLE LIST */
proc sql;
    connect using duck2;

    select *
    from connection to duck2
    (
        SELECT *
        FROM information_schema.tables
        ORDER BY table_schema, table_name
    );

    disconnect from duck2;
quit;
