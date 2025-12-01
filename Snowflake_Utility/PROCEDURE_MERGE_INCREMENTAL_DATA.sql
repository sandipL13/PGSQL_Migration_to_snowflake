CREATE OR REPLACE PROCEDURE STAGGING.PROC.MERGE_INCREMENTAL_DATA("P_SCHEMA_NAME" VARCHAR, "P_TABLE_NAME" VARCHAR, "P_PRI_KEY" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    last_run TIMESTAMP;
    sql_stmt VARCHAR;
    columns_list VARCHAR;
    update_clause VARCHAR;
    insert_values VARCHAR;
BEGIN
    -- Get last run timestamp for the specified table
    last_run := (
        SELECT INSERT_TIMESTAMP 
        FROM STAGGING.CONFIG.LAST_RUN_TIMESTAMP 
        WHERE SCHEMA_NAME = :p_schema_name AND TABLE_NAME = :p_table_name
    );
    IF (last_run IS NULL) THEN
        last_run := ''1970-01-01''::TIMESTAMP;
    END IF;

    columns_list := (
    SELECT LISTAGG(''"'' || COLUMN_NAME || ''"'', '', '')
    WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM RAW.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :p_schema_name AND TABLE_NAME = :p_table_name
     );

    update_clause := (
    SELECT LISTAGG(''target."'' || COLUMN_NAME || ''" = source."'' || COLUMN_NAME || ''"'', '', '')
    WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM RAW.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :p_schema_name AND TABLE_NAME = :p_table_name 
    );
    insert_values := (
    SELECT LISTAGG(''source."'' || COLUMN_NAME || ''"'', '', '')
    WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM RAW.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :p_schema_name AND TABLE_NAME = :p_table_name
    );

    -- Build the dynamic SQL statement
    sql_stmt := ''
        MERGE INTO STAGGING.'' || :p_schema_name || ''.'' || :p_table_name || '' AS target
        USING (
            SELECT ''|| columns_list ||'' FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY ''|| :p_pri_key ||'' ORDER BY insert_timestamp DESC) AS rn
                FROM RAW.'' || :p_schema_name || ''.'' || :p_table_name || ''
                WHERE insert_timestamp > '''''' || :last_run || ''''''  
            ) WHERE rn = 1
        ) AS source
        ON target.''|| :p_pri_key ||'' = source.''|| :p_pri_key ||''
        WHEN MATCHED THEN UPDATE SET
            '' || update_clause || ''
        WHEN NOT MATCHED THEN INSERT ('' || columns_list || '') VALUES (
            '' || insert_values || ''
        );
    '';
    EXECUTE IMMEDIATE sql_stmt;

    -- Update last run timestamp
    sql_stmt := ''
        MERGE INTO STAGGING.CONFIG.LAST_RUN_TIMESTAMP AS target
        USING (SELECT '''''' || :p_schema_name || '''''' AS SCHEMA_NAME, '''''' || :p_table_name || '''''' AS TABLE_NAME, MAX(insert_timestamp) AS INSERT_TIMESTAMP FROM RAW.'' || :p_schema_name || ''.'' || :p_table_name || '' WHERE insert_timestamp > '''''' || last_run || '''''' ) AS source
        ON target.SCHEMA_NAME = source.SCHEMA_NAME AND target.TABLE_NAME = source.TABLE_NAME
        WHEN MATCHED THEN UPDATE SET INSERT_TIMESTAMP = source.INSERT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT VALUES (source.SCHEMA_NAME, source.TABLE_NAME, source.INSERT_TIMESTAMP);
    '';
    EXECUTE IMMEDIATE sql_stmt;

    RETURN ''Incremental merge completed for '' || p_schema_name || ''.'' || p_table_name;
END;
';