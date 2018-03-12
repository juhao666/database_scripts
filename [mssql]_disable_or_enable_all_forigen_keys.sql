USE $(db_name);
GO

BEGIN
    DECLARE fks CURSOR READ_ONLY
    FOR SELECT fk.name,
               OBJECT_NAME(fk.parent_object_id) table_name
        FROM sys.foreign_keys fk
	   WHERE fk.is_disabled <>0
    DECLARE @table_name VARCHAR(200);
    DECLARE @fk_name VARCHAR(200);
    OPEN fks;
    FETCH NEXT FROM fks INTO @fk_name, @table_name;
    WHILE @@fetch_status = 0
        BEGIN
            EXEC('alter table [' + @table_name +'] check constraint [' +@fk_name+']')
		  FETCH NEXT FROM fks INTO @fk_name, @table_name;
        END;
    CLOSE fks
    DEALLOCATE fks
END;
GO

--CHECK DATA
SELECT fk.name,
       OBJECT_NAME(fk.parent_object_id) table_name,
       fk.is_disabled
FROM sys.foreign_keys fk;