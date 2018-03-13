USE $(DB_NAME)

SET NOCOUNT ON
/*clearup FK dirty data
--1. clearup reference table dirty data
--2. clearup parent table dirty data
*/

DECLARE @isSkippedStep1 CHAR(1) = '1' -- 0 means will run step 1 and step 2

IF @isSkippedStep1 == '1'
BEGIN
 GOTO LabelStep2
END

--Step 1 :clearup dirty data in referenced tables
DECLARE pks CURSOR READ_ONLY FOR
SELECT DISTINCT fk.referenced_object_id,fkc.referenced_column_id,OBJECT_NAME(c.object_id) AS reference_table_name,c.name AS reference_column_name
FROM sys.foreign_keys fk 
JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
LEFT JOIN sys.columns c ON fk.referenced_object_id=c.object_id AND fkc.referenced_column_id=c.column_id

DECLARE @ref_obj_id INT
DECLARE @ref_col_id INT
DECLARE @ref_tab_name VARCHAR(200)
DECLARE @ref_col_name VARCHAR(200)
DECLARE @sql VARCHAR(MAX)
OPEN pks
FETCH NEXT FROM pks INTO @ref_obj_id,@ref_col_id,@ref_tab_name,@ref_col_name

WHILE @@fetch_status = 0
BEGIN
SET @sql ='delete p from [' + @ref_tab_name + '] p WHERE 1 =1 ' 
    DECLARE fks CURSOR READ_ONLY FOR
    SELECT OBJECT_NAME(fk.parent_object_id) AS parent_table_name,c.name AS parent_column_name
    FROM sys.foreign_keys fk 
    JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
    LEFT JOIN sys.columns c ON c.object_id=fk.parent_object_id AND c.column_id=fkc.parent_column_id
    WHERE fk.referenced_object_id=@ref_obj_id AND fkc.referenced_column_id = @ref_col_id

    DECLARE @table_name VARCHAR(200)
    DECLARE @column_name VARCHAR(200)
    OPEN fks
    FETCH NEXT FROM fks INTO @table_name,@column_name
    WHILE @@fetch_status = 0
    BEGIN
    SET @sql = @sql + ' and not exists(select 1 from ['+@table_name+'] t where t.'+@column_name+'=p.'+@ref_col_name+') '
    FETCH NEXT FROM fks INTO @table_name,@column_name
    END;
    CLOSE fks
    DEALLOCATE fks
--PRINT @sql
EXEC(@sql)
FETCH NEXT FROM pks INTO @ref_obj_id,@ref_col_id,@ref_tab_name,@ref_col_name
END
CLOSE pks
DEALLOCATE pks

LabelStep2:

--Step2:clearup dirty data in parent tables
GO
DECLARE fks CURSOR READ_ONLY FOR
SELECT 
OBJECT_NAME(rc.object_id) AS reference_table_name
,rc.name AS reference_column_name
,OBJECT_NAME(pc.object_id) AS parent_table_name
,pc.name  AS parent_column_name
FROM sys.foreign_keys fk 
JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
LEFT JOIN sys.columns pc ON fkc.parent_column_id=pc.column_id AND fkc.parent_object_id=pc.object_id
LEFT JOIN sys.columns rc ON fkc.referenced_column_id=rc.column_id AND fkc.referenced_object_id=rc.object_id
ORDER BY rc.object_id

DECLARE @ref_tab_name VARCHAR(200)
DECLARE @ref_col_name VARCHAR(200)
DECLARE @parent_tab_name VARCHAR(200)
DECLARE @parent_col_name VARCHAR(200)
DECLARE @sql VARCHAR(max)

OPEN fks
FETCH NEXT FROM fks INTO @ref_tab_name,@ref_col_name,@parent_tab_name,@parent_col_name
WHILE @@fetch_status = 0
BEGIN
SET @sql = 'DELETE t FROM ['+@parent_tab_name+'] t WHERE NOT EXISTS (SELECT 1 FROM ['+@ref_tab_name+'] r WHERE r.'+@ref_col_name+'=t.'+@parent_col_name+')'
--PRINT @sql
EXEC(@sql)
FETCH NEXT FROM fks INTO @ref_tab_name,@ref_col_name,@parent_tab_name,@parent_col_name
END;
CLOSE fks
DEALLOCATE fks

GO

PRINT '[INFO]- CLEARUP COMPLETED SUCCESSFULLY'

