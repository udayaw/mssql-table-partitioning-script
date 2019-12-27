--ALTER PROCEDURE CreatePartition(partition_key integer)
--AS
DECLARE
@database_name varchar(50) = 'test_db',
@table_name varchar(100) = 'dbo.parted',
@partition_column varchar(10) = 'date_key',
@partition_prefix varchar(50) = 'test_part',
@data_dir varchar(50) = 'D:\\Garbage\\mssq-partitions',
@partition_id varchar(50)='20191103'
BEGIN
	declare @execute_sql_query nvarchar(max);

	--create file group
	IF NOT EXISTS (select * from sys.filegroups where name='fg_'+ @partition_prefix + '_' + @partition_id)
	BEGIN
		set @execute_sql_query = 'ALTER DATABASE ' + @database_name + ' ADD FILEGROUP fg_' + @partition_prefix + '_' + @partition_id;
		exec sp_executesql @execute_sql_query;
	END
	ELSE
	BEGIN
		PRINT 'file group already exists. exiting...';
		--RETURN;
	END;

	--create file for the filegroup
	IF NOT EXISTS (select * from sys.master_files where name='file_' + @partition_prefix + '_' + @partition_id)
	BEGIN
	set @execute_sql_query = 
	'ALTER DATABASE ' + @database_name +' 
	ADD FILE   
	(  
		NAME = file_' + @partition_prefix + '_' + @partition_id + ',  
		FILENAME = ''' + @data_dir +'\\file_' + @partition_prefix + '_' + @partition_id + '.mdf''
	)  TO FILEGROUP fg_' + @partition_prefix + '_' + @partition_id;
	exec sp_executesql @execute_sql_query;
	END
	ELSE
	BEGIN
		PRINT 'db file already exists. exiting...';
		--RETURN;
	END
	
	--create partition function
	IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_'+@partition_prefix)
	BEGIN
		PRINT 'partition function already exist. altering...';

		--check if boundary the boundary exists
		IF EXISTS(	SELECT b.* FROM sys.partition_functions f 
				left join sys.partition_range_values b ON f.function_id = b.function_id
				where b.value = @partition_id)
		BEGIN
			set @execute_sql_query = 'ALTER PARTITION FUNCTION pf_'+@partition_prefix + '()
			SPLIT RANGE (' + @partition_id + ')';
			exec sp_executesql @execute_sql_query;
		END
		ELSE
		BEGIN
			PRINT 'partition boundary value already exist. exiting...';
			--RETURN;
		END;
	END
	ELSE
	BEGIN
		PRINT 'partition function does not exist. creating...';
		set @execute_sql_query = 'CREATE PARTITION FUNCTION pf_'+@partition_prefix+' (int)  
		AS RANGE LEFT FOR VALUES (' + @partition_id + ');';
		exec sp_executesql @execute_sql_query;
	END
	
	
	--create partition scheme
	IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'pscheme_'+@partition_prefix)
	BEGIN
		PRINT 'partition scheme already exists. altering...';
		set @execute_sql_query = 'ALTER PARTITION SCHEME pscheme_'+@partition_prefix + ' 
		NEXT USED fg_' + @partition_prefix + '_' + @partition_id;
		exec sp_executesql @execute_sql_query;
		
	END
	ELSE
	BEGIN
		PRINT 'partition scheme does not exist. creating...';
		set @execute_sql_query = 'CREATE PARTITION SCHEME pscheme_'+@partition_prefix + ' 
		AS PARTITION pf_'+@partition_prefix+'
		TO (fg_' + @partition_prefix + '_' + @partition_id+',fg_' + @partition_prefix + '_' + @partition_id + ');';
		exec sp_executesql @execute_sql_query;
		
	END;


	--update table with scheme if not updated using a clustered index
	IF NOT EXISTS (SELECT * 
	FROM sys.indexes 
	WHERE name='idx_clustered_'+@partition_prefix AND object_id = OBJECT_ID(@table_name))
	BEGIN
		set @execute_sql_query = 'CREATE CLUSTERED INDEX idx_clustered_'+@partition_prefix+' ON ' + @table_name + '(' + @partition_column + ') ON pscheme_'+@partition_prefix + '('+@partition_column+')';
		exec sp_executesql @execute_sql_query;
		PRINT 'table is partitioned now. use the utility query for details';
	END
	ELSE
		PRINT 'table if already partitioned. not creating clustered index';
END


/*
SELECT
DB_NAME() AS 'DatabaseName'
,OBJECT_NAME(p.OBJECT_ID) AS 'TableName'
,p.index_id AS 'IndexId'
,CASE
WHEN p.index_id = 0 THEN 'HEAP'
ELSE i.name
END AS 'IndexName'
,p.partition_number AS 'PartitionNumber'
,prv_left.value AS 'LowerBoundary'
,prv_right.value AS 'UpperBoundary'
,ps.name as PartitionScheme
,pf.name as PartitionFunction
,CASE
WHEN fg.name IS NULL THEN ds.name
ELSE fg.name
END AS 'FileGroupName'
,CAST(p.used_page_count * 0.0078125 AS NUMERIC(18,2)) AS 'UsedPages_MB'
,CAST(p.in_row_data_page_count * 0.0078125 AS NUMERIC(18,2)) AS 'DataPages_MB'
,CAST(p.reserved_page_count * 0.0078125 AS NUMERIC(18,2)) AS 'ReservedPages_MB'
,CASE
WHEN p.index_id IN (0,1) THEN p.row_count
ELSE 0
END AS 'RowCount'
,CASE
WHEN p.index_id IN (0,1) THEN 'data'
ELSE 'index'
END 'Type'
FROM sys.dm_db_partition_stats p
INNER JOIN sys.indexes i
ON i.OBJECT_ID = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.data_spaces ds
ON ds.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_schemes ps
ON ps.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.partition_functions pf
ON ps.function_id = pf.function_id
LEFT OUTER JOIN sys.destination_data_spaces dds
ON dds.partition_scheme_id = ps.data_space_id
AND dds.destination_id = p.partition_number
LEFT OUTER JOIN sys.filegroups fg
ON fg.data_space_id = dds.data_space_id
LEFT OUTER JOIN sys.partition_range_values prv_right
ON prv_right.function_id = ps.function_id
AND prv_right.boundary_id = p.partition_number
LEFT OUTER JOIN sys.partition_range_values prv_left
ON prv_left.function_id = ps.function_id
AND prv_left.boundary_id = p.partition_number - 1
WHERE
OBJECTPROPERTY(p.OBJECT_ID, 'ISMSSHipped') = 0
AND p.index_id IN (0,1)
and p.object_id = object_id('dbo.parted') ;
*/
