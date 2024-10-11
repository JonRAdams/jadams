#azure_workload_files_v2410.ps1

# Clear append setting to clear existing files 
$IsAppend = 0

# Designate the client name
$ClientName = "TestClient"

# Designate the output file location
$Path = 'C:\Users\Public\Documents\workload\'

$TsExt = Get-Date -Format yyyyMMdd_HHmmss

# Import the target server data
$CSVData = Import-Csv -Path "C:\Users\Public\Documents\servers.csv" | ? { $_.enabled -eq "Y" }

# Process each line of the CSV file
# Assigning the columns to respective variables
ForEach ($Row in $CSVData) { 

$SQLServer = $Row.server_ip
$usr = $Row.username
$pwd = $Row.password
$db = "master"

Write-Host "Server:" $SQLServer

if($usr)
{
  $auth=@{UserName=$usr;Password=$pwd}
}
else
{
  $auth=@{}
}

$cpuquery = "DECLARE @ts_now bigint = (SELECT cpu_ticks/(cpu_ticks/ms_ticks) `
FROM sys.dm_os_sys_info); `
SELECT '$($ClientName)' AS Client, ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),'AzureSQL') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
DAT.EventTime `
, DAT.SQLProcessUtilization ` 
, DAT.SystemIdle `
, 100 - (DAT.SystemIdle + DAT.SQLProcessUtilization) OtherUtilization `
FROM ( `
SELECT record.value('(./Record/@id)[1]', 'int') record_id `
, record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') SystemIdle ` 
, record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') SQLProcessUtilization `
, EventTime `
FROM ( `
SELECT DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) EventTime `
, [timestamp] `
, CONVERT(xml, record) AS [record] ` 
FROM sys.dm_os_ring_buffers `
WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' ` 
AND record LIKE N'%<SystemHealth>%') AS x `
) AS DAT `
ORDER BY DAT.EventTime ASC --DESC"

$expqueries = "SELECT TOP(10) qs.execution_count AS [Execution Count], `
'$($ClientName)' AS Client, ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),'AzureSQL') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
(qs.total_logical_reads)*8/1024.0 AS [Total Logical Reads (MB)], `
(qs.total_logical_reads/qs.execution_count)*8/1024.0 AS [Avg Logical Reads (MB)], `
(qs.total_worker_time)/1000.0 AS [Total Worker Time (ms)], `
(qs.total_worker_time/qs.execution_count)/1000.0 AS [Avg Worker Time (ms)], `
(qs.total_elapsed_time)/1000.0 AS [Total Elapsed Time (ms)], `
(qs.total_elapsed_time/qs.execution_count)/1000.0 AS [Avg Elapsed Time (ms)], `
qs.creation_time AS [Creation Time], `
--t.text AS [Complete Query Text], `
CONVERT(nVARCHAR(max),qs.sql_handle, 1) AS [Query ID] `
FROM sys.dm_exec_query_stats AS qs WITH (NOLOCK) `
CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS t `
--CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp `
WHERE t.dbid = DB_ID() `
ORDER BY qs.execution_count DESC OPTION (RECOMPILE) -- frequently ran query `
-- ORDER BY [Total Logical Reads (MB)] DESC OPTION (RECOMPILE);-- High Disk Reading query `
-- ORDER BY [Avg Worker Time (ms)] DESC OPTION (RECOMPILE);-- High CPU query `
-- ORDER BY [Avg Elapsed Time (ms)] DESC OPTION (RECOMPILE);-- Long Running query"

$connquery = "SELECT '$($ClientName)' AS Client, ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),'AzureSQL') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS DateTime, DB_NAME(dbid) as DBName, `
COUNT(dbid) as NumberOfConnections `
FROM sys.sysprocesses `
WHERE dbid > 0 `
GROUP BY dbid"

$waitsquery = "SELECT TOP 10 wait_type AS [Wait Type], `
'$($ClientName)' AS Client, ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),'AzureSQL') AS [CurrentNodeName], ` 
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
wait_time_ms/1000.0 AS [Total Wait Time (second)], `
(wait_time_ms-signal_wait_time_ms)/1000.0 AS [Resource Wait Time (second)], `
signal_wait_time_ms/1000.0 AS [Signal Wait Time (second)], `
waiting_tasks_count AS [Wait Count] `
FROM sys.dm_os_wait_stats `
WHERE wait_type NOT IN `
(N'CLR_SEMAPHORE', `
N'LAZYWRITER_SLEEP', ` 
N'RESOURCE_QUEUE', `
N'SQLTRACE_BUFFER_FLUSH', `
N'SLEEP_TASK', `
N'SLEEP_SYSTEMTASK', `
N'WAITFOR', `
N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', `
N'CHECKPOINT_QUEUE', `
N'REQUEST_FOR_DEADLOCK_SEARCH', `
N'XE_TIMER_EVENT', `
N'XE_DISPATCHER_JOIN', `
N'LOGMGR_QUEUE', `
N'FT_IFTS_SCHEDULER_IDLE_WAIT', `
N'BROKER_TASK_STOP', `
N'CLR_MANUAL_EVENT', `
N'CLR_AUTO_EVENT', `
N'DISPATCHER_QUEUE_SEMAPHORE', `
N'TRACEWRITE', `
N'XE_DISPATCHER_WAIT', `
N'BROKER_TO_FLUSH', `
N'BROKER_EVENTHANDLER', `
N'FT_IFTSHC_MUTEX', `
N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', `
N'DIRTY_PAGE_POLL', `
N'SP_SERVER_DIAGNOSTICS_SLEEP') `
ORDER BY wait_time_ms-signal_wait_time_ms DESC"

$lasthourquery = "SELECT '$($ClientName)' AS Client, ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),'AzureSQL') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
AVG(avg_cpu_percent) AS 'Average CPU Percent', `
MAX(avg_cpu_percent) AS 'Maximum CPU Percent', `
AVG(avg_data_io_percent) AS 'Average Data IO Percent', `
MAX(avg_data_io_percent) AS 'Maximum Data IO Percent', `
AVG(avg_log_write_percent) AS 'Average Log IO Percent', `
MAX(avg_log_write_percent) AS 'Maximum Log IO Percent', `
AVG(avg_memory_usage_percent) AS 'Average Memory Percent', `
MAX(avg_memory_usage_percent) AS 'Maximum Memory Percent' `
FROM sys.dm_db_resource_stats"

$blockingquery = "SELECT '$($ClientName)' AS Client, ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),'AzureSQL') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
substring(st.text,1,200) AS [SQL Text], c.connection_id, w.session_id, `
  w.wait_duration_ms, w.wait_type, substring(w.resource_address,1,200), `
  w.blocking_session_id, w.resource_description, c.client_net_address, c.connect_time `
FROM sys.dm_os_waiting_tasks AS w `
INNER JOIN sys.dm_exec_connections AS c ON w.session_id = c.session_id ` 
CROSS APPLY (SELECT * FROM sys.dm_exec_sql_text(c.most_recent_sql_handle)) AS st `
              WHERE w.session_id > 50 AND w.wait_duration_ms > 0 `
ORDER BY c.connection_id, w.session_id"


$sizequery = "SELECT '$($ClientName)' AS Client, ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),'AzureSQL') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], ` 
file_id, type_desc, `
CAST((CAST(FILEPROPERTY(name, 'SpaceUsed') AS decimal(19,4)) * 8 / 1024.) AS decimal(10,0)) AS space_used_mb, `
CAST((CAST(size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0 AS decimal(19,4))) AS decimal(10,0)) AS space_unused_mb, `
CAST((CAST(size AS decimal(19,4)) * 8 / 1024.) AS decimal(10,0)) AS space_allocated_mb, `
CAST((CAST(max_size AS decimal(19,4)) * 8 / 1024.) AS decimal(10,0)) AS max_size_mb `
FROM sys.database_files"


# CPU
$outpath = $Path + $ClientName + '_cpu_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $cpuquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $cpuquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}
 
# Expensive queries
$outpath = $Path + $ClientName + '_queries_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $expqueries -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $expqueries -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Connections per DB
$outpath = $Path + $ClientName + '_connections_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $connquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $connquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Waits
#$outpath = $Path + $ClientName + '_waits_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $waitsquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $waitsquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Last Hour Performance
$outpath = $Path + $ClientName + '_lasthour_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $lasthourquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $lasthourquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Blocking session
$outpath = $Path + $ClientName + '_blocking_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $blockingquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $blockingquery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# File size
$outpath = $Path + $ClientName + '_sizing_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $sizequery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $sizequery -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}


$IsAppend = 1

}

Write-Host "Completed"
