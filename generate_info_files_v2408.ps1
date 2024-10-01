#generate_info_files_v2407.ps1

# Clear append setting to clear existing files 
$IsAppend = 0

# Designate the client name
$ClientName = "TestClient"

# Designate the output file location
$Path = 'C:\Users\Public\Documents\info\'

$TsExt = Get-Date -Format yyyyMMdd_HHmmss

# Import the target server data, gathering only enabled rows
$CSVData = Import-Csv -Path "C:\Users\Public\Documents\servers.csv" | ? { $_.enabled -eq "Y" }

# Process each enabled line of the CSV file
# Assigning the columns to respective variables
ForEach ($Row in $CSVData) { 

$SQLServer = $Row.server_ip
$usr = $Row.username	
$pwd = $Row.password
$db = "master"

if($usr)
{
  $auth=@{UserName=$usr;Password=$pwd}
}
else
{
  $auth=@{}
}

# Gather node info 
$InfoNodeInfo = "SELECT '$($ClientName)' AS Client, SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
SERVERPROPERTY('productversion') AS ProductVersion, SERVERPROPERTY ('productlevel') AS ProductLevel, `
SERVERPROPERTY ('edition') AS Edition, `
DATEADD(ms,-sample_ms,GETDATE() ) AS StartTime, `
SERVERPROPERTY('LicenseType') AS LICENSE_TYPE, `
ISNULL(SERVERPROPERTY('NumLicenses'),0) AS NUM_LICENCES, `
case when SERVERPROPERTY('IsClustered') = 0 then 'NotClustered' else 'Clustered' end, `
case when SERVERPROPERTY('IsHadrEnabled') = 0 then 'NoAlwaysOn' `
when SERVERPROPERTY('IsHadrEnabled') = 1 then 'AlwaysOn' `
else SERVERPROPERTY('IsHadrEnabled') end `
FROM sys.dm_io_virtual_file_stats(1,1)"

# Gather database info 
$InfoDB = "SELECT '$($ClientName)' AS Client, SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [CurrentNodeName], ` 
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
substring(sdb.name,1,40) AS name,  substring(sdb.state_desc,1,20) AS STATE, `
substring(sdb.recovery_model_desc,1,20) AS RECOVERY_MODEL, `
COALESCE(CONVERT(VARCHAR(12), MAX(bus.backup_finish_date), 101),NULL) AS LastBackUpTime `
FROM sys.databases sdb `
LEFT OUTER JOIN msdb.dbo.backupset bus ON bus.database_name = sdb.name `
where sdb.Name <> 'tempdb' `
GROUP BY sdb.Name, sdb.state_desc, sdb.recovery_model_desc `
order by name"

# Gather database role info 
$InfoRoles = "WITH Roles_CTE(Role_Name, Username) `
AS `
( `
SELECT ` 
User_Name(sm.[groupuid]) as [Role_Name], `
user_name(sm.[memberuid]) as [Username] `
FROM [sys].[sysmembers] sm `
) `
SELECT '$($ClientName)' AS Client, `  
SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
Roles_CTE.Role_Name, `
[DatabaseUserName] = princ.[name], `
[UserType] = CASE princ.[type] `
WHEN 'S' THEN 'SQL User' `
WHEN 'U' THEN 'Windows User' `
WHEN 'G' THEN 'Windows Group' `
WHEN 'A' THEN 'Application Role' `
WHEN 'R' THEN 'Database Role' `
WHEN 'C' THEN 'User mapped to a certificate' `
WHEN 'K' THEN 'User mapped to an asymmetric key' `
END `
FROM `
sys.database_principals princ ` 
JOIN Roles_CTE on Username = princ.name `
where princ.type in ('S', 'U', 'G', 'A', 'R', 'C', 'K') `
ORDER BY princ.name"

# Gather database job info 
$InfoJobs = "SELECT '$($ClientName)' AS Client, `
SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
[JobName] = [jobs].[name] `
,[Owner] = SUSER_SNAME([jobs].[owner_sid]) `
,[Enabled] = CASE [jobs].[enabled] WHEN 1 THEN 'Yes' ELSE 'No' END `
,[Frequency] = `
CASE [schedule].[freq_subday_type] `
WHEN 1 THEN 'Occurs once at ' + `
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [schedule].[active_start_time]), 6), 5, 0, ':'), 3, 0, ':') `
WHEN 2 THEN 'Occurs every ' + `
CONVERT(VARCHAR, [schedule].[freq_subday_interval]) + ' Seconds(s) between ' + ` 
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [schedule].[active_start_time]), 6), 5, 0, ':'), 3, 0, ':') + ' and ' + `
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [schedule].[active_end_time]), 6), 5, 0, ':'), 3, 0, ':') `
WHEN 4 THEN 'Occurs every ' + `
CONVERT(VARCHAR, [schedule].[freq_subday_interval]) + ' Minute(s) between ' + `
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [schedule].[active_start_time]), 6), 5, 0, ':'), 3, 0, ':') + ' and ' + `
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [schedule].[active_end_time]), 6), 5, 0, ':'), 3, 0, ':') `
WHEN 8 THEN 'Occurs every ' + `
CONVERT(VARCHAR, [schedule].[freq_subday_interval]) + ' Hour(s) between ' + `
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [schedule].[active_start_time]), 6), 5, 0, ':'), 3, 0, ':') + ' and ' + `
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [schedule].[active_end_time]), 6), 5, 0, ':'), 3, 0, ':') `
ELSE '' `
END `
,[Next_Run_Date] = `
CASE [jobschedule].[next_run_date] `
WHEN 0 THEN CONVERT(DATETIME, '1900/1/1') `
ELSE CONVERT(DATETIME, CONVERT(CHAR(8), [jobschedule].[next_run_date], 112) + ' ' + `
STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(8), [jobschedule].[next_run_time]), 6), 5, 0, ':'), 3, 0, ':')) `
END `
FROM	 [msdb].[dbo].[sysjobs] AS [jobs] WITh(NOLOCK) ` 
LEFT OUTER JOIN [msdb].[dbo].[sysjobschedules] AS [jobschedule] WITh(NOLOCK) ` 
ON [jobs].[job_id] = [jobschedule].[job_id] `
LEFT OUTER JOIN [msdb].[dbo].[sysschedules] AS [schedule] WITh(NOLOCK) `
ON [jobschedule].[schedule_id] = [schedule].[schedule_id] `
INNER JOIN [msdb].[dbo].[syscategories] [categories] WITh(NOLOCK) `
ON [jobs].[category_id] = [categories].[category_id] `
LEFT OUTER JOIN `
(	SELECT	 [job_id], [AvgDuration] = (SUM((([run_duration] / 10000 * 3600) + `
(([run_duration] % 10000) / 100 * 60) + `
([run_duration] % 10000) % 100)) * 1.0) / COUNT([job_id]) `
FROM	 [msdb].[dbo].[sysjobhistory] WITh(NOLOCK) `
WHERE	 [step_id] = 0 `
GROUP BY [job_id] `
) AS [jobhistory] `
ON [jobhistory].[job_id] = [jobs].[job_id]"

# Gather fragmented index info 
$InfoFrag="EXEC sp_MSforeachdb ' `
IF ''?'' <> ''master'' AND ''?'' <> ''model'' AND ''?'' <> ''msdb'' AND ''?'' <> ''tempdb'' AND ''?'' <> ''distribution'' `
SELECT ''$($ClientName)'' AS Client, `
SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( ''InstanceName''),''Default'') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
''?'' AS DB_NAME, `
QUOTENAME(sysind.name) AS [index_name], `
indstat.index_type_desc, indstat.avg_fragmentation_in_percent `
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, ''LIMITED'') `
AS indstat `
INNER JOIN sys.indexes sysind ON indstat.object_id = sysind.object_id AND `
indstat.index_id = sysind.index_id `
where avg_fragmentation_in_percent >= 30 `
ORDER BY avg_fragmentation_in_percent DESC ` 
'"

# Gather last DBCC check info 
$InfoLastCheck = "EXEC sp_MSforeachdb ' `
IF ''?'' <> ''master'' AND ''?'' <> ''model'' AND ''?'' <> ''msdb'' AND ''?'' <> ''tempdb'' AND ''?'' <> ''distribution'' `
SELECT ''$($ClientName)'' AS Client, `
SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( ''InstanceName''),''Default'') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
''?'' AS DB_NAME, `
DATABASEPROPERTYEX (''?'',''LastGoodCheckDbTime'') AS LastCheck `
'"

# Gather OS info 
#$InfoOS = "SELECT '$($ClientName)' AS Client, `
#SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [CurrentNodeName], `
#ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
#host_distribution FROM sys.dm_os_host_info"
$InfoOS = "IF (SELECT left(convert(varchar(max),SERVERPROPERTY('productversion')),2)) >= 14 `
SELECT '$($ClientName)' AS Client, `
SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
host_distribution FROM sys.dm_os_host_info `
ELSE `
SELECT '$($ClientName)' AS Client, `
SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [CurrentNodeName], `
ISNULL(SERVERPROPERTY( 'InstanceName'),'Default') AS InstanceName, CURRENT_TIMESTAMP AS [Current Date Time], `
serverproperty('Edition')"


# Node Info
$outpath = $Path + $ClientName + '_nodeinfo_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoNodeInfo -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoNodeInfo -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}
 
# DB Info
$outpath = $Path + $ClientName + '_dbinfo_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoDB -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoDB -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Roles
$outpath = $Path + $ClientName + '_roles_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoRoles -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoRoles -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Jobs
$outpath = $Path + $ClientName + '_jobs_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoJobs -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoJobs -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Index fragmentation
$outpath = $Path + $ClientName + '_indfrag_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoFrag -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoFrag -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# Last CheckDB
$outpath = $Path + $ClientName + '_lastcheck_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoLastCheck -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoLastCheck -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}

# OS info
$outpath = $Path + $ClientName + '_osinfo_' + $TsExt + '.csv'
if($IsAppend -eq 0)
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoOS -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -NoTypeInformation
} else
{
  Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $InfoOS -QueryTimeout 30 -TrustServerCertificate | Export-CSV -Path $outpath -Append -NoTypeInformation
}


$IsAppend = 1
 
}
