#drop_collection_user_v2405.ps1

# Designate the client name
$ClientName = "TestClient"

# Designate the output file location
$Path = 'C:\Users\Public\Documents\'

#$TsExt = Get-Date -Format yyyyMMdd_HHmmss

# Import the target server data, gathering only enabled rows
$CSVData = Import-Csv -Path "C:\Users\Public\Documents\servers.csv" | ? { $_.enabled -eq "Y" }

# Process each enabled line of the CSV file
# Assigning the columns to respective variables
ForEach ($Row in $CSVData) { 

$SQLServer = $Row.server_ip
$usr = $Row.username	
$pwd = $Row.password
$db = "master"

$auth=@{}

# Drop user from each database
$DropUser="EXEC sp_MSforeachdb ' `
USE [?] `
IF ''?'' <> ''tempdb'' AND ''?'' <> ''model'' AND ''?'' <> ''distribution'' `
AND EXISTS (SELECT NAME from sys.database_principals where NAME = ''$usr'') `
DROP USER [$usr] `
'"

# Drop login 
$DropLogin="IF EXISTS (SELECT NAME from sys.server_principals where NAME = '$usr') `
DROP LOGIN [$usr]"

# Drop user
Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $DropUser -QueryTimeout 30 -TrustServerCertificate -Verbose

 
# Drop login
Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $DropLogin -QueryTimeout 30 -TrustServerCertificate -Verbose
 
}
