#create_collection_user_v2405.ps1

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

# Create login 
$CreateLogin = "CREATE LOGIN [$usr] WITH PASSWORD=N'$pwd', DEFAULT_DATABASE=[master], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF"

# Create user
$CreateUser="EXEC sp_MSforeachdb ' `
USE [?] `
IF ''?'' <> ''tempdb'' AND ''?'' <> ''model'' AND ''?'' <> ''distribution'' `
CREATE USER [$usr] FOR LOGIN [$usr] `
'"

# Alter User
$AlterUser = "EXEC sp_MSforeachdb ' `
USE [?] `
IF ''?'' <> ''tempdb'' AND ''?'' <> ''model'' AND ''?'' <> ''distribution'' `
ALTER ROLE [db_datareader] add member [$usr] `
'"

# Add views
$AddView = "GRANT VIEW SERVER STATE, VIEW ANY DEFINITION TO [$usr]"

# Create login
Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $CreateLogin -QueryTimeout 30 -TrustServerCertificate

# Create user
Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $CreateUser -QueryTimeout 30 -TrustServerCertificate

# Alter user
Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $AlterUser -QueryTimeout 30 -TrustServerCertificate

# Add views
Invoke-Sqlcmd -ServerInstance $SQLServer -Database $db @Auth -Query $AddView -QueryTimeout 30 -TrustServerCertificate

}
