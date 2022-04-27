#Requires -Module Az.Accounts, Az.Storage
[CmdletBinding()]
param (
    # This param must be the name of a credential in the
    # Automation Account with the name & key for the 
    # username & password accordingly
    [Parameter(Mandatory)]
    [string]
    $storageAccount,

    [Parameter(Mandatory)]
    [string]
    $blobContainer,

    [Parameter()]
    [string]
    $blobSuffix = "*.bak",

    [Parameter()]
    [bool]
    $Simulate = $true,

    [Parameter()]
    [int]
    $dailyBackupRetention = 14,

    [Parameter()]
    [ValidateSet("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")]
    [string]
    $weeklyBackupDay = "Sunday",

    [Parameter()]
    [int]
    $weeklyBackupRetention = 0,

    [Parameter()]
    [ValidateRange(1,31)]
    [int]
    $monthlyBackupDate = 1,
 
    [Parameter()]
    [int]
    $monthlyBackupRetention = 0,

    [Parameter()]
    [ValidateSet("1","2","3","4","5","6","7","8","9","10","11","12")]
    [int]
    $yearlyBackupMonth = "1",

    [Parameter()]
    [int]
    $yearlyBackupRetention = 0

)

# Set the Storage Account context specified in the $storageAccount variable
function Set-StorageAccountContext {
    param (
       [Parameter(Mandatory)]
       [String]
       $storageAccount
    )
   
    $storageAccountCred = Get-AutomationPSCredential -Name $storageAccount
    $storageAccountName = $storageAccountCred.UserName
    $storageAccountKey  = $storageAccountCred.GetNetworkCredential().Password

    $Context = New-AzStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey

    return $Context
}

# Return earliest retention dates
function Get-BackupRetentionDates {
    $dateTimeNow = [datetime]::Now

    $dailyEarliestBackupDate   = $dateTimeNow.AddDays(- $dateTimeNow.DayOfWeek).AddDays(- $dailyBackupRetention).AddHours(- $dateTimeNow.Hour).AddMinutes(- $dateTimeNow.Minute)
    $weeklyEarliestBackupDate  = $dateTimeNow.AddDays(- $dateTimeNow.DayOfWeek).AddDays(- $weeklyBackupRetention * 7).AddHours(- $dateTimeNow.Hour).AddMinutes(- $dateTimeNow.Minute)
    $monthlyEarliestBackupDate = $dateTimeNow.AddDays(- $dateTimeNow.DayOfWeek).AddMonths(- $monthlyBackupRetention).AddHours(- $dateTimeNow.Hour).AddMinutes(- $dateTimeNow.Minute)
    $yearlyEarliestBackupDate   = $dateTimeNow.AddDays(- $dateTimeNow.DayOfWeek).AddYears(- $yearlyBackupRetention).AddHours(- $dateTimeNow.Hour).AddMinutes(- $dateTimeNow.Minute)

    return $dailyEarliestBackupDate, $weeklyEarliestBackupDate, $monthlyEarliestBackupDate, $yearlyEarliestBackupDate

}

# Generate an Array containing the blobs to remove/remain
function Get-OldBlobs {
    $blobsToDelete = @()
    $blobsRemaining = @()

    $batchSize = 1000
    $token = $null

    $storageContext = Set-StorageAccountContext -storageAccount $storageAccount

    $dailyEarliestBackupDate, $weeklyEarliestBackupDate, $monthlyEarliestBackupDate, $yearlyEarliestBackupDate = Get-BackupRetentionDates

    # Loop blobs in batches of 1000 using a continuation token
    do {
        $blobs = Get-AzStorageBlob -Context $storageContext -Container $blobContainer -Blob $blobSuffix -MaxCount $batchSize -ContinuationToken $token

        if ($blobs.Length -le 0) {
            break;
        }

        $token = $blobs[$blobs.count - 1].ContinuationToken
       
        $blobsRemaining += $blobs | Where-Object { ($_.LastModified -gt $dailyEarliestBackupDate) }
	    $blobsRemaining += $blobs | Where-Object { ($_.LastModified -lt $dailyEarliestBackupDate) -and ($_.LastModified -gt $weeklyEarliestBackupDate) -and ($_.LastModified.DayOfWeek -eq $weeklyBackupDay) }
	    $blobsRemaining += $blobs | Where-Object { ($_.LastModified -lt $dailyEarliestBackupDate) -and ($_.LastModified -gt $monthlyEarliestBackupDate) -and ($_.LastModified.Day -eq $monthlyBackupDate) }
        $blobsRemaining += $blobs | Where-Object { ($_.LastModified -lt $dailyEarliestBackupDate) -and ($_.LastModified -gt $yearlyEarliestBackupDate) -and ($_.LastModified.Day -eq $monthlyBackupDate) -and ($_.LastModified.Month -eq $yearlyBackupMonth) }

        $blobsRemaining = $blobsRemaining | Sort-Object -Property Name -Unique

        $blobsToDelete += $blobs | Where-Object { $_.Name -notin $blobsRemaining.Name }

    } while ($null -ne $token)

    return $blobsToDelete, $blobsRemaining
}

# Remove blobs if in LIVE mode
# Return blobs that would be removed if in SIMULATE mode
function Remove-OldBlobs {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [bool]
        $Simulate,
        [Parameter(Mandatory)]
        [object[]]
        $blobsToDelete,
        [Parameter(Mandatory)]
        [object[]]
        $blobsRemaining
    )

    if ($Simulate) {
        Write-Output "*** Running in SIMULATE mode. No actions will be taken. ***"
        Write-Output "*** The following blobs would be removed in live mode: ***"
        $blobsToDelete | Select-Object -Property Name, LastModified
        
        Write-Output "*** The following blobs would remain in live mode ***"
        $blobsRemaining | Select-Object -Property Name, LastModified

        Write-Output "*** Old blobs consume: $("{0:0} Gb in {1} objects" -f (($blobsToDelete | Measure-Object -Property Length -Sum).Sum / 1024 / 1024 / 1024), $blobsToDelete.Length) ***"
        Write-Output "*** Remaining blobs consume: $("{0:0} Gb in {1} objects" -f (($blobsRemaining | Measure-Object -Property Length -Sum).Sum / 1024 / 1024 / 1024), $blobsRemaining.Length) ***"
    } else {
        Write-Output "*** Running in LIVE mode. Old blobs will be removed. ***"

        Write-Output "*** Old blobs consume: $("{0:0} Gb in {1} objects" -f (($blobsToDelete | Measure-Object -Property Length -Sum).Sum / 1024 / 1024 / 1024), $blobsToDelete.Length) ***"

        foreach ($blob in $blobsToDelete) {
            if ($blob.ICloudBlob.Properties.LeaseStatus -eq 'Locked') {
                Write-Output "*** Removing the lease from ***" -f ($blob.Name)
                $blob.ICloudBlob.BreakLease()
            }
            $blob | Remove-AzStorageBlob -Force
        }
        Write-Output "*** Remaining blobs consume: $("{0:0} Gb in {1} objects" -f (($blobsRemaining | Measure-Object -Property Length -Sum).Sum / 1024 / 1024 / 1024), $blobsRemaining.Length) ***"
    }
}

$blobsToDelete, $blobsRemaining = Get-OldBlobs

# Check if there are any blobs to remove
if (!$blobsToDelete) {
    Write-Output "*** No backups to remove. No actions taken ***"
    Write-Output "*** Remaining blobs consume: $("{0:0} Gb in {1} objects" -f (($blobsRemaining | Measure-Object -Property Length -Sum).Sum / 1024 / 1024 / 1024), $blobsRemaining.Length) ***"
} else {
    Remove-OldBlobs -blobsToDelete $blobsToDelete -blobsRemaining $blobsRemaining -Simulate $Simulate
}