# Azure Storage Account Lifecycle Management

This Powershell script will be used to manage the lifecyle of blobs within a storage account, where the blob types are not supported by the native Azure Storage Account Lifecyle Management function.

This script should be deployed within an Azure Automation Account as a Powershell Runbook. This would runbook would be run on a schedule targetting a specific storage account & container.

## Usage

```Powershell
./Invoke-StorageAccountCleanup.ps1 -storageAccount <storage account name> -blobContainer <container name>
```