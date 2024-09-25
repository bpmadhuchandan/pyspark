# List all mounts
mounts = dbutils.fs.mounts()

# Loop through the mounts and print the storage account names
for mount in mounts:
    print(f"Mount Point: {mount.mountPoint}")
    print(f"Source: {mount.source}")
    
    # Extract the storage account name from the source path
    if "abfss://" in mount.source:
        # Example source: abfss://<container-name>@<storage-account-name>.blob.core.windows.net/
        storage_account_name = mount.source.split('@')[1].split('.')[0]
        print(f"Storage Account Name: {storage_account_name}")


# dbutils.fs.mounts(): This function returns a list of all mounted file systems in your Databricks workspace.
# Parsing the Source: The source URL contains the storage account name, which can be extracted by splitting the string.
