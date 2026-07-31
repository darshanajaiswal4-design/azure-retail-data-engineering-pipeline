# =====================================================
# Azure Retail Data Engineering Pipeline
# Configure ADLS Gen2 Connection
# =====================================================

storage_account="<storage-account>"
container="<container>"
tenant_id="<tenant-id>"
client_id="<client-id>"
client_secret="<client-secret>"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net","OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
