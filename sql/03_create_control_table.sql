-- Metadata table used by Azure Data Factory to determine which tables should be processed.

-- Control Table
CREATE TABLE dbo.ControlTable
(
    Id INT IDENTITY(1,1) PRIMARY KEY,
    SchemaName VARCHAR(100) NOT NULL,
    TableName VARCHAR(100) NOT NULL,
    IsActive BIT NOT NULL DEFAULT 1
);