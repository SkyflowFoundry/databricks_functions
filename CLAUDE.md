# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains Databricks User-Defined Functions (UDFs) that integrate with Skyflow's Data Privacy Vault for data tokenization and de-identification. The functions are designed to be registered in Unity Catalog and shared across Databricks accounts.

## Architecture

### Function Types
- **deidentify_string**: SQL function with embedded Python that detects and replaces sensitive entities in unstructured text
- **tokenize_from_csv**: Python UDF that reads CSV data, inserts sensitive values into Skyflow vault, and returns tokens

### Key Components
- SQL function definitions with embedded Python code for Databricks Unity Catalog
- Python UDFs using Skyflow SDK for vault operations
- Databricks secrets integration for secure credential management
- Batch processing capabilities for CSV tokenization (25 records per batch)

## Configuration Requirements

### Skyflow Configuration
Functions require these Skyflow parameters:
- `vault_id`: Skyflow vault identifier
- `vault_url`: Skyflow vault URL prefix
- `account_id`: Skyflow account identifier
- Service account credentials file (`credentials.json`)

### Databricks Secrets
Store Skyflow credentials in Databricks secret scopes:
- API keys: Use `dbutils.secrets.get()` to retrieve
- Configuration JSON: Store as stringified JSON in secrets
- Default scope name pattern: `sky-agentic-demo` or `demoscope`

## Function Installation

### Deidentify String
1. Copy SQL from `deidentify_string/deidentify_string.sql`
2. Replace placeholder values for `vault_id` and `vault_url` 
3. Execute in Databricks Query to register function
4. Function signature: `deidentify_string(input_text STRING, sky_api_key STRING)`

### Tokenize from CSV
1. Upload credentials.json to Databricks volume
2. Create configuration JSON with Skyflow details
3. Store configuration as Databricks secret
4. Copy Python code from `tokenize_from_csv/tokenize_from_csv.py` into notebook
5. Update `CONFIG_SECRET_KEY_NAME` and `DATABRICKS_SECRETS_SCOPE_NAME`
6. Run notebook to register UDF
7. Function signature: `tokenizeCSV(csv_path, skyflow_table, column_map)`

## Dependencies

### Python Packages (for tokenize_from_csv)
- pandas==2.2.2
- pyspark==3.5.1 
- skyflow==1.15.1

### Databricks CLI Setup
Required for secret management:
```bash
brew tap databricks/tap
brew install databricks
databricks configure
```

## Testing and Usage

### Test Deidentify String
```python
sky_api_key = dbutils.secrets.get(scope="sky-agentic-demo", key="sky_api_key")
input_text = "Hi my name is Joseph McCarron and I live in Austin TX"
result_df = spark.sql(f"SELECT agentic.default.deidentify_string('{input_text}', '{sky_api_key}') AS deidentified_text")
```

### Test Tokenize CSV
```sql
SELECT tokenizeCSV("/path/to/file.csv", "persons", MAP("csv_col", "skyflow_col")) AS tokenized_data;
```

## Important Notes

- All code is provided as sample code without warranty
- Functions require active Skyflow vault with appropriate permissions
- Service accounts need insert and tokenization permissions
- Test and validate code before production deployment
- CSV tokenization processes data in batches of 25 records