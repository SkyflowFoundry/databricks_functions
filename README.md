# Databricks Functions (UDFs) from Skyflow

This repository contains a collection of example functions that can be used in Databricks to integrate the Skyflow Data Privacy vault for data security and privacy. 
Register these functions in Unity Catalog to share them throughout your Databricks account.

All code contained in this repo is offered as sample code without warranty.

## Functions

### Tokenize from CSV

This function takes source data from a CSV file and tokenizes it by inserting sensitive values into a Skyflow Privacy Vault and getting non-sensitive tokens back.

### Deidentify String(s)

This function takes any string, detects sensitive enitities in the string, and replaces sensitive data with non-sensitive tokens.