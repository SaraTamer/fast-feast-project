# FastFeast

This repository contains the data pipeline architecture for FastFeast.

## Directory Structure

*   **`config/`**: Configuration loaders (YAML, credentials, paths).
*   **`core/`**: Pipeline-wide utilities (Audit Logger, Alerting).
*   **`db/`**: State management (Quarantine, Metadata rules, Temp logic, DWH loader).
*   **`watchers/`**: Event triggers (Batch and Stream).
*   **`ingestion/`**: Abstract factory for reading JSON/CSV.
*   **`processing/`**: The core, Schema Validators, Quality Checkers, and Transformations (PII Masking, SCD Type 2).
*   **`sql/`**: Pure SQL scripts executed by the processing layer (DDL, Quality Checks, Transformations).
*   **`tests/`**: Unit/Integration tests.
*   **`data/`**: Local storage for batch/stream files and the local database.

## Architecture

1.  **Watchers** detect files in `data/input/`.
2.  **Ingestion Factory** parses JSON or CSV into DataFrames.
3.  **SchemaValidator** checks required columns and formats against the `MetadataDB`.
4.  **QualityCheck** evaluates duplicates and orphans, pushing bad records to `QurantineDB`.
5.  **Transformations** cleanses valid data and executes SQL logic (`sql/transformations/`).
6.  **DWHLoader** pushes the final dimensional models.