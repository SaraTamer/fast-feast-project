YAML_TO_DUCKDB = {
    "int":       "BIGINT",
    "float":     "DOUBLE",
    "string":    "VARCHAR",
    "date":      "DATE",
    "timestamp": "TIMESTAMP",
    "boolean":   "BOOLEAN",
}

# DuckDB can infer many subtypes — map them all back to your YAML vocabulary
DUCKDB_TO_YAML = {
    "TINYINT":    "int",
    "SMALLINT":   "int",
    "INTEGER":    "int",
    "BIGINT":     "int",
    "HUGEINT":    "int",
    "UTINYINT":   "int",
    "USMALLINT":  "int",
    "UINTEGER":   "int",
    "UBIGINT":    "int",
    "FLOAT":      "float",
    "DOUBLE":     "float",
    "DECIMAL":    "float",
    "VARCHAR":    "string",
    "JSON":       "string",
    "UUID":       "string",
    "DATE":       "date",
    "TIMESTAMP":  "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp",
    "BOOLEAN":    "boolean",
}

def duckdb_type_to_yaml(duckdb_type: str) -> str:
    """
    Map a DuckDB type string to your YAML type vocabulary.
    Handles parameterized types like DECIMAL(18,3) → 'float'
    """
    upper = str(duckdb_type).upper().strip()

    # Exact match
    if upper in DUCKDB_TO_YAML:
        return DUCKDB_TO_YAML[upper]

    # Parameterized: DECIMAL(18,3) → DECIMAL
    base = upper.split("(")[0].strip()
    if base in DUCKDB_TO_YAML:
        return DUCKDB_TO_YAML[base]

    # Fallback
    if "VARCHAR" in upper or "TEXT" in upper:
        return "string"

    return "unknown"


def yaml_types_to_duckdb(yaml_types: dict) -> dict:
    """
    Convert YAML type definitions to DuckDB type definitions.
    {'customer_id': 'int', 'full_name': 'string'}
    → {'customer_id': 'BIGINT', 'full_name': 'VARCHAR'}


    which means that yaml_type is the dataype of yaml {int, floar, string}
    """

    duckdb_types = {}
    for col, yaml_type in yaml_types.items():
        key = yaml_type.lower()
        if key not in YAML_TO_DUCKDB:
            raise ValueError(
                f"Unknown YAML type '{yaml_type}' for column '{col}'. "
                f"Valid types: {list(YAML_TO_DUCKDB.keys())}"
            )
        duckdb_types[col] = YAML_TO_DUCKDB[key]
    return duckdb_types