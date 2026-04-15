import yaml

class SchemaLoader:
    def __init__(self, schemas_path: str):
        with open(schemas_path, 'r') as file:
            self.schemas = yaml.safe_load(file)['tables']

    @property
    def tables(self):
        """Property to access tables (alias for schemas for compatibility)."""
        return self.schemas

    def get_table_names(self):
        return list(self.schemas.keys())

    def get_required_cols(self, table_name: str):
        return self.schemas.get(table_name, {}).get('required_columns', [])

    def get_data_types(self, table_name: str):
        return self.schemas.get(table_name, {}).get('types', {})

    def get_primary_key(self, table_name: str):
        return self.schemas.get(table_name, {}).get('primary_key', {})

    def get_fact_table_names(self):
        fact_tables = []
        for table_name, schema in self.schemas.items():
            pk_col = schema.get('primary_key')
            if pk_col:
                pk_type = schema.get('types', {}).get(pk_col)
                if pk_type == 'string':
                    fact_tables.append(table_name)
        return fact_tables

        # Fix: Handle both 'primary_key' and 'primary_keys' in schema
        pk = self.schemas.get(table_name, {}).get('primary_key')
        if pk is None:
            pk = self.schemas.get(table_name, {}).get('primary_keys', [])
            if isinstance(pk, list) and len(pk) > 0:
                pk = pk[0]  # Return first primary key if list
        return pk

    def get_primary_keys(self, table_name: str):
        """Return primary keys as list (for composite keys)."""
        pk = self.schemas.get(table_name, {}).get('primary_keys')
        if pk is None:
            pk = self.schemas.get(table_name, {}).get('primary_key', [])
            if isinstance(pk, str):
                pk = [pk]
        return pk if isinstance(pk, list) else [pk] if pk else []

    def get_columns_meta(self, table_name):
        table = self.schemas.get(table_name, {})
        formats = table.get("formats") or {}

        return [
            {
                "column": col,
                "format": fmt
            }
            for col, fmt in formats.items()
        ]

    def get_foreign_keys(self, table_name: str):
        """Get foreign keys for a table."""
        return self.schemas.get(table_name, {}).get('foreign_keys', [])

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in schema."""
        return table_name in self.schemas