import yaml

class SchemaLoader:
    def __init__(self, schemas_path: str):
        with open(schemas_path, 'r') as file:
            self.schemas = yaml.safe_load(file)['tables']

    def get_table_names(self):
        return list(self.schemas.keys())

    def get_required_cols(self, table_name: str):
        return self.schemas.get(table_name, {}).get('required_columns', [])

    def get_data_types(self, table_name: str):
        return self.schemas.get(table_name, {}).get('types', {})

    def get_primary_key(self, table_name: str):
        return self.schemas.get(table_name, {}).get('primary_key', {})
    
    def get_columns_meta(self, table_name):
        table   = self.schemas.get(table_name, {})
        formats = table.get("formats") or {}

        return [
            {
                "column": col,
                "format": fmt
            }
            for col, fmt in formats.items()
        ]
