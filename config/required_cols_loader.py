import yaml

class RequiredColsLoader:
    def __init__(self, req_cols_path: str):
        with open(req_cols_path, 'r') as file:
            self.tables = yaml.safe_load(file)['tables']

    def get_table_names(self):
        return list(self.tables.keys())

    def get_required_cols(self, table_name: str):
        return self.tables.get(table_name, {}).get('required_columns', [])

    def get_data_types(self, table_name: str):
        return self.tables.get(table_name, {}).get('types', {})

    def get_foreign_keys(self, table_name: str):
        return self.tables.get(table_name, {}).get('foreign_keys', [])