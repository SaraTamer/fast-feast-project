import yaml

class Config:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as file:
            self.settings = yaml.safe_load(file)

    def load_the_yaml(self):
        return self.settings

    def stream_input_path(self):
        return self.settings['paths']['stream_input']
    def batch_input_path(self):
        return self.settings['paths']['batch_input']
    def master_data_path(self):
        return self.settings['paths']['master_data']
    
    def log_file_path(self):
        return self.settings['paths']['log_file']
    def err_file_path(self):
        return self.settings['paths']['error_log_file']
    
    def schemas_path(self):
        return self.settings['paths']['scheme_path']
    def orphans_wait_time(self):
        return self.settings['rules']['orphan_check_delay_seconds']
    def get_errors_table_name(self):
        return self.settings['snowflake_tables']['errors_table']
