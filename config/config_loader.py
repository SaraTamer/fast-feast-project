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
