import random
import string
import pandas as pd

import yaml

from local_local import ProcessData


class CreateSampleData(ProcessData):
    def __init__(self, config_dict) -> None:
        super().__init__(config_dict)
        self.config_dict = config_dict
        self.data_folder = self.configure_from_dict("persistence_file_path")
        self.create_folder()
        self.batch_size = self.configure_from_dict('batch_size')
        self.sample_data = self.configure_from_dict("sample_data")
        self.generate_tfv_file()

    def generate_tfv_file(self):
        for file_name, nos_rows in self.sample_data.items():
            print("FILE NAME", file_name)
            data = self.generate_sample_data(nos_rows)
            df = pd.DataFrame(data)
            df.to_csv(f'{self.data_folder}/{file_name}',
                      sep='\t', compression='gzip', index=False)

    def generate_random_string(self, length):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def generate_fingerprint(self):
        # Generate a list of 2048 random numbers between 0 and 1000
        fingerprint_values = [random.randint(0, 1000) for _ in range(2048)]

        # Convert the list to a comma-separated string
        fingerprint_string = ','.join(map(str, fingerprint_values))

        return fingerprint_string

    def generate_sample_data(self, num_rows):
        lib_id = self.generate_random_string(4)
        data = []
        for _ in range(num_rows):
            row = {
                'ID': self.generate_random_string(10),
                'Library_ID': lib_id,
                'Sub_ID_1': self.generate_random_string(3),
                'Sub_ID_2': self.generate_random_string(3),
                'Sub_ID_3': self.generate_random_string(3),
                'MW': random.uniform(0, 100),
                'LogP': random.uniform(0, 10),
                'FP1': self.generate_fingerprint(),
                'FP2': self.generate_fingerprint(),
                'FP3': self.generate_fingerprint(),
                'FP3': self.generate_fingerprint(),
                'FP4': self.generate_fingerprint() if random.choice([True, False]) else None,
                'FP5': self.generate_fingerprint() if random.choice([True, False]) else None,
            }
            data.append(row)
        return data


def load_yaml(yaml_file_path: str) -> dict:
    """
    Loads the contents of the yaml file into a dictionary for use by a job
    :param yaml_file_path: a path to a yaml file with configuration information
    """
    data = None

    try:
        with open(yaml_file_path, "r", encoding="utf8") as file:
            data = yaml.safe_load(file)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"No config file found at {yaml_file_path}") from exc

    if data is None:
        raise ValueError("Config File Contains Nothing or Does Not Exist")
    return data


if __name__ == "__main__":
    CONFIG_FILE_PATH = "./configs/info_config.yaml"
    config_dict = load_yaml(CONFIG_FILE_PATH)
    CreateSampleData(config_dict)
