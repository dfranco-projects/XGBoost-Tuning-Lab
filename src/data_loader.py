import os
import json
import pandas as pd

class DataLoader:
    def __init__(self, metadata_file_path):
        '''
        Initialize the DataLoader with the path to the metadata file.
        
        Args:
            metadata_file_path (str): Path to the metadata JSON file.
        '''
        self.metadata_file_path = metadata_file_path
        self.metadata = self.load_metadata()

    def load_metadata(self) -> dict:
        '''
        Load metadata from the given JSON file.

        Returns:
            dict: Loaded metadata.

        Raises:
            Exception: If there is an error loading the metadata.
        '''
        try:
            with open(self.metadata_file_path, 'r') as metadata_file:
                metadata = json.load(metadata_file)
            return metadata
        except Exception as e:
            raise Exception(f'Error loading metadata: {e}')

    def load_data(self) -> list | pd.DataFrame:
        '''
        Load data based on the metadata instructions.

        Returns:
            list: List of loaded DataFrames if multiple files are provided.
            pd.DataFrame: A single DataFrame if only one file is provided.

        Raises:
            Exception: If any error occurs while loading the data.
        '''
        # Get the number of files for flow control
        if len(self.metadata) == 1:
            print(f'Loading file...\n\n')
            singular_file = True
        else:
            print(f'Loading files...\n\n')
            singular_file = False
            dfs = []

        # Iterate over metadata and load the data
        for entry in self.metadata:
            try:
                # Extract file name, file path without extesion, and import instructions
                file_name = entry.get('file_name')
                file_name_no_ext = os.path.splitext(file_name)[0]
                import_function = entry['import_instructions']['function']
                import_args = entry['import_instructions']['arguments']

                # Dynamically call the pandas function
                df = eval(import_function)(**import_args)

                if singular_file:
                    print(f'Loaded \033[1m{file_name_no_ext}\033[0m with {len(df)} rows and {len(df.columns)} features.\n')
                    display(df.head().style)
                else:
                    print(f'Loaded \033[1m{file_name_no_ext}\033[0m with {len(df)} rows and {len(df.columns)} features.\n')
                    display(df.head())
                    dfs.append(df)

            except Exception as e:
                raise Exception(f'Failed to load data from {file_name}: {e}')

        return df if singular_file else dfs