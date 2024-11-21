import os
import json
import zipfile
import warnings
import pandas as pd
from typing import Union, List
from abc import ABC, abstractmethod

# Define tabular data ingestor interface instance
class TabularDataIngestor(ABC):
    @abstractmethod
    def ingest(self, file_path: str) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        '''
        Abstract method to ingest tabular data for multiple file formats in a Factory Design Pattern.

        Args:
            file_path (str): Path to the file to be ingested.

        Returns:
            Union[pd.DataFrame, List[pd.DataFrame]]:
                - pd.DataFrame: If a single dataset is ingested.
                - List[pd.DataFrame]: If multiple datasets are ingested (e.g., files with different structures).
        '''
        pass

    def save_metadata(self, metadata: dict, output_dir: str = None) -> None:
        '''
        Saves metadata to a JSON file.

        Args:
            metadata (dict): Metadata dictionary.
            file_path (str): Path of the original data file.
            output_dir (str, optional): Directory where metadata will be saved.
                                        If None, defaults to the `metadata` folder.
        '''
        if output_dir is None:
            # If None, metadata JSON is 
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 2 levels up from `src`
            metadata_dir = os.path.join(project_root, 'metadata')
        else:
            metadata_dir = output_dir

        # Ensure the directory exists
        os.makedirs(metadata_dir, exist_ok=True)

        # Metadata file path
        metadata_path = os.path.join(metadata_dir, 'metadata.json')

        # Save JSON file
        try:
            with open(metadata_path, 'w') as metadata_file:
                json.dump(metadata, metadata_file, indent=4)
            print(f'Metadata saved at {metadata_path}')
        except Exception as e:
            warnings.warn(f'Failed to save metadata: {e}')

# Define a specific data ingestor for zip ingestion
class ZipDataIngestor(TabularDataIngestor):
    def ingest(self, file_path: str, output_dir: str = None) -> list:
        '''
        Extracts .zip files and returns their content as a list of pandas DataFrames.
        Also generates metadata to help users import the data automatically.

        Args:
            file_path (str): Path to the .zip file to be extracted and ingested.
            output_dir (str, optional): Directory where extracted files will be saved.
                                        If None, files are extracted to a default temporary folder.

        Returns:
            list: A list of pandas DataFrames, one for each tabular file extracted from the .zip.
        '''
        # Ensure the file is a .zip
        if not file_path.endswith('.zip'):
            raise ValueError('The provided file is not a .zip file.')

        # Define directory for extraction if none is given, creates a temporary directory
        temp_dir = tempfile.mkdtemp() if output_dir is None else output_dir

        # Extract .zip file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # List extracted files
        extracted_files = os.listdir(temp_dir)
        if not extracted_files:
            raise FileNotFoundError('No files found within the .zip file.')

        # Supported formats
        supported_formats = {'.csv', '.xlsx', '.parquet', '.json', '.txt'}
        dataframes = []
        metadata = []

        for file_name in extracted_files:
            file_path = os.path.join(temp_dir, file_name)
            file_extension = os.path.splitext(file_name)[1].lower()

            if file_extension not in supported_formats:
                warnings.warn(f'Unsupported file type: {file_name}. Skipping.')
                continue

            try:
                # Handle different file types
                if file_extension == '.csv':
                    df = pd.read_csv(file_path)
                    import_instructions = {
                        'function': 'pd.read_csv',
                        'arguments': {'filepath_or_buffer': file_path}
                    }

                elif file_extension == '.xlsx':
                    df = pd.read_excel(file_path)
                    import_instructions = {
                        'function': 'pd.read_excel',
                        'arguments': {'io': file_path}
                    }

                elif file_extension == '.parquet':
                    df = pd.read_parquet(file_path)
                    import_instructions = {
                        'function': 'pd.read_parquet',
                        'arguments': {'path': file_path}
                    }

                elif file_extension == '.json':
                    df = pd.read_json(file_path)
                    import_instructions = {
                        'function': 'pd.read_json',
                        'arguments': {'path_or_buf': file_path}
                    }

                elif file_extension == '.txt':
                    df = pd.read_csv(file_path, delimiter='\t')
                    import_instructions = {
                        'function': 'pd.read_csv',
                        'arguments': {'filepath_or_buffer': file_path, 'delimiter': '\t'}
                    }

                dataframes.append(df)

                # Add metadata for the file
                metadata.append({
                    'file_name': file_name,
                    'file_type': file_extension,
                    'columns': df.columns.tolist(),
                    'row_count': len(df),
                    'import_instructions': import_instructions
                })

            except Exception as e:
                warnings.warn(f'Failed to process {file_name}: {e}')

        # Save metadata to a JSON file
        self.save_metadata(metadata, output_dir)

        return dataframes

# Define a specific data ingestor for csv ingestion
class CsvDataIngestor(TabularDataIngestor):
    def ingest(self, file_path: str, output_dir: str = None) -> pd.DataFrame:
        '''
        Reads a single CSV file and returns its content as a pandas DataFrame.
        Also generates metadata to help users import the data automatically.

        Args:
            file_path (str): Path to the CSV file to be ingested.
            output_dir (str, optional): Directory where metadata will be saved.
                                        If None, defaults to the `metadata` folder.

        Returns:
            pd.DataFrame: Data from the CSV file.
        '''
        # Ensure the file is a CSV
        if not file_path.endswith('.csv'):
            raise ValueError('The provided file is not a CSV file.')

        # Ensure the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f'The file {file_path} does not exist.')

        # Read the CSV file
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            raise ValueError(f'Failed to read the CSV file: {e}')

        # Generate metadata
        metadata = {
            'file_name': os.path.basename(file_path),
            'file_type': '.csv',
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'import_instructions': {
                'function': 'pd.read_csv',
                'arguments': {'filepath_or_buffer': file_path}
            }
        }

        # Save metadata as JSON
        self.save_metadata(metadata, output_dir)

        return df

# Define a specific data ingestor for excel ingestion
class ExcelDataIngestor(TabularDataIngestor):
    def ingest(self, file_path: str, output_dir: str = None) -> pd.DataFrame:
        '''
        Reads an Excel file and generates metadata.

        Args:
            file_path (str): Path to the Excel file.
            output_dir (str, optional): Directory to save metadata.
                                        If None, defaults to the `metadata` folder.

        Returns:
            pd.DataFrame: Data from the Excel file.
        '''
        if not file_path.endswith(('.xlsx', '.xls')):
            raise ValueError('The provided file is not an Excel file.')

        df = pd.read_excel(file_path)
        metadata = {
            'file_name': os.path.basename(file_path),
            'file_type': '.xlsx',
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'import_instructions': {
                'function': 'pd.read_excel',
                'arguments': {'io': file_path}
            }
        }

        # Save metadata as JSON
        self.save_metadata(metadata, output_dir)

        return df

# Define a specific data ingestor for parquet ingestion
class ParquetDataIngestor(TabularDataIngestor):
    def ingest(self, file_path: str, output_dir: str = None) -> pd.DataFrame:
        '''
        Reads a Parquet file and generates metadata.

        Args:
            file_path (str): Path to the Parquet file.
            output_dir (str, optional): Directory to save metadata.
                                        If None, defaults to the `metadata` folder.

        Returns:
            pd.DataFrame: Data from the Parquet file.
        '''
        if not file_path.endswith('.parquet'):
            raise ValueError('The provided file is not a Parquet file.')

        df = pd.read_parquet(file_path)
        metadata = {
            'file_name': os.path.basename(file_path),
            'file_type': '.parquet',
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'import_instructions': {
                'function': 'pd.read_parquet',
                'arguments': {'path': file_path}
            }
        }

        # Save metadata as JSON
        self.save_metadata(metadata, output_dir)

        return df

# Define a specific data ingestor for JSON ingestion
class JsonDataIngestor(TabularDataIngestor):
    def ingest(self, file_path: str, output_dir: str = None) -> pd.DataFrame:
        '''
        Reads a JSON file and generates metadata.

        Args:
            file_path (str): Path to the JSON file.
            output_dir (str, optional): Directory to save metadata.
                                        If None, defaults to the `metadata` folder.

        Returns:
            pd.DataFrame: Data from the JSON file.
        '''
        if not file_path.endswith('.json'):
            raise ValueError('The provided file is not a JSON file.')

        df = pd.read_json(file_path)
        metadata = {
            'file_name': os.path.basename(file_path),
            'file_type': '.json',
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'import_instructions': {
                'function': 'pd.read_json',
                'arguments': {'path_or_buf': file_path}
            }
        }

        # Save metadata as JSON
        self.save_metadata(metadata, output_dir)

        return df

# Define a specific data ingestor for txt ingestion
class TxtDataIngestor(TabularDataIngestor):
    def ingest(self, file_path: str, output_dir: str = None) -> pd.DataFrame:
        '''
        Reads a tab-delimited TXT file and generates metadata.

        Args:
            file_path (str): Path to the TXT file.
            output_dir (str, optional): Directory to save metadata.
                                        If None, defaults to the `metadata` folder.

        Returns:
            pd.DataFrame: Data from the TXT file.
        '''
        if not file_path.endswith('.txt'):
            raise ValueError('The provided file is not a TXT file.')

        df = pd.read_csv(file_path, delimiter='\t')
        metadata = {
            'file_name': os.path.basename(file_path),
            'file_type': '.txt',
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'import_instructions': {
                'function': 'pd.read_csv',
                'arguments': {'filepath_or_buffer': file_path, 'delimiter': '\t'}
            }
        }

        # Save metadata as JSON
        self.save_metadata(metadata, output_dir)

        return df

# Define a factory to implement Data Ingestors
class DataFactory:
    @staticmethod
    def get_tabular_ingestor(file_extension: str) -> TabularDataIngestor:
        '''
        Returns the appropriate tabular data ingestor based on the file extension.

        Args:
            file_extension (str): The extension of the data file (e.g., '.csv', '.xlsx').

        Returns:
            TabularDataIngestor: An instance of the corresponding data ingestor class for the given file extension.
        
        Raises:
            ValueError: If the provided file extension is currently not supported.
        '''
        if file_extension == '.csv':
            return CsvDataIngestor()  # Return the CSV ingestor class
        
        elif file_extension == '.xlsx':
            return ExcelDataIngestor()  # Return the excel ingestor class
        
        elif file_extension == '.json':
            return JsonDataIngestor()  # Return the JSON ingestor class
        
        elif file_extension == '.parquet':
            return ParquetDataIngestor()  # Return the parquet ingestor class
        
        elif file_extension == '.txt':
            return TxtDataIngestor()  # Return the txt ingestor class
        
        else:
            raise ValueError(f'Unsupported file extension: {file_extension}')

if __name__ == '__main__':

    # Specify data directory
    project_folder = os.getcwd()
    project_folder = os.path.dirname(current_folder)
    print(project_folder)
    # data_folder = os.path.join(project_folder, 'data')
    # file_path = os.path.join(data_folder, os.listdir(data_folder)[0])

    # # Get file extension
    # file_extension = os.path.splitext(file_path)[1]

    # # Get data ingestor
    # data_ingestor = DataFactory.get_tabular_ingestor(file_extension)

    # # Ingest data
    # df = data_ingestor.ingest(file_path)

    # # Display top 3 rows
    # print(df.head(3))