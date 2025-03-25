import duckdb
from pathlib import Path
from typing import List, Dict, Any
from loguru import logger

class DatabaseManager:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.conn = duckdb.connect(database=':memory:')
        self._load_parquet_files()

    def _load_parquet_files(self):
        """Load all parquet files into DuckDB"""
        try:
            # Create a view combining all survey responses
            parquet_files = list(self.data_dir.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError("No parquet files found in data directory")

            # Create a view for each brand's data
            for file in parquet_files:
                view_name = file.stem
                # Use str() to get proper path string and replace backslashes with forward slashes
                file_path = str(file).replace('\\', '/')
                self.conn.execute(f"""
                    CREATE VIEW IF NOT EXISTS {view_name} AS 
                    SELECT * FROM read_parquet('{file_path}')
                """)

            # Create a combined view of all responses
            # Use str() and replace backslashes for the directory path too
            data_dir_path = str(self.data_dir).replace('\\', '/')
            self.conn.execute(f"""
                CREATE VIEW IF NOT EXISTS all_responses AS 
                SELECT * FROM read_parquet('{data_dir_path}/*.parquet')
            """)

            logger.info(f"Loaded {len(parquet_files)} parquet files into DuckDB")
        except Exception as e:
            logger.error(f"Error loading parquet files: {str(e)}")
            raise

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as a list of dictionaries"""
        try:
            result = self.conn.execute(query).fetchdf()
            return result.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}\nQuery: {query}")
            raise

    def get_schema_info(self) -> dict:
        """Get information about available tables/views and their structure"""
        views = self.conn.execute("SHOW TABLES").fetchdf()
        schema_info = {}
        
        for view_name in views['name']:
            columns = self.conn.execute(f"DESCRIBE {view_name}").fetchdf()
            schema_info[view_name] = columns.to_dict(orient='records')
        
        return schema_info

    def close(self):
        """Close the database connection"""
        self.conn.close()
