import os
import pandas as pd
from db_connection import PostgresDB

def generate_insert_statements(df: pd.DataFrame, table_name: str) -> str:
    """
    Generates a string of INSERT statements from a Pandas DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data to be inserted.
        table_name (str): Table name to insert into.

    Returns:
        str: A string of INSERT statements.
    """
    insert_statements = []
    for index, row in df.iterrows():
        values = [f"'{value}'" if isinstance(value, str) else str(value) for value in row]
        insert_stmt = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(values)});\n"
        insert_statements.append(insert_stmt)
    return ''.join(insert_statements)

def write_dml_file(insert_statements: str, output_path: str) -> None:
    """
    Writes the INSERT statements to a file.

    Args:
        insert_statements (str): A string of INSERT statements.
        output_path (str): Path to the file to write to.

    Returns:
        None
    """
    with open(output_path, 'w') as f:
        f.write(insert_statements)

def generate_dml_file(db: PostgresDB, table_name: str, output_dir: str, output_file: str) -> None:
    """
    Generates a DML file to populate a table from a PostgreSQL database.

    Args:
        db (PostgresDB): Database connection object.
        table_name (str): Table name to read from.
        output_dir (str): Absolute path to the directory for writing the DML file.
        output_file (str): File name for writing the DML commands.

    Returns:
        None
    """
    try:
        # Establish a connection to the database
        db.connect()

        # Retrieve all rows from the table
        query = f"SELECT * FROM {table_name}"
        df = db.execute_query(query)

        # Create the output file path
        output_path = os.path.join(output_dir, output_file)

        # Create the directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Generate the INSERT statements
        insert_statements = generate_insert_statements(df, table_name)

        # Write the INSERT statements to the file
        write_dml_file(insert_statements, output_path)

    finally:
        # Close the database connection
        db.disconnect()

# Example usage
db = PostgresDB()
output_dir = r'E:\LeetCode\db_comparator\db_comparator\Func_dict'
generate_dml_file(db, 'public.new_table1', output_dir, 'clients_dml1.sql')