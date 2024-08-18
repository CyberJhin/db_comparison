# Database Table Comparator and Function Dependency Analyzer

A Python project for comparing tables and analyzing function dependencies in a PostgreSQL database. This tool provides functionalities to analyze and compare two tables, including checking for common rows, differences, null values, and duplicates, as well as analyzing function dependencies within specified schemas.

## Features

- **Compare Tables**: Count rows that are common between two tables, only in the first table, and only in the second table.
- **Null Value Analysis**: Analyze and display the percentage of null values for each column in the tables.
- **Duplicate Analysis**: Identify and count duplicate values in each column of the tables.
- **Row Comparison**: Compare rows between two tables and display differences.
- **Function Dependency Analysis**: Identify and list all functions that are called within the body of other functions in specified PostgreSQL schemas. This analysis can be outputted to both Excel and TXT files.

## Prerequisites

- PostgreSQL database
- Python 3.6 or higher

## Installation

1. **Clone the repository**:

    ```bash
    git clone https://github.com/CyberJhin/db_comparator.git
    cd db_comparator
    ```

2. **Create a virtual environment** (optional but recommended):

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

4. **Set up environment variables**:

   Create a `.env` file in the root directory of the project with the following content:

    ```env
    DB_NAME=your_database_name
    DB_USER=your_database_user
    DB_PASSWORD=your_database_password
    DB_HOST=localhost
    DB_PORT=5432
    ```

## Usage

### Table Comparison

1. **Run the script**:

    ```bash
    python main.py
    ```

2. **Configure tables and keys**:

   In `main.py`, set the table names and keys you want to compare:

    ```python
    table1 = "employee_data"
    table2 = "employee_data1"
    keys = ["emp_code"]
    ```

   Modify these variables to fit your specific use case.

### Function Dependency Analysis

1. **Prepare the list of functions**:
   - Create an Excel (`.xlsx`) or text (`.txt`) file with the list of functions to analyze.

2. **Run the script**:

    ```bash
    python main.py
    ```

3. **Configure schemas and functions**:

   In `main.py`, set the schema names and specify the path to the file containing the list of functions:

    ```python
    schema_list = ['schema1', 'schema2']
    function_list_file = 'functions_to_analyze.xlsx'  # or 'functions_to_analyze.txt'
    ```

4. **Review the output**:
   - The analysis will generate two output files:
      - `function_dependencies.xlsx`: An Excel file with the dependencies listed.
      - `function_dependencies.txt`: A text file with a simple list of found dependencies.

## Code Overview

- `db_comparator/db_connection.py`: Contains the `PostgresDB` class for handling database connections and executing queries.
- `db_comparator/comparator.py`: Contains the `TableComparator` class for comparing tables and performing various analyses.
- `db_comparator/function_dependency_analyzer.py`: Contains the `FunctionDependencyAnalyzer` class for analyzing function dependencies.
- `main.py`: The main entry point for running the script. Configures the database connection and performs table comparisons and function dependency analysis.

## Contributing

1. **Fork the repository**.
2. **Create a new branch** (`git checkout -b feature/your-feature`).
3. **Make your changes**.
4. **Commit your changes** (`git commit -am 'Add new feature'`).
5. **Push to the branch** (`git push origin feature/your-feature`).
6. **Create a new Pull Request**.
