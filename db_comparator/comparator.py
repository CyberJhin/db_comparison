import pandas as pd
from tabulate import tabulate

class TableComparator:
    def __init__(self, db, table1, table2, keys):
        self.db = db
        self.table1 = table1
        self.table2 = table2
        self.keys = keys

    def _generate_join_condition(self):
        return ' AND '.join([f't1.{key} = t2.{key}' for key in self.keys])

    def count_common_rows(self):
        join_condition = self._generate_join_condition()
        query = f"""
        SELECT COUNT(*) AS common_count
        FROM {self.table1} t1
        INNER JOIN {self.table2} t2
        ON {join_condition}
        """
        df = self.db.execute_query(query)
        return df['common_count'][0]

    def count_only_in_first(self):
        join_condition = self._generate_join_condition()
        query = f"""
        SELECT COUNT(*) AS only_in_first_count
        FROM {self.table1} t1
        LEFT JOIN {self.table2} t2
        ON {join_condition}
        WHERE t2.{self.keys[0]} IS NULL
        """
        df = self.db.execute_query(query)
        return df['only_in_first_count'][0]

    def count_only_in_second(self):
        join_condition = self._generate_join_condition()
        query = f"""
        SELECT COUNT(*) AS only_in_second_count
        FROM {self.table2} t2
        LEFT JOIN {self.table1} t1
        ON {join_condition}
        WHERE t1.{self.keys[0]} IS NULL
        """
        df = self.db.execute_query(query)
        return df['only_in_second_count'][0]

    def get_columns(self, table):
        """Получить имена всех столбцов таблицы."""
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        """
        df = self.db.execute_query(query)
        return df['column_name'].tolist()

    def compare_rows(self):
        """Сравнить строки между двумя таблицами и вернуть различия."""
        columns1 = self.get_columns(self.table1)
        columns2 = self.get_columns(self.table2)

        join_condition = self._generate_join_condition()

        common_columns = list(set(columns1) & set(columns2))

        column_comparison = ' OR '.join([
            f"t1.{col} IS DISTINCT FROM t2.{col}"
            for col in common_columns
        ])

        query = f"""
        SELECT
            t1.*, t2.*
        FROM {self.table1} t1
        INNER JOIN {self.table2} t2
        ON {join_condition}
        WHERE {column_comparison}
        """

        df = self.db.execute_query(query)
        return df

    def get_null_analysis(self):
        tables = [self.table1, self.table2]
        results = []
        for table in tables:
            columns = self.get_columns(table)
            if not columns:
                continue
            null_query = f"""
            SELECT {', '.join([f'SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls' for col in columns])},
                   {', '.join([f'COUNT({col}) AS {col}_count' for col in columns])}
            FROM {table}
            """
            df = self.db.execute_query(null_query)
            for col in columns:
                null_count = df[f"{col}_nulls"][0]
                total_count = df[f"{col}_count"][0]
                percent_nulls = (null_count / (total_count + null_count) * 100) if (total_count + null_count) > 0 else 0
                results.append({
                    "table": table,
                    "column": col,
                    "nulls": null_count,
                    "percent_nulls": round(percent_nulls, 2)
                })
        return pd.DataFrame(results)

    def get_duplicates_analysis(self):
        tables = [self.table1, self.table2]
        results = []
        for table in tables:
            columns = self.get_columns(table)
            for col in columns:
                duplicate_query = f"""
                SELECT COUNT(*) - COUNT(DISTINCT {col}) AS duplicates
                FROM {table}
                """
                df = self.db.execute_query(duplicate_query)
                duplicates = df["duplicates"][0]
                results.append({
                    "table": table,
                    "column": col,
                    "duplicates": duplicates
                })
        return pd.DataFrame(results)

    def display_null_analysis(self, df):
        tables = df['table'].unique()
        for table in tables:
            print(f"--- Null Analysis for {table} ---")
            table_df = df[df['table'] == table][['column', 'nulls', 'percent_nulls']]
            total_nulls = table_df['nulls'].sum()
            total_row = pd.DataFrame([{
                'column': 'total',
                'nulls': total_nulls,
                'percent_nulls': 'NaN'
            }])
            table_df = pd.concat([table_df, total_row], ignore_index=True)
            print(tabulate(table_df, headers='keys', tablefmt='psql'))
            print()

    def display_duplicates_analysis(self, df):
        tables = df['table'].unique()
        for table in tables:
            print(f"--- Duplicates Analysis for {table} ---")
            table_df = df[df['table'] == table][['column', 'duplicates']]
            print(tabulate(table_df, headers='keys', tablefmt='psql'))
            print()

    def display_comparison(self):
        """Display the row comparison results."""
        df = self.compare_rows()
        print("--- Row Comparison ---")
        print(self.format_dataframe(df))
        print()

    @staticmethod
    def format_dataframe(df):
        """Format DataFrame as a string with tabulate."""
        return tabulate(df, headers='keys', tablefmt='psql')