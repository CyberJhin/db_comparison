from db_comparator.db_connection import PostgresDB
from db_comparator.comparator import TableComparator
from tabulate import tabulate
def main():
    db = PostgresDB()
    db.connect()

    try:
        table1 = "employee_data"
        table2 = "employee_data1"
        keys = ["emp_code"]

        comparator = TableComparator(db, table1, table2, keys)

        nulls_df = comparator.get_null_analysis()
        comparator.display_null_analysis(nulls_df)

        duplicates_df = comparator.get_duplicates_analysis()
        comparator.display_duplicates_analysis(duplicates_df)

        common_count = comparator.count_common_rows()
        print(f"Common rows count: {common_count}")

        only_in_first_count = comparator.count_only_in_first()
        print(f"Rows only in the first table count: {only_in_first_count}")

        only_in_second_count = comparator.count_only_in_second()
        print(f"Rows only in the second table count: {only_in_second_count}")

        # Сравнение строк
        comparator.display_comparison()

    finally:
        db.disconnect()

if __name__ == "__main__":
    main()
