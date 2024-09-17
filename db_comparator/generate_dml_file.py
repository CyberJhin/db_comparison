from datetime import date, timedelta, time, datetime
import os
import psycopg2
from db_connection import PostgresDB
import jinja2

# Шаблон для вставки данных
TEMPLATE = """
INSERT INTO {{ table_name }} ({{ columns }})
VALUES {{ values }};
"""

# Запись данных в файл порциями
def write_dml_file_chunk(insert_statements: str, output_path: str, append: bool = True) -> None:
    mode = 'a' if append else 'w'
    with open(output_path, mode) as f:
        f.write(insert_statements)

def get_columns_from_cursor(cursor) -> str:
    if cursor.description is None:
        raise ValueError("Cursor description is None. Ensure the query returns results.")
    return ', '.join([desc[0] for desc in cursor.description])

def fetch_data(db: PostgresDB, query: str, batch_size: int = 10000):
    try:
        db.connect()
        with db.conn.cursor() as cursor:
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
    except (psycopg2.DatabaseError, ValueError) as e:
        print(f"Error during database operation: {e}")
        raise
    finally:
        db.disconnect()

# Функция для генерации вставок данных
def generate_insert_statements(rows, columns, table_name: str) -> str:
    buffer = []
    template = jinja2.Template(TEMPLATE)
    values = []

    for row in rows:
        row_values = []
        for value in row:
            if value is None:
                row_values.append('NULL')
            elif isinstance(value, str):
                row_values.append(f"'{value}'")
            elif isinstance(value, date):
                row_values.append(f"'{value.strftime('%Y-%m-%d')}'")
            elif isinstance(value, time):
                row_values.append(f"'{value.strftime('%H:%M:%S')}'")
            elif isinstance(value, datetime):
                row_values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif isinstance(value, timedelta):
                days = value.days
                hours, remainder = divmod(value.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                row_values.append(f"'{days} days {hours:02d}:{minutes:02d}:{seconds:02d}'")
            else:
                row_values.append(str(value))
        values.append(f"({', '.join(row_values)})")

    values_str = ',\n'.join(values)
    insert_stmt = template.render(table_name=table_name, columns=columns, values=values_str)
    return insert_stmt

def generate_dml_file(db: PostgresDB, table_name: str, output_dir: str, output_file: str, batch_size: int = 10000) -> None:
    query = f"SELECT * FROM {table_name} LIMIT 1000000"
    output_path = os.path.join(output_dir, output_file)
    os.makedirs(output_dir, exist_ok=True)

    # Получаем и сохраняем названия колонок один раз
    db.connect()
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(query)
            columns = get_columns_from_cursor(cursor)
    finally:
        db.disconnect()

    i = 1
    for rows in fetch_data(db, query, batch_size):
        insert_statements = generate_insert_statements(rows, columns, table_name)
        write_dml_file_chunk(insert_statements, output_path, append=True)
        print(f"Обработано {len(rows) * i} строк...")
        i += 1

# Пример использования
db = PostgresDB()
output_dir = r'E:\LeetCode\db_comparator\db_comparator\Func_dict'
generate_dml_file(db, 's_grnplm_ld_cib_sbc_core.my_table', output_dir, 'giga1.sql', batch_size=50000)
