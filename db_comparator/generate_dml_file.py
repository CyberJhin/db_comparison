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

# Функция для генерации вставок данных
def generate_insert_statements(cursor, columns, table_name: str) -> str:
    buffer = []
    template = jinja2.Template(TEMPLATE)
    for row in cursor:
        values = []
        for value in row:
            if value is None:
                values.append('NULL')
            elif isinstance(value, str):
                values.append(f"'{value}'")
            elif isinstance(value, date):
                values.append(f"'{value.strftime('%Y-%m-%d')}'")
            elif isinstance(value, time):
                values.append(f"'{value.strftime('%H:%M:%S')}'")
            elif isinstance(value, datetime):
                values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif isinstance(value, timedelta):
                days = value.days
                hours, remainder = divmod(value.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                values.append(f"'{days} days {hours:02d}:{minutes:02d}:{seconds:02d}'")
            else:
                values.append(str(value))
        values_str = ', '.join(values)
        insert_stmt = template.render(table_name=table_name, columns=columns, values=values_str)
        buffer.append(insert_stmt)
    return '\n'.join(buffer)

# Запись данных в файл порциями
def write_dml_file_chunk(insert_statements: str, output_path: str, append: bool = True) -> None:
    mode = 'a' if append else 'w'
    with open(output_path, mode) as f:
        f.write(insert_statements)

# Основная функция генерации DML файла с обработкой данных по частям (batch processing)
def generate_dml_file(db: PostgresDB, table_name: str, output_dir: str, output_file: str, batch_size: int = 10000) -> None:
    try:
        db.connect()

        # Обычный курсор без именованного серверного курсора
        with db.conn.cursor() as cursor:
            query = f"SELECT * FROM {table_name} LIMIT 500000"
            cursor.execute(query)

            # Проверяем, что запрос вернул данные
            if cursor.description is None:
                raise ValueError(f"Query did not return any results or failed: {query}")

            # Получаем имена столбцов
            columns = ', '.join([desc[0] for desc in cursor.description])

            output_path = os.path.join(output_dir, output_file)
            os.makedirs(output_dir, exist_ok=True)

            # Обработка данных по частям
            while True:
                rows = cursor.fetchmany(batch_size)  # Получаем порцию данных
                if not rows:
                    break  # Завершаем цикл, если данные закончились

                # Генерация DML-запросов для текущей порции данных
                insert_statements = generate_insert_statements(rows, columns, table_name)

                # Запись DML-запросов в файл
                write_dml_file_chunk(insert_statements, output_path, append=True)

                print(f"Processed {len(rows)} rows...")  # Отладочная информация

    except (psycopg2.DatabaseError, ValueError) as e:
        print(f"Error during database operation: {e}")
        raise
    finally:
        db.disconnect()


# Пример использования
db = PostgresDB()
output_dir = r'E:\LeetCode\db_comparator\db_comparator\Func_dict'
generate_dml_file(db, 's_grnplm_ld_cib_sbc_core.my_table', output_dir, 'giga.sql', batch_size=50000)
