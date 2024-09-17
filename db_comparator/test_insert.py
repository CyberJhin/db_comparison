import concurrent.futures
import psycopg2
from psycopg2 import sql
import datetime
from tqdm import tqdm

# Функция для генерации insert-выражений для партии
def generate_insert_statements_batch(table_name, batch_size, offset, conn):
    try:
        with conn.cursor() as cur:
            # Вызываем функцию для генерации insert запросов для текущей партии
            cur.execute(sql.SQL("SELECT generate_insert_statements(%s, %s, %s)"), [table_name, batch_size, offset])
            # Поскольку функция в SQL использует RAISE NOTICE, мы ничего не получим здесь
            conn.commit()
    except Exception as e:
        print(f"Ошибка при выполнении: {e}")

# Функция для обновления строки с новыми insert выражениями
def append_to_log(log_id, insert_str, conn):
    try:
        with conn.cursor() as cur:
            # Обновляем строку в таблице insert_statements_log, дописывая новые выражения
            cur.execute(
                sql.SQL("""UPDATE insert_statements_log
                           SET insert_str = insert_str || %s
                           WHERE id = %s"""),
                [insert_str, log_id]
            )
            conn.commit()
    except Exception as e:
        print(f"Ошибка обновления лога: {e}")

# Функция для обработки данных партиями в параллельном режиме
def process_in_parallel(table_name, total_rows, batch_size):
    db_config = {
        'dbname': 'habrdb1',
        'user': 'habrpguser1',
        'password': 'pgpwd4habr',
        'host': 'localhost',
        'port': '54321'
    }

    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO insert_statements_log (table_name_db, insert_str, create_dtm) VALUES (%s, %s, %s) RETURNING id"),
                [table_name, '', datetime.datetime.now()]
            )
            log_id = cur.fetchone()[0]
            conn.commit()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            offsets = range(0, total_rows, batch_size)
            with tqdm(total=total_rows, desc="Обработка строк", unit="rows") as pbar:
                for offset in offsets:
                    futures.append(
                        executor.submit(generate_insert_statements_batch, table_name, batch_size, offset, conn)
                    )

                for future in concurrent.futures.as_completed(futures):
                    future.result()  # Проверка на ошибки
                    pbar.update(batch_size)

        # После обработки всех данных добавляем новую информацию в лог
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT insert_str FROM insert_statements_log WHERE id = %s"), [log_id])
            insert_str = cur.fetchone()[0] or ''
            cur.execute(sql.SQL("UPDATE insert_statements_log SET insert_str = %s WHERE id = %s"), [insert_str, log_id])
            conn.commit()

    finally:
        conn.close()

# Главная функция
def main():
    table_name = 'lite2'
    batch_size = 5000  # Размер партии
    total_rows = 50000  # Количество строк

    process_in_parallel(table_name, total_rows, batch_size)

if __name__ == "__main__":
    main()

###

CREATE OR REPLACE FUNCTION update_insert_log(
    table_name_db1 text,
insert_str1 text
)
RETURNS void AS $$
BEGIN
-- Обновляем строку в таблице insert_statements_log, дописывая новые insert выражения
UPDATE insert_statements_log
SET insert_str = insert_str || insert_str1
WHERE table_name_db = table_name_db1;

-- Проверяем, изменена ли строка
IF NOT FOUND THEN
-- Если строки не было, то добавляем новую запись
INSERT INTO insert_statements_log (table_name_db, insert_str, create_dtm)
VALUES (table_name_db1, insert_str1, now());
END IF;
END;
$$ LANGUAGE plpgsql;

select * from insert_statements_log

SELECT update_insert_log('employees2', 'INSERT INTO employees2 (id, name, salary) VALUES (3, ''Bob'', 5000);');


DROP FUNCTION update_insert_log(text,text)
DROP FUNCTION generate_insert_statements(text,integer,integer)

select generate_insert_statements('employees2', 100, 0)

CREATE OR REPLACE FUNCTION generate_insert_statements(
    table_name1 text,
limit_rows int,
offset_rows int
)
RETURNS void AS $$
DECLARE
col_names text;
rec record;
col_values text;
col_name text;
col_type text;
value text;
insert_str text := '';
BEGIN
RAISE NOTICE 'Processing table: %, limit: %, offset: %', table_name1, limit_rows, offset_rows;

-- Получаем список колонок
SELECT string_agg(quote_ident(column_name), ', ') INTO col_names
FROM information_schema.columns
WHERE table_name = table_name1
AND table_schema = 'public';

RAISE NOTICE 'Columns: %', col_names;

-- Обрабатываем каждую строку из таблицы с LIMIT и OFFSET
FOR rec IN EXECUTE format('SELECT ctid, * FROM %I ORDER BY ctid LIMIT %L OFFSET %L', table_name1, limit_rows, offset_rows) LOOP
col_values := '';

-- Проходим по каждой колонке
FOR col_name, col_type IN
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = table_name1
AND table_schema = 'public'
LOOP
EXECUTE format('SELECT %I FROM %I WHERE ctid = %L', col_name, table_name1, rec.ctid)
INTO value;

col_values := col_values ||
CASE
WHEN col_type IN ('text', 'character varying', 'character') THEN
'''' || replace(coalesce(value, 'NULL'), '''', '''''') || ''''
WHEN col_type = 'date' THEN
COALESCE('''' || value || '''', 'NULL')
WHEN col_type IN ('integer', 'bigint', 'smallint', 'numeric', 'double precision') THEN
COALESCE(value, 'NULL')
ELSE
COALESCE(value, 'NULL')
END || ', ';
END LOOP;

-- Убираем последнюю запятую
col_values := left(col_values, length(col_values) - 2);

-- Формируем INSERT-запрос
insert_str := insert_str || format('INSERT INTO %I ( %s ) VALUES ( %s );', table_name1, col_names, col_values) || ' ';

RAISE NOTICE 'Generated INSERT: %', insert_str;
END LOOP;

-- После обработки партии обновляем лог
RAISE NOTICE 'Updating log for table: % with insert_str of length %', table_name1, length(insert_str);
PERFORM update_insert_log(table_name1, insert_str);

END;
$$ LANGUAGE plpgsql;
####