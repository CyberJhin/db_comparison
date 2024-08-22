from db_connection import PostgresDB
from FunctionDependencyAnalyzer_v3 import  FunctionDependencyAnalyzer_v3

def test():
    # Список схем
    schema_list = ['s_grnplm_ld_cib_sbc_refdata', 's_grnplm_ld_cib_sbc_core']

    # Путь к директории с файлами
    directory_path = 'Func_dict'  # Путь к директории с файлами

    # Инициализация подключения к базе данных
    db = PostgresDB()
    db.connect()

    try:
        # Инициализация анализатора зависимостей
        analyzer = FunctionDependencyAnalyzer_v3(db)

        # Анализ зависимостей и сохранение в Excel и TXT
        analyzer.analyze_dependencies(schema_list, directory_path)

    finally:
        # Отключение от базы данных
        db.disconnect()

if __name__ == '__main__':
    test()