from db_connection import PostgresDB
from function_dependency_analyzer_v2 import FunctionDependencyAnalyzer

def test():
    # Путь к файлу, содержащему список функций для поиска
    file_path = 'functions_to_search.txt'  # Поддерживается .xlsx и .txt

    # Список схем
    schema_list = ['s_grnplm_ld_cib_sbc_refdata', 's_grnplm_ld_cib_sbc_core']

    # Инициализация подключения к базе данных
    db = PostgresDB()
    db.connect()

    try:
        # Инициализация анализатора зависимостей
        analyzer = FunctionDependencyAnalyzer(db)

        # Получение целевых функций из файла (Excel или TXT)
        target_functions = analyzer.get_target_functions(file_path)

        # Анализ зависимостей и сохранение в Excel
        analyzer.analyze_dependencies(schema_list, target_functions)

    finally:
        # Отключение от базы данных
        db.disconnect()

if __name__ == '__main__':
    test()