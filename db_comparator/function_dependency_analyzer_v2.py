import re
import pandas as pd
import os

class FunctionDependencyAnalyzer:
    def __init__(self, db):
        self.db = db

    def get_target_functions(self, file_path):
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            return self.get_functions_from_excel(file_path)
        elif file_extension == '.txt':
            return self.get_functions_from_txt(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

    def get_functions_from_excel(self, file_path):
        df = pd.read_excel(file_path)
        target_functions = df.iloc[:, 0].dropna().tolist()
        print("Функции для поиска из Excel:", target_functions)
        return target_functions

    def get_functions_from_txt(self, file_path):
        with open(file_path, 'r') as file:
            target_functions = [line.strip() for line in file if line.strip()]
        print("Функции для поиска из TXT:", target_functions)
        return target_functions

    def get_functions(self, schema_list):
        query = f"""
        select n.nspname, pp.proname, pp.prosrc
        from pg_catalog.pg_proc pp
        left join pg_catalog.pg_namespace n on pronamespace = n.oid
        where n.nspname in ({','.join(["%s"] * len(schema_list))})
        order by pp.proname, n.nspname;
        """
        return self.db.execute_query(query, schema_list)

    def find_function_dependencies(self, function_body):
        pattern = r'\b([A-Za-z0-9_]+\.[A-Za-z0-9_]+|\b(f_[A-Za-z0-9_]+))\('  ## ('schema.function_name', '') или ('', 'f_function_name')
        matches = re.findall(pattern, function_body)
        extracted_funcs = [m[1] if m[1] else m[0].split('.')[1] for m in matches]
        print("Найденные зависимости:", extracted_funcs)
        return extracted_funcs

    def analyze_dependencies(self, schema_list, target_functions):
        functions_df = self.get_functions(schema_list)

        data_for_excel = []
        data_for_txt = []
        unique_dependencies = set()  # Для хранения уникальных зависимостей

        for _, row in functions_df.iterrows():
            schema_name = row['nspname']
            function_name = row['proname']
            function_body = row['prosrc']

            if function_name in target_functions:
                print(f"Поиск зависимостей для функции: {function_name}")
                dependent_functions = self.find_function_dependencies(function_body)
                if dependent_functions:
                    for dep_func in dependent_functions:
                        data_for_excel.append([function_name, dep_func])
                        data_for_txt.append(dep_func)
                        unique_dependencies.add(dep_func)  # Добавляем в уникальный список
                else:
                    data_for_excel.append([function_name, ''])

        # Сохранение результатов в Excel
        df = pd.DataFrame(data_for_excel, columns=['Function', 'Dependency'])
        with pd.ExcelWriter('function_dependencies.xlsx') as writer:
            df.to_excel(writer, sheet_name='Dependencies', index=False)

            # Сохранение уникальных зависимостей в отдельный лист
            unique_df = pd.DataFrame(list(unique_dependencies), columns=['Unique Dependencies'])
            unique_df.to_excel(writer, sheet_name='Unique Dependencies', index=False)

        print("Результаты сохранены в файл function_dependencies.xlsx")

        # Сохранение результатов в TXT (простой список)
        with open('function_dependencies.txt', 'w') as txt_file:
            txt_file.write("\n".join(data_for_txt))
        print("Результаты сохранены в файл function_dependencies.txt")

        # Сохранение уникальных зависимостей в отдельный TXT файл
        with open('unique_function_dependencies.txt', 'w') as unique_txt_file:
            unique_txt_file.write("\n".join(sorted(unique_dependencies)))
        print("Уникальные зависимости сохранены в файл unique_function_dependencies.txt")
