import os
import re

class FunctionDependencyAnalyzer_v3:
    def __init__(self, db):
        self.db = db

    def get_all_functions(self, schema_list):
        print("\n### Получение списка всех функций из указанных схем...")
        query = f"""
        select pp.proname
        from pg_catalog.pg_proc pp
        left join pg_catalog.pg_namespace n on pronamespace = n.oid
        where n.nspname in ({','.join(["%s"] * len(schema_list))})
        order by pp.proname;
        """
        functions_df = self.db.execute_query(query, schema_list)
        all_functions = functions_df['proname'].tolist()
        print(f"✓ Найдено {len(all_functions)} функций в указанных схемах.")
        return all_functions

    def find_functions_in_directory(self, function_list, directory_path):
        print(f"\n### Поиск функций в директории {directory_path}...")
        found_functions = set()

        for file in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                for func in function_list:
                     if func in content:
                        found_functions.add(func)
                        print(f"    • Функция '{func}' найдена в файле {file_path}.")
        return list(found_functions)

    def get_function_bodies(self, schema_list, function_list):
        print("\n### Получение тел функций...")
        query = f"""
        select pp.proname, pp.prosrc
        from pg_catalog.pg_proc pp
        left join pg_catalog.pg_namespace n on pronamespace = n.oid
        where n.nspname in ({','.join(["%s"] * len(schema_list))})
        and pp.proname in ({','.join(["%s"] * len(function_list))})
        order by pp.proname;
        """
        params = schema_list + function_list
        functions_df = self.db.execute_query(query, params)
        print(f"✓ Найдено {len(functions_df)} тел функций.")
        return functions_df

    def find_function_dependencies(self, function_body):
        pattern = r'\b([A-Za-z0-9_]+\.f_[A-Za-z0-9_]+|\b(f_[A-Za-z0-9_]+))\('

        matches = re.findall(pattern, function_body)
        extracted_funcs = [m[1] if m[1] else m[0].split('.')[1] for m in matches]
        return extracted_funcs

    def analyze_dependencies(self, schema_list, directory_path):
        all_functions = self.get_all_functions(schema_list)
        found_functions = self.find_functions_in_directory(all_functions, directory_path)

        function_bodies_df = self.get_function_bodies(schema_list, found_functions)

        unique_dependencies = set()
        all_used_and_searched = set()

        for _, row in function_bodies_df.iterrows():
            function_name = row['proname']
            function_body = row['prosrc']

            print(f"\n### Поиск зависимостей для функции: '{function_name}'")
            dependent_functions = self.find_function_dependencies(function_body)

            all_used_and_searched.add(function_name)
            all_used_and_searched.update(dependent_functions)

            if dependent_functions:
                unique_dependencies.update(dependent_functions)
                print(f"    ➜ Найдены зависимости: {', '.join(dependent_functions)}")
            else:
                print(f"    ➜ Зависимости не найдены.")

        # Сохранение результатов в Excel
        self.save_to_excel(function_bodies_df, unique_dependencies)

        # Сохранение уникальных функций в TXT
        self.save_unique_functions_to_txt(all_used_and_searched)

    def save_to_excel(self, function_bodies_df, unique_dependencies):
        df = function_bodies_df[['proname']]
        df['Dependencies'] = [', '.join(unique_dependencies) for _ in range(len(df))]
        df.to_excel('function_dependencies.xlsx', index=False)
        print("\n✓ Результаты сохранены в файл 'function_dependencies.xlsx'")

    def save_unique_functions_to_txt(self, all_used_and_searched):
        with open('unique_functions.txt', 'w') as txt_file:
            txt_file.write("\n".join(sorted(all_used_and_searched)))
        print("✓ Список всех уникальных функций сохранен в файл 'unique_functions.txt'")
