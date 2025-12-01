import pandas as pd
import psycopg2
import os


DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST", "localhost")
CSV_FILE_NAME = 'filtered_tech_stocks_2024.csv'

try:
    print(f"Загрузка файла: {CSV_FILE_NAME}...")
    df = pd.read_csv(CSV_FILE_NAME)

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', utc=True)
    df = df.dropna(subset=['Date'])
    df['Year_Extracted'] = df['Date'].dt.year

    df_filtered = df[
        (df['Year_Extracted'] == 2024) &
        (df['Industry_Tag'] == 'technology')
        ]

    final_df = df_filtered.drop(columns=['Year_Extracted'], errors='ignore')

    if final_df.empty:
        print("ВНИМАНИЕ: После фильтрации не найдено ни одной строки.")
        exit()

    print(f"Фильтрация завершена. Найдено {len(final_df)} строк для загрузки.")

    COLUMNS = list(final_df.columns)

except Exception as e:
    print(f"КРИТИЧЕСКАЯ ОШИБКА при чтении/фильтрации данных: {e}")
    exit()

try:
    print("Подключение к PostgreSQL...")
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    column_definitions = []
    for col in COLUMNS:
        if col in ['Date']:

            column_definitions.append(f'"{col}" TIMESTAMP WITH TIME ZONE')
        elif col in ['Brand_Name', 'Ticker', 'Industry_Tag', 'Country']:
            column_definitions.append(f'"{col}" VARCHAR(100)')
        elif col in ['Volume']:
            column_definitions.append(f'"{col}" BIGINT')
        else:
            column_definitions.append(f'"{col}" DECIMAL')

    sql_columns_def = ", ".join(column_definitions)
    sql_columns_names = ", ".join([f'"{c}"' for c in COLUMNS])
    sql_placeholders = ", ".join(['%s'] * len(COLUMNS))

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS stock_data ({sql_columns_def});
    """)
    conn.commit()

    print("Начало записи данных в таблицу stock_data...")
    for index, row in final_df.iterrows():

        values = [row[col] for col in COLUMNS]
        cursor.execute(f"""
            INSERT INTO stock_data ({sql_columns_names})
            VALUES ({sql_placeholders});
        """, values)

    conn.commit()
    print("\n✅ УСПЕХ: Данные успешно загружены в таблицу stock_data.")

except Exception as e:
    print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА при работе с базой данных: {e}")
    print("Проверьте: 1. Пароль. 2. Имя БД. 3. Запущен ли сервер PostgreSQL.")
finally:
    if 'conn' in locals() and conn:
        conn.close()