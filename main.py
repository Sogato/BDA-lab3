import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


def connect_to_database(db_file):
    """Установление соединения с базой данных."""
    return sqlite3.connect(db_file)


def execute_query(connection, query, fetchall=False, fetchone=False):
    """Выполнение запроса к базе данных и возвращение результатов."""
    cursor = connection.cursor()
    cursor.execute(query)
    if fetchall:
        return cursor.fetchall()
    if fetchone:
        return cursor.fetchone()
    return cursor


def print_first_rows(connection):
    """Вывод первых 10 строк из таблицы."""
    print("Первые 10 строк DateTime из LA_wifi_speed_UK:")
    query = 'SELECT DateTime FROM LA_wifi_speed_UK ORDER BY DateTime LIMIT 10;'
    rows = execute_query(connection, query, fetchall=True)
    for row in rows:
        print(row)


def print_columns_description(connection):
    """Вывод описания столбцов таблицы."""
    print("\nОписание столбцов таблицы LA_wifi_speed_UK:")
    query = 'SELECT * FROM LA_wifi_speed_UK LIMIT 1'
    cursor = execute_query(connection, query)
    print(cursor.description[:10])

    columns = [member[0] for member in cursor.description]
    columns = [c.split('_')[0] for c in columns[1:]]  # Исключая первый столбец из метрик
    columns = list(set(columns))
    print("\nУникальные префиксы названий столбцов (первые 10):")
    print(columns[:10])
    return columns


def plot_metrics(connection, columns):
    """Построение графиков для метрик пинга, загрузки и выгрузки."""
    plt.figure(figsize=(10, 8))
    area = columns[0]  # Пример области
    suffix = {'_p': 'ping', '_d': 'download', '_u': 'upload'}

    for s, label in suffix.items():
        query = f'SELECT DateTime, {area}{s} FROM LA_wifi_speed_UK ORDER BY DateTime'
        df = pd.read_sql_query(query, connection)
        plt.plot(pd.to_datetime(df['DateTime']), df[f'{area}{s}'], label=label)

    plt.legend()
    plt.title(f"Internet Speed Metrics for {area}")
    plt.savefig(f'metrics_plot.png')


def calculate_averages(connection, columns):
    """Расчет средних значений для каждой области."""
    new_columns = ['Area', 'Average_p', 'Average_d', 'Average_u']
    df = pd.DataFrame(columns=new_columns)
    suffix = {'_p': 'ping', '_d': 'download', '_u': 'upload'}

    for area in columns:
        tmp_list = [area]
        for s, _ in suffix.items():
            column_name = f"{area}{s}"
            query = f'SELECT AVG("{column_name}") FROM LA_wifi_speed_UK'
            mean = execute_query(connection, query, fetchone=True)
            tmp_list.append(mean[0] if mean[0] is not None else 0)

        df = df._append(pd.Series(tmp_list, index=new_columns), ignore_index=True)

    print("\nСредние значения метрик интернет-скорости для различных областей (первые 5 записей):")
    print(df.head())

    return df


def plot_averages(df):
    """Построение графика средних значений."""
    plt.figure(figsize=(20, 10))
    plt.plot(df.index, df[['Average_d', 'Average_u', 'Average_p']], 'o-')
    plt.legend(['Average Download', 'Average Upload', 'Average Ping'])
    plt.title('Average Internet Speed Metrics')
    plt.savefig('average_metrics_plot.png')


def create_average_speed_table(connection, df):
    """Создание таблицы средних скоростей в базе данных."""
    try:
        execute_query(connection, 'DROP TABLE IF EXISTS average_speed')
        df.to_sql('average_speed', connection)
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")


def fetch_and_print_joined_data(connection):
    """Извлечение и печать объединенных данных из таблиц."""
    query = 'SELECT * FROM average_speed JOIN LA_population ON LA_population."LA_code"=average_speed.Area'
    rows = execute_query(connection, query, fetchall=True)
    print("\nОбъединенные данные из average_speed и LA_population (первые 10 записей):")
    for row in rows[:10]:  # Ограничение вывода первыми 10 строками
        print(row)


if __name__ == "__main__":
    # Подключение к базе данных
    conn = connect_to_database('InternetSpeed.db')

    # Вывод первых строк и описания столбцов
    print_first_rows(conn)
    columns = print_columns_description(conn)

    # Построение графиков и расчет средних значений
    plot_metrics(conn, columns)
    df_averages = calculate_averages(conn, columns)

    # Построение графика средних значений и создание таблицы
    plot_averages(df_averages)
    create_average_speed_table(conn, df_averages)

    # Извлечение и печать объединенных данных
    fetch_and_print_joined_data(conn)

    # Закрытие соединения с базой данных
    conn.close()
