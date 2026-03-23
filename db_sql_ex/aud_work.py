import sqlite3
import pandas as pd
from datetime import datetime


# Создание таблиц
def create_tables(conn):
    """Создать таблицы в соответствии со схемой данных"""
    cursor = conn.cursor()
    
    # Таблица клиентов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        phone TEXT,
        city TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Таблица товаров
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT,
            price REAL NOT NULL,
            stock INTEGER DEFAULT 0
        )
    ''')
    
    # Таблица заказов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL,
            status TEXT DEFAULT 'pending',
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    ''')
    
    # Таблица элементов заказа
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    ''')
    
    conn.commit()
    print("Таблицы созданы успешно!")


# Заполнение данных
def fill_tables(conn):
    """Заполнить таблицы демонстрационными данными"""
    cursor = conn.cursor()
    
    # Очистка таблиц (если уже существуют данные)
    cursor.execute('DELETE FROM order_items')
    cursor.execute('DELETE FROM orders')
    cursor.execute('DELETE FROM products')
    cursor.execute('DELETE FROM customers')
    
    # Сброс автоинкремента
    cursor.execute("DELETE FROM sqlite_sequence WHERE name='order_items'")
    cursor.execute("DELETE FROM sqlite_sequence WHERE name='orders'")
    cursor.execute("DELETE FROM sqlite_sequence WHERE name='products'")
    cursor.execute("DELETE FROM sqlite_sequence WHERE name='customers'")
    
    # Данные клиентов
    customers_data = [
        ('Иван Петров', 'ivan@example.com', '+7-900-111-11-11', 'Москва', '2025-01-15 10:30:00'),
        ('Мария Сидорова', 'maria@example.com', '+7-900-222-22-22', 'СПб', '2025-02-20 14:45:00'),
        ('Алексей Смирнов', 'alexey@example.com', '+7-900-333-33-33', 'Москва', '2025-03-10 09:15:00'),
        ('Елена Козлова', 'elena@example.com', '+7-900-444-44-44', 'Казань', '2025-04-05 16:20:00'),
        ('Дмитрий Морозов', 'dmitry@example.com', '+7-900-555-55-55', 'СПб', '2025-05-12 11:00:00'),
    ]
    cursor.executemany(
        'INSERT INTO customers (name, email, phone, city, created_at) VALUES (?, ?, ?, ?, ?)',
        customers_data
    )
    
    # Данные товаров
    products_data = [
        ('Ноутбук Dell XPS 15', 'Электроника', 120000.00, 10),
        ('Смартфон iPhone 15', 'Электроника', 95000.00, 25),
        ('Наушники Sony WH-1000XM5', 'Электроника', 35000.00, 50),
        ('Клавиатура Logitech MX Keys', 'Аксессуары', 12000.00, 100),
        ('Мышь Logitech MX Master 3', 'Аксессуары', 8500.00, 150),
        ('Монитор LG UltraFine 27"', 'Электроника', 45000.00, 30),
        ('Веб-камера Logitech Brio', 'Аксессуары', 15000.00, 40),
        ('SSD Samsung 980 PRO 1TB', 'Комплектующие', 13000.00, 75),
        ('Оперативная память Kingston 32GB', 'Комплектующие', 11000.00, 60),
        ('Блок питания be quiet! 750W', 'Комплектующие', 9500.00, 45),
    ]
    cursor.executemany(
        'INSERT INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)',
        products_data
    )
    
    # Данные заказов
    orders_data = [
        (1, '2025-06-01 12:00:00', 155000.00, 'completed'),
        (2, '2025-06-05 14:30:00', 103500.00, 'completed'),
        (1, '2025-06-10 10:15:00', 47000.00, 'shipped'),
        (3, '2025-06-15 16:45:00', 120000.00, 'pending'),
        (4, '2025-06-20 09:30:00', 23500.00, 'completed'),
        (5, '2025-06-25 11:00:00', 95000.00, 'shipped'),
        (2, '2025-07-01 13:20:00', 58000.00, 'pending'),
        (3, '2025-07-05 15:00:00', 35000.00, 'completed'),
        (4, '2025-07-10 10:45:00', 24000.00, 'pending'),
        (5, '2025-07-15 14:15:00', 133000.00, 'shipped'),
    ]
    cursor.executemany(
        'INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES (?, ?, ?, ?)',
        orders_data
    )
    
    # Данные элементов заказа
    order_items_data = [
        (1, 1, 1, 120000.00),
        (1, 5, 1, 8500.00),
        (1, 4, 2, 12000.00),
        (2, 2, 1, 95000.00),
        (2, 3, 1, 35000.00),
        (3, 3, 1, 35000.00),
        (3, 4, 1, 12000.00),
        (4, 1, 1, 120000.00),
        (5, 6, 1, 45000.00),
        (5, 4, 1, 12000.00),
        (5, 5, 1, 8500.00),
        (6, 2, 1, 95000.00),
        (7, 3, 1, 35000.00),
        (7, 7, 1, 15000.00),
        (7, 5, 1, 8500.00),
        (8, 3, 1, 35000.00),
        (9, 4, 2, 12000.00),
        (10, 1, 1, 120000.00),
        (10, 8, 1, 13000.00),
    ]
    cursor.executemany(
        'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)',
        order_items_data
    )
    
    conn.commit()
    print("Таблицы заполнены данными!")


# 3. Простые запросы (count, max, sum, avg)
def simple_queries(conn):
    """Пять простых запросов: count, max, sum, avg"""
    cursor = conn.cursor()
    
    
    print("ПРОСТЫЕ ЗАПРОСЫ (count, max, sum, avg)")
    
    
    # 1. COUNT - количество клиентов
    cursor.execute('SELECT COUNT(*) AS total_customers FROM customers')
    result = cursor.fetchone()
    print(f"\n1. COUNT - Количество клиентов: {result[0]}")
    
    # 2. MAX - максимальная цена товара
    cursor.execute('SELECT name, MAX(price) AS max_price FROM products')
    result = cursor.fetchone()
    print(f"2. MAX - Самый дорогой товар: {result[0]} ({result[1]:.2f} руб.)")
    
    # 3. SUM - общая сумма всех заказов
    cursor.execute('SELECT SUM(total_amount) AS total_orders FROM orders')
    result = cursor.fetchone()
    print(f"3. SUM - Общая сумма заказов: {result[0]:.2f} руб.")
    
    # 4. AVG - средняя стоимость заказа
    cursor.execute('SELECT AVG(total_amount) AS avg_order FROM orders')
    result = cursor.fetchone()
    print(f"4. AVG - Средняя стоимость заказа: {result[0]:.2f} руб.")
    
    # 5. COUNT + WHERE - количество выполненных заказов
    cursor.execute("SELECT COUNT(*) FROM orders WHERE status = 'completed'")
    result = cursor.fetchone()
    print(f"5. COUNT с условием - Выполненных заказов: {result[0]}")


# Запросы с агрегацией (GROUP BY)
def aggregation_queries(conn):
    """Три запроса с агрегацией данных"""
    cursor = conn.cursor()
    
    
    print("ЗАПРОСЫ С АГРЕГАЦИЕЙ (GROUP BY)")
    
    
    # 1. Количество заказов и сумма по статусам
    print("\n1. Статистика заказов по статусам:")
    cursor.execute('''
        SELECT status, 
               COUNT(*) AS order_count, 
               SUM(total_amount) AS total_sum,
               AVG(total_amount) AS avg_sum
        FROM orders 
        GROUP BY status
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]}: заказов={row[1]}, сумма={row[2]:.2f} руб., среднее={row[3]:.2f} руб.")
    
    # 2. Товары по категориям с общей стоимостью
    print("\n2. Товары по категориям:")
    cursor.execute('''
        SELECT category, 
               COUNT(*) AS product_count,
               SUM(stock) AS total_stock,
               AVG(price) AS avg_price,
               SUM(price * stock) AS total_value
        FROM products 
        GROUP BY category
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]}: товаров={row[1]}, на складе={row[2]}, ср.цена={row[3]:.2f}, общая стоимость={row[4]:.2f} руб.")
    
    # 3. Клиенты с количеством заказов и общей суммой покупок
    print("\n3. Активность клиентов:")
    cursor.execute('''
        SELECT c.name, 
               COUNT(o.id) AS order_count,
               COALESCE(SUM(o.total_amount), 0) AS total_spent
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        GROUP BY c.id, c.name
        ORDER BY total_spent DESC
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]}: заказов={row[1]}, потрачено={row[2]:.2f} руб.")


# Запросы с объединением таблиц (JOIN)
def join_queries(conn):
    """Три запроса с объединением таблиц и условиями"""
    cursor = conn.cursor()
    
    
    print("ЗАПРОСЫ С ОБЪЕДИНЕНИЕМ И УСЛОВИЯМИ (JOIN)")
    
    
    # 1. Детализация заказов с клиентами и товарами
    print("\n1. Детализация заказов (клиенты + товары):")
    cursor.execute('''
        SELECT c.name AS customer,
               o.id AS order_id,
               o.order_date,
               p.name AS product,
               oi.quantity,
               oi.price AS item_price,
               (oi.quantity * oi.price) AS subtotal
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        WHERE o.status IN ('completed', 'shipped')
        ORDER BY o.order_date
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]} | Заказ #{row[1]} от {row[2]} | {row[3]} x{row[4]} = {row[6]:.2f} руб.")
    
    # 2. Товары, которые были заказаны более одного раза
    print("\n2. Популярные товары (заказаны > 1 раза):")
    cursor.execute('''
        SELECT p.name, p.category, p.price,
               SUM(oi.quantity) AS total_sold,
               COUNT(DISTINCT oi.order_id) AS order_count
        FROM products p
        JOIN order_items oi ON p.id = oi.product_id
        GROUP BY p.id, p.name, p.category, p.price
        HAVING COUNT(DISTINCT oi.order_id) > 1
        ORDER BY total_sold DESC
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]} ({row[1]}): продано={row[3]} шт., в заказах={row[4]}, цена={row[2]:.2f} руб.")
    
    # 3. Клиенты с суммой заказов выше средней
    print("\n3. VIP-клиенты (сумма заказов выше средней):")
    cursor.execute('''
        SELECT c.name, c.email,
               COUNT(o.id) AS orders_count,
               SUM(o.total_amount) AS total_amount
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.id, c.name, c.email
        HAVING SUM(o.total_amount) > (SELECT AVG(total_amount) * 2 FROM orders)
        ORDER BY total_amount DESC
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]} | {row[1]} | заказов={row[2]}, сумма={row[3]:.2f} руб.")


# Импорт данных из внешних файлов (csv)
def import_from_csv(conn, csv_file='customers.csv'):
    """Импорт данных из CSV файла с использованием pandas"""
    try:
        # Чтение CSV с помощью pandas
        df = pd.read_csv(csv_file)
        # Вставка данных в таблицу
        df.to_sql('customers', conn, if_exists='append', index=False)
        
        print(f"\nДанные успешно импортированы из {csv_file}")
        print(f"Добавлено строк: {len(df)}")
        return True
    except FileNotFoundError:
        print(f"\nФайл {csv_file} не найден")
        return False
    except Exception as e:
        print(f"\nОшибка при импорте: {e}")
        return False


def import_from_txt(conn, txt_file='products.txt'):
    """Импорт данных из TXT файла (разделитель - табуляция или запятая)"""
    try:
        # Чтение TXT с помощью pandas (указываем разделитель)
        # Пробуем сначала табуляцию, потом запятую
        try:
            df = pd.read_csv(txt_file, sep='\t')
        except:
            df = pd.read_csv(txt_file, sep=',')
        
        print(f"\nДанные из {txt_file}:")
        print(df.to_string(index=False))
        
        return True
    except FileNotFoundError:
        print(f"\nФайл {txt_file} не найден")
        return False
    except Exception as e:
        print(f"\nОшибка при импорте: {e}")
        return False


def create_sample_csv():
    """Создать пример CSV файла для импорта"""
    csv_content = """name,email,phone,created_at
Пётр Иванов,petr@example.com,+7-900-666-66-66,2025-08-01 10:00:00
Анна Васильева,anna@example.com,+7-900-777-77-77,2025-08-05 14:30:00
Сергей Кузнецов,sergey@example.com,+7-900-888-88-88,2025-08-10 09:45:00"""
    with open('customers.csv', 'w', encoding='utf-8') as f:
        f.write(csv_content)
    print("Создан файл customers.csv для импорта")


def create_sample_txt():
    """Создать пример TXT файла для импорта"""
    txt_content = """name\tcategory\tprice\tstock
Планшет iPad Air\tЭлектроника\t55000.00\t20
Чехол для ноутбука\tАксессуары\t2500.00\t100
USB-хаб Anker\tАксессуары\t3500.00\t80"""
    with open('products.txt', 'w', encoding='utf-8') as f:
        f.write(txt_content)
    print("Создан файл products.txt для импорта")




def ege_task_solution_sql(conn):
    """Решение задачи ЕГЭ средствами SQL"""
    cursor = conn.cursor()
    
    
    print("РЕШЕНИЕ ЗАДАЧИ ЕГЭ (SQL)")
    
    # Вопрос 1: Клиенты из Москвы, сделавшие заказы
    print("\n1. Клиенты из Москвы, сделавшие заказы:")
    cursor.execute('''
        SELECT DISTINCT c.name, c.city
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        WHERE c.city = 'Москва'
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]} ({row[1]})")
    
    # Вопрос 2: Общая сумма заказов клиентов из СПб
    print("\n2. Общая сумма заказов клиентов из СПб:")
    cursor.execute('''
        SELECT c.name, SUM(o.total_amount) AS total_sum
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        WHERE c.city = 'СПб'
        GROUP BY c.id, c.name
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]:.2f} руб.")
    
    # Вопрос 3: Товары, заказанные клиентами из Казани
    print("\n3. Товары, заказанные клиентами из Казани:")
    cursor.execute('''
        SELECT DISTINCT p.name, p.price
        FROM products p
        JOIN order_items oi ON p.id = oi.product_id
        JOIN orders o ON oi.order_id = o.id
        JOIN customers c ON o.customer_id = c.id
        WHERE c.city = 'Казань'
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]} ({row[1]:.2f} руб.)")
    
    # Вопрос 4: Средний чек по городам
    print("\n4. Средний чек по городам:")
    cursor.execute('''
        SELECT c.city, AVG(o.total_amount) AS avg_check
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.city
        ORDER BY avg_check DESC
    ''')
    for row in cursor.fetchall():
        print(f"   {row[0]}: {row[1]:.2f} руб.")
    
    # Вопрос 5: Клиент, потративший больше всех
    print("\n5. Клиент, потративший больше всех:")
    cursor.execute('''
        SELECT c.name, SUM(o.total_amount) AS total_spent
        FROM customers c
        JOIN orders o ON c.id = o.customer_id
        GROUP BY c.id, c.name
        ORDER BY total_spent DESC
        LIMIT 1
    ''')
    row = cursor.fetchone()
    print(f"   {row[0]}: {row[1]:.2f} руб.")


def ege_task_solution_python(conn):
    """Решение задачи ЕГЭ средствами Python с обработкой данных"""
    cursor = conn.cursor()
    
    
    print("РЕШЕНИЕ ЗАДАЧИ ЕГЭ (Python + pandas)")
    
    
    # Загружаем данные в DataFrame
    customers_df = pd.read_sql_query('SELECT * FROM customers', conn)
    products_df = pd.read_sql_query('SELECT * FROM products', conn)
    orders_df = pd.read_sql_query('SELECT * FROM orders', conn)
    order_items_df = pd.read_sql_query('SELECT * FROM order_items', conn)
    
    print("\nИсходные данные загружены в DataFrame:")
    print(f"   Клиентов: {len(customers_df)}")
    print(f"   Товаров: {len(products_df)}")
    print(f"   Заказов: {len(orders_df)}")
    print(f"   Позиций в заказах: {len(order_items_df)}")
    
    # Вопрос 1: Клиенты из Москвы
    print("\n1. Клиенты из Москвы, сделавшие заказы (Python):")
    moscow_customers = customers_df[customers_df['city'] == 'Москва']
    moscow_ids = moscow_customers['id'].tolist()
    ordered_customers = orders_df[orders_df['customer_id'].isin(moscow_ids)]['customer_id'].unique()
    result = moscow_customers[moscow_customers['id'].isin(ordered_customers)]
    for _, row in result.iterrows():
        print(f"   {row['name']} ({row['city']})")
    
    # Вопрос 2: Сумма заказов клиентов из СПб
    print("\n2. Общая сумма заказов клиентов из СПб (Python):")
    spb_customers = customers_df[customers_df['city'] == 'СПб']
    spb_orders = orders_df[orders_df['customer_id'].isin(spb_customers['id'].tolist())]
    grouped = spb_orders.groupby('customer_id')['total_amount'].sum()
    for customer_id, total in grouped.items():
        name = customers_df[customers_df['id'] == customer_id]['name'].values[0]
        print(f"   {name}: {total:.2f} руб.")
    
    # Вопрос 4: Средний чек по городам (Python)
    print("\n4. Средний чек по городам (Python):")
    merged = orders_df.merge(customers_df, left_on='customer_id', right_on='id')
    avg_by_city = merged.groupby('city')['total_amount'].mean().sort_values(ascending=False)
    for city, avg in avg_by_city.items():
        print(f"   {city}: {avg:.2f} руб.")
    
    # Вопрос 5: Клиент, потративший больше всех (Python)
    print("\n5. Клиент, потративший больше всех (Python):")
    total_by_customer = orders_df.groupby('customer_id')['total_amount'].sum()
    max_customer_id = total_by_customer.idxmax()
    max_customer_name = customers_df[customers_df['id'] == max_customer_id]['name'].values[0]
    max_amount = total_by_customer[max_customer_id]
    print(f"   {max_customer_name}: {max_amount:.2f} руб.")


import os

def main():
    db_path = 'shop.db'
    
    # Удаляем старую базу данных если существует
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Старая база данных '{db_path}' удалена")
    
    conn = sqlite3.connect(db_path)
    
    try:
        # 1. Создание таблиц
        create_tables(conn)
        
        # 2. Заполнение таблиц
        fill_tables(conn)
        
        # 3. Выполнение запросов
        simple_queries(conn)
        aggregation_queries(conn)
        join_queries(conn)
        
        # 4. Создание примеров файлов для импорта
        
        print("ИМПОРТ ДАННЫХ ИЗ ВНЕШНИХ ФАЙЛОВ")
        
        create_sample_csv()
        create_sample_txt()
        
        # 5. Демонстрация импорта
        import_from_csv(conn, 'customers.csv')
        import_from_txt(conn, 'products.txt')
        
        # Проверка результата импорта
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM customers')
        print(f"\nВсего клиентов после импорта: {cursor.fetchone()[0]}")
        
        # 6. Задача на связывание таблиц (ЕГЭ 3)
        ege_task_solution_sql(conn)
        ege_task_solution_python(conn)
        
    finally:
        conn.close()
        print(f"\nРабота с базой данных '{db_path}' завершена")


if __name__ == '__main__':
    main()