import sqlite3
import json
from datetime import datetime
import os

class RealEstateDatabase:
    def __init__(self, db_path='real_estate.db'):
        """Инициализация подключения к базе данных"""
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        
        # Создаем базу, если не существует
        self.connect()
        
    def connect(self):
        """Подключение к базе данных"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Для доступа к колонкам по имени
            self.cursor = self.connection.cursor()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            return False
            
    def close(self):
        """Закрытие подключения к базе данных"""
        if self.connection:
            self.connection.close()
            
    def init_database(self, schema_file='schema.sql'):
        """Инициализация базы данных из файла со схемой"""
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
                
            self.connection.executescript(sql_script)
            self.connection.commit()
            print("База данных успешно инициализирована")
            return True
        except sqlite3.Error as e:
            print(f"Ошибка инициализации базы данных: {e}")
            return False
        except FileNotFoundError:
            print(f"Файл {schema_file} не найден")
            return False
            
    def search_properties(self, filters=None, limit=10, offset=0):
        """
        Поиск недвижимости по заданным фильтрам
        
        Параметры:
        - filters (dict): Словарь с фильтрами поиска
            - min_price, max_price: минимальная/максимальная цена
            - min_area, max_area: минимальная/максимальная площадь
            - district_id: ID района
            - type_id: ID типа недвижимости
            - rooms: количество комнат
            - features: список ID особенностей
        - limit (int): Максимальное количество результатов
        - offset (int): Смещение для пагинации
        
        Возвращает:
        - list: Список объектов недвижимости
        """
        if filters is None:
            filters = {}
            
        query = """
        SELECT DISTINCT p.* 
        FROM Properties p
        """
        
        conditions = ["p.is_available = 1"]
        params = []
        
        # Добавляем JOIN для поиска по особенностям
        if 'features' in filters and filters['features']:
            query += """
            JOIN PropertyFeatures pf ON p.property_id = pf.property_id
            """
            feature_conditions = []
            for feature_id in filters['features']:
                feature_conditions.append("pf.feature_id = ?")
                params.append(feature_id)
                
            if feature_conditions:
                conditions.append(f"({' OR '.join(feature_conditions)})")
        
        # Добавляем условия фильтрации
        if 'min_price' in filters:
            conditions.append("p.price >= ?")
            params.append(filters['min_price'])
            
        if 'max_price' in filters:
            conditions.append("p.price <= ?")
            params.append(filters['max_price'])
            
        if 'min_area' in filters:
            conditions.append("p.area >= ?")
            params.append(filters['min_area'])
            
        if 'max_area' in filters:
            conditions.append("p.area <= ?")
            params.append(filters['max_area'])
            
        if 'district_id' in filters:
            conditions.append("p.district_id = ?")
            params.append(filters['district_id'])
            
        if 'type_id' in filters:
            conditions.append("p.type_id = ?")
            params.append(filters['type_id'])
            
        if 'rooms' in filters:
            conditions.append("p.rooms = ?")
            params.append(filters['rooms'])
            
        if 'has_balcony' in filters:
            conditions.append("p.has_balcony = ?")
            params.append(1 if filters['has_balcony'] else 0)
            
        if 'has_elevator' in filters:
            conditions.append("p.has_elevator = ?")
            params.append(1 if filters['has_elevator'] else 0)
            
        if 'has_parking' in filters:
            conditions.append("p.has_parking = ?")
            params.append(1 if filters['has_parking'] else 0)
        
        # Добавляем условия в запрос
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        # Добавляем сортировку, лимит и смещение
        query += """
        ORDER BY p.date_added DESC
        LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        
        try:
            self.cursor.execute(query, params)
            rows = self.cursor.fetchall()
            properties = []
            
            for row in rows:
                property_dict = {key: row[key] for key in row.keys()}
                
                # Получаем особенности недвижимости
                features_query = """
                SELECT f.*
                FROM Features f
                JOIN PropertyFeatures pf ON f.feature_id = pf.feature_id
                WHERE pf.property_id = ?
                """
                self.cursor.execute(features_query, (row['property_id'],))
                features = [dict(feature) for feature in self.cursor.fetchall()]
                property_dict['features'] = features
                
                # Получаем информацию о типе недвижимости
                self.cursor.execute("SELECT * FROM PropertyTypes WHERE type_id = ?", (row['type_id'],))
                property_type = dict(self.cursor.fetchone())
                property_dict['property_type'] = property_type
                
                # Получаем информацию о районе
                self.cursor.execute("SELECT * FROM Districts WHERE district_id = ?", (row['district_id'],))
                district = dict(self.cursor.fetchone())
                property_dict['district'] = district
                
                properties.append(property_dict)
                
            return properties
        except sqlite3.Error as e:
            print(f"Ошибка при поиске недвижимости: {e}")
            return []
    
    def get_property_by_id(self, property_id):
        """Получение информации о конкретном объекте недвижимости по ID"""
        try:
            # Увеличиваем счетчик просмотров
            self.cursor.execute(
                "UPDATE Properties SET views_count = views_count + 1 WHERE property_id = ?", 
                (property_id,)
            )
            self.connection.commit()
            
            # Получаем данные о недвижимости
            self.cursor.execute("SELECT * FROM Properties WHERE property_id = ?", (property_id,))
            property_row = self.cursor.fetchone()
            
            if not property_row:
                return None
                
            property_dict = dict(property_row)
            
            # Получаем особенности недвижимости
            features_query = """
            SELECT f.*
            FROM Features f
            JOIN PropertyFeatures pf ON f.feature_id = pf.feature_id
            WHERE pf.property_id = ?
            """
            self.cursor.execute(features_query, (property_id,))
            features = [dict(feature) for feature in self.cursor.fetchall()]
            property_dict['features'] = features
            
            # Получаем информацию о типе недвижимости
            self.cursor.execute("SELECT * FROM PropertyTypes WHERE type_id = ?", (property_row['type_id'],))
            property_type = dict(self.cursor.fetchone())
            property_dict['property_type'] = property_type
            
            # Получаем информацию о районе
            self.cursor.execute("SELECT * FROM Districts WHERE district_id = ?", (property_row['district_id'],))
            district = dict(self.cursor.fetchone())
            property_dict['district'] = district
            
            return property_dict
        except sqlite3.Error as e:
            print(f"Ошибка при получении информации о недвижимости: {e}")
            return None
    
    def get_districts(self):
        """Получение списка всех районов"""
        try:
            self.cursor.execute("SELECT * FROM Districts ORDER BY popularity DESC")
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Ошибка при получении списка районов: {e}")
            return []
    
    def get_property_types(self):
        """Получение списка всех типов недвижимости"""
        try:
            self.cursor.execute("SELECT * FROM PropertyTypes")
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Ошибка при получении списка типов недвижимости: {e}")
            return []
    
    def get_features(self):
        """Получение списка всех особенностей недвижимости"""
        try:
            self.cursor.execute("SELECT * FROM Features")
            return [dict(row) for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Ошибка при получении списка особенностей: {e}")
            return []
    
    def add_property(self, property_data):
        """
        Добавление нового объекта недвижимости
        
        Параметры:
        - property_data (dict): Данные о недвижимости
            - title: название
            - description: описание
            - type_id: ID типа недвижимости
            - district_id: ID района
            - address: адрес
            - price: цена
            - area: площадь
            - rooms: количество комнат
            - floor: этаж
            - total_floors: всего этажей
            - year_built: год постройки
            - renovation_year: год ремонта
            - has_balcony: наличие балкона
            - has_elevator: наличие лифта
            - has_parking: наличие парковки
            - image_url: URL изображения
            - contact_phone: контактный телефон
            - contact_name: контактное имя
            - latitude: широта
            - longitude: долгота
            - features: список ID особенностей
        
        Возвращает:
        - int: ID добавленного объекта или None в случае ошибки
        """
        try:
            # Подготавливаем запрос для добавления
            columns = [
                'title', 'description', 'type_id', 'district_id', 'address', 
                'price', 'area', 'rooms', 'floor', 'total_floors', 'year_built', 
                'renovation_year', 'has_balcony', 'has_elevator', 'has_parking', 
                'image_url', 'contact_phone', 'contact_name', 'latitude', 'longitude'
            ]
            
            values = []
            params = []
            
            for col in columns:
                if col in property_data:
                    values.append('?')
                    params.append(property_data[col])
                else:
                    values.append('NULL')
            
            query = f"""
            INSERT INTO Properties ({', '.join(columns)})
            VALUES ({', '.join(values)})
            """
            
            self.cursor.execute(query, params)
            property_id = self.cursor.lastrowid
            
            # Если есть особенности, добавляем их связи
            if 'features' in property_data and property_data['features']:
                features_query = """
                INSERT INTO PropertyFeatures (property_id, feature_id)
                VALUES (?, ?)
                """
                
                for feature_id in property_data['features']:
                    self.cursor.execute(features_query, (property_id, feature_id))
            
            self.connection.commit()
            return property_id
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении недвижимости: {e}")
            if self.connection:
                self.connection.rollback()
            return None
    
    def update_property(self, property_id, property_data):
        """
        Обновление информации об объекте недвижимости
        
        Параметры:
        - property_id (int): ID объекта недвижимости
        - property_data (dict): Данные для обновления
        
        Возвращает:
        - bool: True в случае успеха, False в случае ошибки
        """
        try:
            # Формируем SET часть запроса на обновление
            set_parts = []
            params = []
            
            update_columns = [
                'title', 'description', 'type_id', 'district_id', 'address', 
                'price', 'area', 'rooms', 'floor', 'total_floors', 'year_built', 
                'renovation_year', 'has_balcony', 'has_elevator', 'has_parking', 
                'image_url', 'contact_phone', 'contact_name', 'latitude', 'longitude',
                'is_available'
            ]
            
            for col in update_columns:
                if col in property_data:
                    set_parts.append(f"{col} = ?")
                    params.append(property_data[col])
            
            if not set_parts:
                return False  # Нет данных для обновления
                
            params.append(property_id)  # Для WHERE property_id = ?
            
            query = f"""
            UPDATE Properties 
            SET {', '.join(set_parts)}
            WHERE property_id = ?
            """
            
            self.cursor.execute(query, params)
            
            # Если есть особенности и нужно их обновить
            if 'features' in property_data:
                # Удаляем старые связи
                self.cursor.execute(
                    "DELETE FROM PropertyFeatures WHERE property_id = ?", 
                    (property_id,)
                )
                
                # Добавляем новые связи
                if property_data['features']:
                    features_query = """
                    INSERT INTO PropertyFeatures (property_id, feature_id)
                    VALUES (?, ?)
                    """
                    
                    for feature_id in property_data['features']:
                        self.cursor.execute(features_query, (property_id, feature_id))
            
            self.connection.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при обновлении недвижимости: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def delete_property(self, property_id):
        """
        Удаление объекта недвижимости (или пометка как недоступного)
        
        Параметры:
        - property_id (int): ID объекта недвижимости
        
        Возвращает:
        - bool: True в случае успеха, False в случае ошибки
        """
        try:
            # Мягкое удаление (пометка как недоступного)
            query = """
            UPDATE Properties 
            SET is_available = 0 
            WHERE property_id = ?
            """
            
            self.cursor.execute(query, (property_id,))
            self.connection.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при удалении недвижимости: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def natural_language_search(self, query_text):
        """
        Поиск недвижимости по естественному запросу пользователя
        Эта функция будет интегрироваться с ИИ-моделью для обработки запросов
        
        Параметры:
        - query_text (str): Текстовый запрос пользователя
        
        Возвращает:
        - list: Список объектов недвижимости
        """
        # В реальном приложении здесь будет вызов ИИ-модели для анализа запроса
        # и преобразования его в структурированные фильтры для поиска
        
        # Пока реализуем простой поиск по ключевым словам
        filters = {}
        
        # Примеры ключевых слов для демонстрации
        if 'квартира' in query_text.lower():
            filters['type_id'] = 1
        elif 'дом' in query_text.lower():
            filters['type_id'] = 2
        elif 'студия' in query_text.lower():
            filters['type_id'] = 3
            
        # Поиск по районам
        districts_query = "SELECT district_id, name FROM Districts"
        self.cursor.execute(districts_query)
        districts = self.cursor.fetchall()
        
        for district in districts:
            if district['name'].lower() in query_text.lower():
                filters['district_id'] = district['district_id']
                break
                
        # Поиск по количеству комнат
        for i in range(1, 10):
            if f"{i}-комнатн" in query_text.lower() or f"{i} комнат" in query_text.lower():
                filters['rooms'] = i
                break
                
        # Поиск по особенностям
        features_query = "SELECT feature_id, name FROM Features"
        self.cursor.execute(features_query)
        features = self.cursor.fetchall()
        
        feature_ids = []
        for feature in features:
            if feature['name'].lower() in query_text.lower():
                feature_ids.append(feature['feature_id'])
                
        if feature_ids:
            filters['features'] = feature_ids
            
        # Пытаемся найти диапазон цен
        # Это упрощенная логика, в реальном приложении нужен более сложный парсинг
        import re
        
        # Поиск минимальной цены
        min_price_match = re.search(r'от\s+(\d+)', query_text)
        if min_price_match:
            try:
                filters['min_price'] = int(min_price_match.group(1))
            except ValueError:
                pass
                
        # Поиск максимальной цены
        max_price_match = re.search(r'до\s+(\d+)', query_text)
        if max_price_match:
            try:
                filters['max_price'] = int(max_price_match.group(1))
            except ValueError:
                pass
        
        # Поиск по площади
        min_area_match = re.search(r'площадь\s+от\s+(\d+)', query_text)
        if min_area_match:
            try:
                filters['min_area'] = int(min_area_match.group(1))
            except ValueError:
                pass
                
        max_area_match = re.search(r'площадь\s+до\s+(\d+)', query_text)
        if max_area_match:
            try:
                filters['max_area'] = int(max_area_match.group(1))
            except ValueError:
                pass
        
        # Поиск по наличию балкона
        if 'балкон' in query_text.lower():
            filters['has_balcony'] = True
            
        # Поиск по наличию лифта
        if 'лифт' in query_text.lower():
            filters['has_elevator'] = True
            
        # Поиск по наличию парковки
        if 'парковк' in query_text.lower() or 'паркинг' in query_text.lower():
            filters['has_parking'] = True
            
        return self.search_properties(filters)

    def get_statistics(self):
        """
        Получение статистики по базе данных недвижимости
        
        Возвращает:
        - dict: Словарь со статистикой
        """
        stats = {}
        
        try:
            # Общее количество объектов
            self.cursor.execute("SELECT COUNT(*) as count FROM Properties WHERE is_available = 1")
            stats['total_properties'] = self.cursor.fetchone()['count']
            
            # Средняя цена
            self.cursor.execute("SELECT AVG(price) as avg_price FROM Properties WHERE is_available = 1")
            stats['average_price'] = round(self.cursor.fetchone()['avg_price'], 2)
            
            # Средняя площадь
            self.cursor.execute("SELECT AVG(area) as avg_area FROM Properties WHERE is_available = 1")
            stats['average_area'] = round(self.cursor.fetchone()['avg_area'], 2)
            
            # Статистика по типам недвижимости
            self.cursor.execute("""
                SELECT pt.name, COUNT(*) as count 
                FROM Properties p
                JOIN PropertyTypes pt ON p.type_id = pt.type_id
                WHERE p.is_available = 1
                GROUP BY pt.name
            """)
            stats['types'] = {row['name']: row['count'] for row in self.cursor.fetchall()}
            
            # Статистика по районам
            self.cursor.execute("""
                SELECT d.name, COUNT(*) as count 
                FROM Properties p
                JOIN Districts d ON p.district_id = d.district_id
                WHERE p.is_available = 1
                GROUP BY d.name
            """)
            stats['districts'] = {row['name']: row['count'] for row in self.cursor.fetchall()}
            
            # Самые популярные особенности
            self.cursor.execute("""
                SELECT f.name, COUNT(*) as count 
                FROM PropertyFeatures pf
                JOIN Features f ON pf.feature_id = f.feature_id
                JOIN Properties p ON pf.property_id = p.property_id
                WHERE p.is_available = 1
                GROUP BY f.name
                ORDER BY count DESC
                LIMIT 5
            """)
            stats['popular_features'] = {row['name']: row['count'] for row in self.cursor.fetchall()}
            
            return stats
        except sqlite3.Error as e:
            print(f"Ошибка при получении статистики: {e}")
            return {}

    def export_to_json(self, filename='real_estate_export.json'):
        """
        Экспортирует данные о недвижимости в JSON-файл
        
        Параметры:
        - filename (str): Имя файла для экспорта
        
        Возвращает:
        - bool: True в случае успеха, False в случае ошибки
        """
        try:
            # Получаем все объекты недвижимости
            properties = self.search_properties(limit=1000)
            
            # Сериализуем в JSON
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(properties, f, ensure_ascii=False, indent=4)
                
            return True
        except Exception as e:
            print(f"Ошибка при экспорте данных: {e}")
            return False
            
    def import_from_json(self, filename='real_estate_import.json'):
        """
        Импортирует данные о недвижимости из JSON-файла
        
        Параметры:
        - filename (str): Имя файла для импорта
        
        Возвращает:
        - bool: True в случае успеха, False в случае ошибки
        """
        try:
            # Читаем данные из JSON-файла
            with open(filename, 'r', encoding='utf-8') as f:
                properties = json.load(f)
                
            # Добавляем каждый объект недвижимости
            for property_data in properties:
                # Очищаем ID и другие автогенерируемые поля
                if 'property_id' in property_data:
                    del property_data['property_id']
                    
                if 'date_added' in property_data:
                    del property_data['date_added']
                    
                if 'views_count' in property_data:
                    del property_data['views_count']
                    
                # Извлекаем особенности
                features = []
                if 'features' in property_data:
                    for feature in property_data['features']:
                        features.append(feature['feature_id'])
                    
                    property_data['features'] = features
                    
                # Извлекаем тип недвижимости и район
                if 'property_type' in property_data:
                    property_data['type_id'] = property_data['property_type']['type_id']
                    del property_data['property_type']
                    
                if 'district' in property_data:
                    property_data['district_id'] = property_data['district']['district_id']
                    del property_data['district']
                
                # Добавляем объект в базу
                self.add_property(property_data)
                
            return True
        except Exception as e:
            print(f"Ошибка при импорте данных: {e}")
            return False