#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import logging # Для настройки уровня логирования
from dotenv import load_dotenv

# Добавляем корень проекта в sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from arkham.arkham_monitor import ArkhamMonitor
from arkham.config import ArkhamAPIError, get_logger

# --- Настройка Логирования для Теста ---
# Установим DEBUG уровень для arkham.filter, чтобы видеть генерируемые параметры API
logging.getLogger('arkham.filter').setLevel(logging.DEBUG)
# Основной логгер теста
logger = get_logger('test_e2e')
logger.setLevel(logging.INFO) 

def run_e2e_test():
    load_dotenv()
    api_key = os.getenv("ARKHAM_API_KEY")
    if not api_key:
        print("ОШИБКА: Не найден ARKHAM_API_KEY в .env файле.")
        return

    print("\n--- === Шаг 1: Инициализация и Первичное Наполнение Кеша === ---")
    monitor = None
    try:
        monitor = ArkhamMonitor(api_key=api_key)
        logger.info("ArkhamMonitor инициализирован.")
        
        init_lookback = '1d'
        init_usd = 100000
        init_limit = 50 # Берем небольшой лимит для инициализации
        
        logger.info(f"Вызов initialize_cache(lookback='{init_lookback}', usd_gte={init_usd}, limit={init_limit})...")
        success = monitor.initialize_cache(lookback=init_lookback, usd_gte=init_usd, limit=init_limit)
        
        if not success:
            logger.error("Не удалось выполнить initialize_cache. Тест прерван.")
            return
            
        logger.info("initialize_cache выполнен успешно.")
        known_addresses = monitor.get_known_address_names()
        known_tokens = monitor.get_known_token_symbols()
        token_map = monitor.get_token_symbol_map()
        
        logger.info(f"Найдено реальных имен адресов: {len(known_addresses)}")
        if known_addresses:
            print(f"  Пример имен: {known_addresses[:10]}...")
        logger.info(f"Найдено символов токенов: {len(known_tokens)}")
        if known_tokens:
            print(f"  Символы: {known_tokens}")
        # print(f"  Карта токенов: {json.dumps(token_map, indent=2, default=list)}") # Можно раскомментировать для отладки
        
    except (ValueError, ArkhamAPIError) as e:
        logger.error(f"Ошибка на Шаге 1: {e}")
        return
    except Exception as e:
        logger.exception("Непредвиденная ошибка на Шаге 1")
        return
        
    # --- === Шаг 2: Установка Фильтров на Основе Полученных Данных === --- 
    print("\n--- === Шаг 2: Установка Фильтров === ---")
    if not monitor or not known_addresses or not known_tokens:
        logger.error("Нет данных для установки фильтров после Шага 1. Тест прерван.")
        return
        
    # Выбираем конкретные значения из полученных данных
    # Пример: берем первый реальный адрес и первый токен
    # ВАЖНО: Убедитесь, что эти элементы реально существуют в выводе Шага 1
    target_address_name = known_addresses[0] if known_addresses else None 
    target_token_symbol = known_tokens[0] if known_tokens else None 
    target_usd = 50000 # Уменьшим для теста
    target_lookback = '1h'
    
    if not target_address_name or not target_token_symbol:
        logger.warning("Не удалось выбрать адрес или токен из кеша для фильтрации. Используем только USD и lookback.")
        target_address_name_list = []
        target_token_symbol_list = []
    else:
         target_address_name_list = [target_address_name]
         target_token_symbol_list = [target_token_symbol]
         logger.info(f"Выбраны для фильтра: Адрес = '{target_address_name}', Токен = '{target_token_symbol}'")
         
    logger.info(f"Вызов set_filters(min_usd={target_usd}, lookback='{target_lookback}', token_symbols={target_token_symbol_list}, from_address_names={target_address_name_list})...")
    try:
        monitor.set_filters(
            min_usd=target_usd,
            lookback=target_lookback,
            token_symbols=target_token_symbol_list,
            from_address_names=target_address_name_list, # Фильтруем по ОТКУДА
            to_address_names=[] # Не фильтруем КУДА
        )
        logger.info("Фильтры установлены.")
    except Exception as e:
        logger.exception("Непредвиденная ошибка на Шаге 2")
        return
        
    # --- === Шаг 3: Запрос Транзакций по Фильтрам === --- 
    print("\n--- === Шаг 3: Запрос Транзакций по Фильтрам === ---")
    if not monitor:
         logger.error("Монитор не инициализирован. Тест прерван.")
         return
         
    try:
        logger.info("Вызов get_transactions()...")
        # Логи уровня DEBUG от arkham.filter покажут параметры, отправленные в API
        df = monitor.get_transactions(limit=20) # Небольшой лимит для теста
        
        logger.info(f"get_transactions завершен. Получено строк: {len(df)}")
        
        if not df.empty:
            print("Первые 5 строк DataFrame:")
            pd.set_option('display.max_rows', 10)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            print(df.head().to_string(index=False))
            
            # Визуальная проверка соответствия фильтрам (т.к. локальная фильтрация отключена)
            print("\nПроверка соответствия результата фильтрам (визуальная):")
            print(f"  Ожидался Токен: {target_token_symbol_list}")
            print(f"  Ожидался Отправитель (имя): {target_address_name_list}")
            print(f"  Ожидался USD >= {target_usd}")
            # Проверим первые несколько строк
            for index, row in df.head().iterrows():
                match = True
                if target_token_symbol_list and row['Символ'] not in target_token_symbol_list:
                     # Учитываем синонимы ETH/WETH и BTC/BITCOIN
                    if not ((row['Символ'] in ['ETH','WETH'] and any(ts in ['ETH','WETH'] for ts in target_token_symbol_list)) or \
                            (row['Символ'] in ['BTC','BITCOIN'] and any(ts in ['BTC','BITCOIN'] for ts in target_token_symbol_list))):
                        print(f"    Строка {index}: НЕСООТВЕТСТВИЕ ТОКЕНА! Ожидался '{target_token_symbol_list}', получен '{row['Символ']}'")
                        match = False
                if target_address_name_list and row['Откуда'] not in target_address_name_list:
                    print(f"    Строка {index}: НЕСООТВЕТСТВИЕ ОТПРАВИТЕЛЯ! Ожидался '{target_address_name_list}', получен '{row['Откуда']}'")
                    match = False
                # Проверка USD - API должен был отфильтровать, но проверим для уверенности
                try:
                    usd_num_str = row['USD'].replace('$','').replace(',','')
                    if float(usd_num_str) < target_usd:
                        print(f"    Строка {index}: НЕСООТВЕТСТВИЕ USD! Ожидался >= {target_usd}, получен {row['USD']}")
                        match = False
                except ValueError:
                    pass # Не можем сравнить, если USD не число
                if match:
                     print(f"    Строка {index}: Соответствует (визуально).")
                     
        else:
            logger.info("DataFrame пуст. Транзакций по заданным фильтрам не найдено.")
            
    except ArkhamAPIError as e:
        logger.error(f"Ошибка API на Шаге 3: {e}")
    except Exception as e:
        logger.exception("Непредвиденная ошибка на Шаге 3")

    print("\n--- === Сквозное Тестирование Завершено === ---")

if __name__ == "__main__":
    run_e2e_test() 