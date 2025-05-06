#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import json
from dotenv import load_dotenv

# Добавляем корень проекта в sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Импортируем ТОЛЬКО необходимые классы для этого теста
from arkham.arkham_client import ArkhamClient
from arkham.config import ArkhamAPIError, get_logger

logger = get_logger('test_client')
# logger.setLevel(logging.DEBUG) # Раскомментировать для детальных логов клиента
# logging.getLogger('arkham.arkham_client').setLevel(logging.DEBUG)

def test_api_calls():
    load_dotenv()
    api_key = os.getenv("ARKHAM_API_KEY")
    if not api_key:
        print("ОШИБКА: Не найден ARKHAM_API_KEY в .env файле.")
        return

    print("\n--- Тестирование ArkhamClient ---")
    try:
        client = ArkhamClient(api_key=api_key)
        print("ArkhamClient успешно инициализирован.")

        # --- Тест 1: Базовый запрос get_transfers ---
        print("\n1. Выполняем базовый запрос get_transfers (limit=5, lookback='5m')")
        params_step1 = {'limit': 5, 'timeLast': '5m'}
        try:
            response_step1 = client.get_transfers(params=params_step1)
            print("  Запрос выполнен успешно.")
            
            # Проверяем базовую структуру ответа
            if isinstance(response_step1, dict):
                print("  Ответ является словарем (dict).")
                print(f"  Ключи в ответе: {list(response_step1.keys())}")
                assert 'transfers' in response_step1
                print("  Ключ 'transfers' присутствует.")
                transfers_list = response_step1.get('transfers')
                if isinstance(transfers_list, list):
                    print(f"  Получено транзакций в списке 'transfers': {len(transfers_list)}")
                    # Показываем краткую информацию о первой транзакции, если есть
                    if transfers_list:
                        print("  Пример первой транзакции (часть данных):", json.dumps(transfers_list[0], indent=2)[:500] + "...")
                else:
                    print("  ОШИБКА: Значение по ключу 'transfers' не является списком!")
            else:
                print("  ОШИБКА: Ответ API не является словарем!")
                
        except ArkhamAPIError as e:
            print(f"  ОШИБКА API при базовом запросе: {e}")
        except Exception as e:
            print(f"  НЕПРЕДВИДЕННАЯ ОШИБКА при базовом запросе: {e}")
            logger.exception("Непредвиденная ошибка в Тесте 1")

        # --- Тест 2: Запрос с фильтрами --- 
        print("\n2. Выполняем запрос get_transfers с фильтрами (limit=10, lookback='1h', usdGte=100000)")
        params_step2 = {'limit': 10, 'timeLast': '1h', 'usdGte': 100000}
        # Можно добавить 'tokens': 'usdc,eth' или 'from': 'some_real_address' если нужно проверить их
        try:
            response_step2 = client.get_transfers(params=params_step2)
            print("  Запрос с фильтрами выполнен успешно.")
            if isinstance(response_step2, dict) and 'transfers' in response_step2 and isinstance(response_step2['transfers'], list):
                transfers_list_2 = response_step2['transfers']
                print(f"  Получено транзакций (с фильтрами): {len(transfers_list_2)}")
                # Проверяем несколько транзакций на соответствие usdGte (визуально)
                for i, tx in enumerate(transfers_list_2[:3]): # Смотрим первые 3
                    usd_val = tx.get('historicalUSD')
                    print(f"    Транзакция #{i+1}: USD = {usd_val}")
                    if usd_val is not None and usd_val < 100000:
                        print("      ПРЕДУПРЕЖДЕНИЕ: USD меньше ожидаемого! Возможно, ошибка фильтрации API?", json.dumps(tx, indent=2))
            else:
                print("  ОШИБКА: Структура ответа некорректна.")

        except ArkhamAPIError as e:
            print(f"  ОШИБКА API при запросе с фильтрами: {e}")
        except Exception as e:
            print(f"  НЕПРЕДВИДЕННАЯ ОШИБКА при запросе с фильтрами: {e}")
            logger.exception("Непредвиденная ошибка в Тесте 2")
            
        # --- Тест 3: Запрос с потенциально невалидными параметрами --- 
        print("\n3. Выполняем запрос с невалидным параметром (timeLast='invalid-format')")
        params_step3 = {'limit': 1, 'timeLast': 'invalid-format'}
        try:
            response_step3 = client.get_transfers(params=params_step3)
            # Если мы сюда попали, API не вернул ошибку - это странно, но возможно
            print(f"  ПРЕДУПРЕЖДЕНИЕ: Запрос с невалидным timeLast НЕ вызвал ошибку API. Ответ: {response_step3}") 
        except ArkhamAPIError as e:
            # Ожидаем ошибку 4xx от API
            print(f"  ПОЛУЧЕНА ОЖИДАЕМАЯ ОШИБКА API: {e}")
            # Проверяем, что ошибка содержит код (если он есть в тексте)
            if '400' in str(e) or '422' in str(e): # Bad Request или Unprocessable Entity
                 print("  Ошибка содержит код 4xx, что соответствует ожиданию.")
            else:
                 print(f"  ПРЕДУПРЕЖДЕНИЕ: Ошибка API есть, но не содержит ожидаемый код 4xx.")
        except Exception as e:
            print(f"  НЕПРЕДВИДЕННАЯ ОШИБКА при запросе с невалидным параметром: {e}")
            logger.exception("Непредвиденная ошибка в Тесте 3")

    except ValueError as ve: # Ошибка инициализации клиента (нет ключа)
        print(f"ОШИБКА инициализации клиента: {ve}")
    except Exception as e:
        print(f"НЕПРЕДВИДЕННАЯ ОШИБКА на уровне инициализации: {e}")
        logger.exception("Непредвиденная ошибка при инициализации клиента")
        
    print("\n--- Тестирование ArkhamClient Завершено ---")

if __name__ == "__main__":
    test_api_calls() 