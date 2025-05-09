print("Скрипт test_processor_and_filter_interaction.py ЗАПУЩЕН")
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

# Добавляем корень проекта в sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from arkham.cache import AddressCache, TokenCache
from arkham.data_processor import DataProcessor
from arkham.filter import TransactionFilter

def test_identifier_resolution_and_filtering():
    print("\n--- Тестирование разрешения идентификаторов и взаимодействия с фильтром ---")

    # 1. Инициализация компонентов
    address_cache = AddressCache()
    token_cache = TokenCache() # Нужен для DataProcessor, хотя здесь не тестируется напрямую
    data_processor = DataProcessor(address_cache, token_cache)
    transaction_filter = TransactionFilter(address_cache, token_cache)

    print("\n1. Компоненты инициализированы.")

    # 2. Тест: Проблемные данные от API - отсутствует `address` в `fromAddress`
    print("\n2. Тест: `fromAddress` без фактического адреса, но с entity name = 'ProblemEntityFrom'")
    
    raw_tx_data_from_no_address = {
        "txid": "tx_from_no_addr",
        "chain": "ethereum",
        "blockTimestamp": "2023-01-01T12:00:00Z",
        "fromAddress": {
            "arkhamEntity": {"name": "ProblemEntityFrom", "type": "Unknown"},
            "arkhamLabel": {"name": "From Label"}
        },
        "toAddress": {
            "address": "0xToAddressNormal",
            "arkhamEntity": {"name": "NormalToEntity", "type": "Exchange"},
            "arkhamLabel": {"name": "To Label"}
        },
        "unitValue": "100",
        "historicalUSD": "1000.00",
        "tokenId": "eth-main",
        "tokenSymbol": "ETH"
    }

    processed_tx1 = data_processor.process_transaction(raw_tx_data_from_no_address)
    print(f"  Обработанная транзакция (1): {processed_tx1}")
    
    from_display_name_1 = processed_tx1.get("Откуда")
    print(f"  Сгенерированное имя 'Откуда' (1): {from_display_name_1}")
    # Ожидаем: "ProblemEntityFrom (Unknown) - From Label"

    from_ids_in_cache_1 = address_cache.get_identifiers_by_name(from_display_name_1)
    print(f"  ID, найденные в кеше для имени '{from_display_name_1}': {from_ids_in_cache_1}")
    # Ожидаем: {"ProblemEntityFrom"}

    transaction_filter.update(from_address_names=[from_display_name_1])
    api_params_1 = transaction_filter.get_api_params()
    print(f"  Параметры API (1) (from_address_names=['{from_display_name_1}']): {api_params_1}")
    # Ожидаем: 'fromAddresses' содержит 'problementityfrom'
    if 'fromAddresses' in api_params_1 and "problementityfrom" in api_params_1['fromAddresses'].split(','):
        print("  ПРОВЕРКА: 'problementityfrom' присутствует в fromAddresses (1) - проблема воспроизведена.")
    else:
        print("  ПРОВЕРКА: 'problementityfrom' ОТСУТСТВУЕТ в fromAddresses (1) - проблема НЕ воспроизведена или что-то не так.")

    # 3. Тест: Проблемные данные от API - отсутствует `address` в `toAddress`
    print("\n3. Тест: `toAddress` без фактического адреса, но с entity name = 'ProblemEntityTo'")
    address_cache = AddressCache() 
    token_cache = TokenCache()
    data_processor = DataProcessor(address_cache, token_cache)
    transaction_filter = TransactionFilter(address_cache, token_cache)

    raw_tx_data_to_no_address = {
        "txid": "tx_to_no_addr",
        "chain": "ethereum",
        "blockTimestamp": "2023-01-02T12:00:00Z",
        "fromAddress": {
            "address": "0xFromAddressNormal",
            "arkhamEntity": {"name": "NormalFromEntity", "type": "Exchange"},
            "arkhamLabel": {"name": "From Label"}
        },
        "toAddress": {
            "arkhamEntity": {"name": "ProblemEntityTo", "type": "Unknown"},
            "arkhamLabel": {"name": "To Label"}
        },
        "unitValue": "200",
        "historicalUSD": "2000.00",
        "tokenId": "eth-main",
        "tokenSymbol": "ETH"
    }

    processed_tx2 = data_processor.process_transaction(raw_tx_data_to_no_address)
    print(f"  Обработанная транзакция (2): {processed_tx2}")

    to_display_name_2 = processed_tx2.get("Куда")
    print(f"  Сгенерированное имя 'Куда' (2): {to_display_name_2}")
    # Ожидаем: "ProblemEntityTo (Unknown) - To Label"

    to_ids_in_cache_2 = address_cache.get_identifiers_by_name(to_display_name_2)
    print(f"  ID, найденные в кеше для имени '{to_display_name_2}': {to_ids_in_cache_2}")
    # Ожидаем: {"ProblemEntityTo"}

    transaction_filter.update(to_address_names=[to_display_name_2])
    api_params_2 = transaction_filter.get_api_params()
    print(f"  Параметры API (2) (to_address_names=['{to_display_name_2}']): {api_params_2}")
    if 'toAddresses' in api_params_2 and "problementityto" in api_params_2['toAddresses'].split(','):
        print("  ПРОВЕРКА: 'problementityto' присутствует в toAddresses (2) - проблема воспроизведена.")
    else:
        print("  ПРОВЕРКА: 'problementityto' ОТСУТСТВУЕТ в toAddresses (2) - проблема НЕ воспроизведена или что-то не так.")

    # 4. Тест: Нормальные данные (контрольный)
    print("\n4. Тест: Нормальные данные с фактическими адресами")
    address_cache = AddressCache() 
    token_cache = TokenCache()
    data_processor = DataProcessor(address_cache, token_cache)
    transaction_filter = TransactionFilter(address_cache, token_cache)

    raw_tx_data_normal = {
        "txid": "tx_normal",
        "chain": "ethereum",
        "blockTimestamp": "2023-01-03T12:00:00Z",
        "fromAddress": {
            "address": "0xFromActualAddress",
            "arkhamEntity": {"name": "FromGoodEntity", "type": "Wallet"},
            "arkhamLabel": {"name": "From Wallet Label"}
        },
        "toAddress": {
            "address": "0xToActualAddress",
            "arkhamEntity": {"name": "ToGoodEntity", "type": "Wallet"},
            "arkhamLabel": {"name": "To Wallet Label"}
        },
        "unitValue": "300",
        "historicalUSD": "3000.00",
        "tokenId": "eth-main",
        "tokenSymbol": "ETH"
    }
    processed_tx3 = data_processor.process_transaction(raw_tx_data_normal)
    print(f"  Обработанная транзакция (3): {processed_tx3}")

    from_display_name_3 = processed_tx3.get("Откуда")
    to_display_name_3 = processed_tx3.get("Куда")
    print(f"  Сгенерированное имя 'Откуда' (3): {from_display_name_3}")
    print(f"  Сгенерированное имя 'Куда' (3): {to_display_name_3}")
    
    from_ids_in_cache_3 = address_cache.get_identifiers_by_name(from_display_name_3)
    to_ids_in_cache_3 = address_cache.get_identifiers_by_name(to_display_name_3)

    print(f"  ID для '{from_display_name_3}': {from_ids_in_cache_3}")
    # Ожидаем: {"0xFromActualAddress"}
    print(f"  ID для '{to_display_name_3}': {to_ids_in_cache_3}")
    # Ожидаем: {"0xToActualAddress"}

    transaction_filter.update(from_address_names=[from_display_name_3], to_address_names=[to_display_name_3])
    api_params_3 = transaction_filter.get_api_params()
    print(f"  Параметры API (3): {api_params_3}")
    if 'fromAddresses' in api_params_3 and api_params_3['fromAddresses'] == "0xfromactualaddress" and 'toAddresses' in api_params_3 and api_params_3['toAddresses'] == "0xtoactualaddress":
        print("  ПРОВЕРКА: Параметры API для нормальных данных (3) корректны.")
    else:
        print("  ПРОВЕРКА: Параметры API для нормальных данных (3) НЕКОРРЕКТНЫ.")

    print("\n--- Все тесты завершены ---")

if __name__ == "__main__":
    test_identifier_resolution_and_filtering() 