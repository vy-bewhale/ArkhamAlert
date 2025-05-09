#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
from dotenv import load_dotenv

# Добавляем корень проекта в sys.path, чтобы можно было импортировать arkham
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from arkham.cache import AddressCache, TokenCache
from arkham.arkham_monitor import ArkhamMonitor

def deep_compare_states(state1: dict, state2: dict, cache_type: str) -> bool:
    """Deep compares two cache states, handling sets stored as lists."""
    if cache_type == "address":
        s1_cache = state1.get('cache')
        s2_cache = state2.get('cache')
        if s1_cache != s2_cache:
            print(f"  [FAIL] AddressCache '_cache' mismatch:\n  State1: {s1_cache}\n  State2: {s2_cache}")
            return False

        s1_name_to_ids = state1.get('name_to_ids')
        s2_name_to_ids = state2.get('name_to_ids')
        if s1_name_to_ids.keys() != s2_name_to_ids.keys():
            print(f"  [FAIL] AddressCache 'name_to_ids' keys mismatch:\n  State1 keys: {s1_name_to_ids.keys()}\n  State2 keys: {s2_name_to_ids.keys()}")
            return False
        for key in s1_name_to_ids:
            if set(s1_name_to_ids[key]) != set(s2_name_to_ids[key]): # Compare as sets
                print(f"  [FAIL] AddressCache 'name_to_ids' value mismatch for key '{key}':\n  State1: {s1_name_to_ids[key]}\n  State2: {s2_name_to_ids[key]}")
                return False
        print("  [OK] AddressCache states appear identical.")
        return True

    elif cache_type == "token":
        s1_id_to_symbol = state1.get('id_to_symbol')
        s2_id_to_symbol = state2.get('id_to_symbol')
        if s1_id_to_symbol != s2_id_to_symbol:
            print(f"  [FAIL] TokenCache 'id_to_symbol' mismatch:\n  State1: {s1_id_to_symbol}\n  State2: {s2_id_to_symbol}")
            return False

        s1_symbol_to_ids = state1.get('symbol_to_ids')
        s2_symbol_to_ids = state2.get('symbol_to_ids')
        if s1_symbol_to_ids.keys() != s2_symbol_to_ids.keys():
            print(f"  [FAIL] TokenCache 'symbol_to_ids' keys mismatch:\n  State1 keys: {s1_symbol_to_ids.keys()}\n  State2 keys: {s2_symbol_to_ids.keys()}")
            return False
        for key in s1_symbol_to_ids:
            if set(s1_symbol_to_ids[key]) != set(s2_symbol_to_ids[key]): # Compare as sets
                print(f"  [FAIL] TokenCache 'symbol_to_ids' value mismatch for key '{key}':\n  State1: {s1_symbol_to_ids[key]}\n  State2: {s2_symbol_to_ids[key]}")
                return False
        print("  [OK] TokenCache states appear identical.")
        return True
    return False

def test_address_cache_serialization():
    print("\n--- Тестирование сериализации AddressCache ---")
    original_cache = AddressCache()

    # 1. Наполняем кеш данными
    original_cache.update("binance-id-1", "Binance", True)
    original_cache.update("kraken-id-1", "Kraken", True)
    original_cache.update("0x123abc", "0x123...abc", False)
    original_cache.update("binance-id-2", "Binance", True) # Еще один ID для Binance
    original_cache.update("user-wallet-1", "My Wallet", True)
    original_cache.update("user-wallet-1", "My Personal Wallet", True) # Обновляем имя для существующего ID
    
    print("1. Исходный AddressCache наполнен:")
    print(f"  _cache: {original_cache._cache}")
    print(f"  _name_to_ids: {dict(original_cache._name_to_ids)}")
    print(f"  All real names: {original_cache.get_all_names()}")

    # 2. Получаем состояние кеша
    cache_state = original_cache.get_state()
    print("\n2. Получено состояние AddressCache:")
    print(f"  State: {cache_state}")
    assert 'cache' in cache_state
    assert 'name_to_ids' in cache_state

    # 3. Создаем новый кеш и загружаем состояние
    loaded_cache = AddressCache()
    loaded_cache.load_state(cache_state)
    print("\n3. Состояние загружено в новый AddressCache:")
    print(f"  Loaded _cache: {loaded_cache._cache}")
    print(f"  Loaded _name_to_ids: {dict(loaded_cache._name_to_ids)}")

    # 4. Сравниваем состояния и результаты методов
    print("\n4. Сравнение исходного и загруженного AddressCache:")
    
    # Сравнение внутренних структур (ключевой момент)
    # Прямое сравнение dict(original_cache._name_to_ids) и dict(loaded_cache._name_to_ids) может быть неточным из-за порядка в set
    # Поэтому get_state() из обоих должен быть идентичен, т.к. он сортирует/нормализует вывод list(set)
    loaded_cache_state_after_load = loaded_cache.get_state()
    assert deep_compare_states(cache_state, loaded_cache_state_after_load, "address"), "Состояния AddressCache (до и после загрузки) не совпадают!"

    # Сравнение результатов публичных методов
    assert original_cache.get_all_names() == loaded_cache.get_all_names(), "get_all_names() не совпадает"
    print("  [OK] get_all_names() совпадает.")
    
    test_name = "Binance"
    assert original_cache.get_identifiers_by_name(test_name) == loaded_cache.get_identifiers_by_name(test_name), f"get_identifiers_by_name('{test_name}') не совпадает"
    print(f"  [OK] get_identifiers_by_name('{test_name}') совпадает.")

    test_name_updated = "My Personal Wallet" # Имя, которое было обновлено
    assert original_cache.get_identifiers_by_name(test_name_updated) == loaded_cache.get_identifiers_by_name(test_name_updated), f"get_identifiers_by_name('{test_name_updated}') не совпадает"
    print(f"  [OK] get_identifiers_by_name('{test_name_updated}') совпадает ({original_cache.get_identifiers_by_name(test_name_updated)}).")

    print("--- Тестирование сериализации AddressCache Завершено ---")

def test_token_cache_serialization():
    print("\n--- Тестирование сериализации TokenCache ---")
    original_cache = TokenCache()

    # 1. Наполняем кеш данными
    original_cache.update("bitcoin-main", "BTC")
    original_cache.update("ethereum-main", "ETH")
    original_cache.update("weth-on-eth", "WETH") # Синоним ETH
    original_cache.update("usdc-on-eth", "USDC")
    original_cache.update("usdc-on-polygon", "USDC") # Другой ID для USDC
    original_cache.update("shiba-inu", "SHIB")
    original_cache.update("shiba-inu", "SHIBA") # Обновляем символ для существующего ID

    print("1. Исходный TokenCache наполнен:")
    print(f"  _id_to_symbol: {original_cache._id_to_symbol}")
    print(f"  _symbol_to_ids: {dict(original_cache._symbol_to_ids)}")
    print(f"  All symbols: {original_cache.get_all_symbols()}")

    # 2. Получаем состояние кеша
    cache_state = original_cache.get_state()
    print("\n2. Получено состояние TokenCache:")
    print(f"  State: {cache_state}")
    assert 'id_to_symbol' in cache_state
    assert 'symbol_to_ids' in cache_state

    # 3. Создаем новый кеш и загружаем состояние
    loaded_cache = TokenCache()
    loaded_cache.load_state(cache_state)
    print("\n3. Состояние загружено в новый TokenCache:")
    print(f"  Loaded _id_to_symbol: {loaded_cache._id_to_symbol}")
    print(f"  Loaded _symbol_to_ids: {dict(loaded_cache._symbol_to_ids)}")

    # 4. Сравниваем состояния и результаты методов
    print("\n4. Сравнение исходного и загруженного TokenCache:")
    loaded_cache_state_after_load = loaded_cache.get_state()
    assert deep_compare_states(cache_state, loaded_cache_state_after_load, "token"), "Состояния TokenCache (до и после загрузки) не совпадают!"

    assert original_cache.get_all_symbols() == loaded_cache.get_all_symbols(), "get_all_symbols() не совпадает"
    print("  [OK] get_all_symbols() совпадает.")

    test_symbol_eth = "ETH"
    assert original_cache.get_ids(test_symbol_eth) == loaded_cache.get_ids(test_symbol_eth), f"get_ids('{test_symbol_eth}') не совпадает"
    print(f"  [OK] get_ids('{test_symbol_eth}') совпадает.")
    
    test_symbol_usdc = "USDC"
    assert original_cache.get_ids(test_symbol_usdc) == loaded_cache.get_ids(test_symbol_usdc), f"get_ids('{test_symbol_usdc}') не совпадает"
    print(f"  [OK] get_ids('{test_symbol_usdc}') совпадает.")

    test_symbol_shiba = "SHIBA" # Обновленный символ
    assert original_cache.get_ids(test_symbol_shiba) == loaded_cache.get_ids(test_symbol_shiba), f"get_ids('{test_symbol_shiba}') не совпадает"
    print(f"  [OK] get_ids('{test_symbol_shiba}') совпадает ({original_cache.get_ids(test_symbol_shiba)}).")
    
    original_map = original_cache.get_symbol_to_ids_map()
    loaded_map = loaded_cache.get_symbol_to_ids_map()
    assert original_map.keys() == loaded_map.keys(), "get_symbol_to_ids_map() keys не совпадают"
    for k in original_map:
        assert original_map[k] == loaded_map[k], f"get_symbol_to_ids_map() values for key '{k}' не совпадает"
    print("  [OK] get_symbol_to_ids_map() совпадает.")

    print("--- Тестирование сериализации TokenCache Завершено ---")

def test_arkham_monitor_cache_serialization():
    print("\n--- Тестирование сериализации кешей через ArkhamMonitor ---")
    load_dotenv() # Для загрузки ARKHAM_API_KEY, если он нужен для инициализации
    api_key = os.getenv("ARKHAM_API_KEY", "dummy_api_key_for_test") # Используем dummy, если нет в .env

    # 1. Создаем и наполняем исходный монитор и его кеши
    original_monitor = ArkhamMonitor(api_key=api_key)
    
    # Наполняем AddressCache
    original_monitor.address_cache.update("addr-id-1", "Test Entity 1", True)
    original_monitor.address_cache.update("addr-id-2", "0xAnotherAddress", False)
    original_monitor.address_cache.update("addr-id-3", "Test Entity 1", True) # Еще ID для Test Entity 1
    
    # Наполняем TokenCache
    original_monitor.token_cache.update("token-id-btc", "BTC")
    original_monitor.token_cache.update("token-id-eth", "ETH")
    original_monitor.token_cache.update("token-id-weth", "WETH")

    print("1. Исходные кеши ArkhamMonitor наполнены.")
    print(f"  Original AddressCache state: {original_monitor.address_cache.get_state()}")
    print(f"  Original TokenCache state: {original_monitor.token_cache.get_state()}")

    # 2. Получаем полное состояние кешей через монитор
    full_cache_state = original_monitor.get_full_cache_state()
    print("\n2. Получено полное состояние кешей от ArkhamMonitor:")
    print(f"  Full state: {full_cache_state}")
    assert 'address_cache' in full_cache_state
    assert 'token_cache' in full_cache_state

    # 3. Создаем новый монитор и загружаем полное состояние
    loaded_monitor = ArkhamMonitor(api_key=api_key) # Новый экземпляр
    loaded_monitor.load_full_cache_state(full_cache_state)
    print("\n3. Полное состояние загружено в новый ArkhamMonitor.")

    # 4. Сравниваем состояния кешей нового монитора с исходным полным состоянием
    print("\n4. Сравнение кешей исходного и загруженного ArkhamMonitor:")
    
    loaded_monitor_address_cache_state = loaded_monitor.address_cache.get_state()
    assert deep_compare_states(full_cache_state['address_cache'], loaded_monitor_address_cache_state, "address"), \
        "Состояние AddressCache в ArkhamMonitor (до и после загрузки) не совпадает!"
    
    loaded_monitor_token_cache_state = loaded_monitor.token_cache.get_state()
    assert deep_compare_states(full_cache_state['token_cache'], loaded_monitor_token_cache_state, "token"), \
        "Состояние TokenCache в ArkhamMonitor (до и после загрузки) не совпадает!"

    print("--- Тестирование сериализации кешей ArkhamMonitor Завершено ---")

if __name__ == "__main__":
    test_address_cache_serialization()
    test_token_cache_serialization()
    test_arkham_monitor_cache_serialization() 