#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import json # Для красивого вывода словарей

# Добавляем корень проекта в sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from arkham.cache import AddressCache, TokenCache
from arkham.filter import TransactionFilter, DEFAULT_LIMIT

def populate_caches():
    """Создает и наполняет кеши тестовыми данными."""
    address_cache = AddressCache()
    token_cache = TokenCache()

    # Данные, похожие на те, что получились после теста DataProcessor
    # Адреса
    address_cache.update("0xSenderRealAddress", "Binance (Cex) - Hot Wallet 7", True)
    address_cache.update("0xReceiverRealAddress", "Some Fund (Fund) - Deposit Address", True)
    address_cache.update("0xAnotherSender", "Kraken (Cex)", True)
    address_cache.update("0xAnotherSender_2", "Kraken (Cex)", True) # Добавляем второй ID для Kraken
    address_cache.update("0xAnotherReceiver", "0xAno...eiver", False) # Не реальное имя
    address_cache.update("bc1qBitcoinSender1...", "Miner Fee", True)
    address_cache.update("bc1qBitcoinReceiver1...", "Exchange X", True)
    address_cache.update("0xPlainSender...", "0xPla...nder", False) # Не реальное имя

    # Токены
    token_cache.update("ethereum", "ETH") # ETH
    token_cache.update("usd-coin", "USDC") # USDC mainnet
    token_cache.update("dai", "DAI") # DAI
    token_cache.update("BITCOIN", "BTC") # BTC
    token_cache.update("l2-standard-bridged-weth-base", "WETH") # WETH (Синоним ETH)
    token_cache.update("bridged-usdc-polygon-pos-bridge", "USDC") # USDC Polygon
    token_cache.update("some-weird-op-token-id", "Obscure OP Token") # Токен без простого символа

    print("--- Начальное состояние кешей ---")
    print(f"  Address Names (real): {address_cache.get_all_names()}")
    print(f"  Token Symbols: {token_cache.get_all_symbols()}")
    print(f"  Token Map: {json.dumps(token_cache.get_symbol_to_ids_map(), indent=4, default=list)}")
    print("---------------------------------")
    return address_cache, token_cache

def test_filter_param_generation():
    print("\n--- Тестирование TransactionFilter.get_api_params ---")
    address_cache, token_cache = populate_caches()
    test_filter = TransactionFilter(address_cache, token_cache)

    # --- Тест 1: Нет активных фильтров (только лимит по умолчанию) ---
    print("\n1. Фильтры не установлены:")
    test_filter.update() # Сброс/установка пустых фильтров
    params = test_filter.get_api_params()
    print(f"  Generated Params: {params}")
    assert params == {'limit': DEFAULT_LIMIT}

    # --- Тест 2: Фильтры по USD и времени ---
    print("\n2. Фильтры: min_usd=1000000, lookback='1d'")
    test_filter.update(min_usd=1000000, lookback='1d')
    params = test_filter.get_api_params()
    print(f"  Generated Params: {params}")
    assert params == {'limit': DEFAULT_LIMIT, 'usdGte': 1000000, 'timeLast': '1d'}

    # --- Тест 3: Фильтр по известным токенам ---
    print("\n3. Фильтры: token_symbols=['ETH', 'BTC']")
    test_filter.update(token_symbols=['ETH', 'BTC'])
    params = test_filter.get_api_params()
    print(f"  Generated Params: {params}")
    # Ожидаем: ID для ETH/WETH и BTC/BITCOIN, отсортированы, lowercase
    expected_tokens = ",".join(sorted(["ethereum", "l2-standard-bridged-weth-base", "bitcoin"])).lower()
    assert params == {'limit': DEFAULT_LIMIT, 'tokens': expected_tokens}

    # --- Тест 4: Фильтр по известным ИМЕНАМ адресов (From) ---
    print("\n4. Фильтры: from_address_names=['Binance (Cex) - Hot Wallet 7', 'Miner Fee']")
    test_filter.update(from_address_names=['Binance (Cex) - Hot Wallet 7', 'Miner Fee'])
    params = test_filter.get_api_params()
    print(f"  Generated Params: {params}")
    # Ожидаем: ID для этих имен, отсортированы, lowercase
    expected_from = ",".join(sorted(["0xSenderRealAddress", "bc1qBitcoinSender1..."])).lower()
    assert params == {'limit': DEFAULT_LIMIT, 'from': expected_from}

    # --- Тест 5: Фильтр по известным ИМЕНАМ адресов (To) ---
    print("\n5. Фильтры: to_address_names=['Some Fund (Fund) - Deposit Address', 'Exchange X']")
    test_filter.update(to_address_names=['Some Fund (Fund) - Deposit Address', 'Exchange X'])
    params = test_filter.get_api_params()
    print(f"  Generated Params: {params}")
    expected_to = ",".join(sorted(["0xReceiverRealAddress", "bc1qBitcoinReceiver1..."])).lower()
    assert params == {'limit': DEFAULT_LIMIT, 'to': expected_to}

    # --- Тест 6: Комбинированные фильтры (USD, time, token, from, to) ---
    print("\n6. Фильтры: min_usd=50k, lookback=6h, tokens=['USDC'], from=['Kraken (Cex)'], to=['Exchange X']")
    test_filter.update(
        min_usd=50000,
        lookback='6h',
        token_symbols=['USDC'],
        from_address_names=['Kraken (Cex)'],
        to_address_names=['Exchange X']
    )
    params = test_filter.get_api_params()
    print(f"  Generated Params: {params}")
    expected_tokens_usdc = ",".join(sorted(['usd-coin', 'bridged-usdc-polygon-pos-bridge'])).lower()
    expected_from_kraken_multi = ",".join(sorted(["0xAnotherSender", "0xAnotherSender_2"])).lower()
    expected_to_exchangex = "bc1qbitcoinreceiver1..." # lowercase
    assert params == {
        'limit': DEFAULT_LIMIT,
        'usdGte': 50000,
        'timeLast': '6h',
        'tokens': expected_tokens_usdc,
        'from': expected_from_kraken_multi,
        'to': expected_to_exchangex
    }

    # --- Тест 7: Фильтр по имени/токену, которых НЕТ в кеше --- 
    print("\n7. Фильтры: tokens=['UNKNOWN'], from=['NonExistent Name']")
    test_filter.update(token_symbols=['UNKNOWN'], from_address_names=['NonExistent Name'])
    params = test_filter.get_api_params()
    print(f"  Generated Params: {params}")
    # Ожидаем, что параметры 'tokens' и 'from' НЕ будут добавлены, т.к. ID не найдены
    assert params == {'limit': DEFAULT_LIMIT}

    # --- Тест 8: Фильтр по НЕСКОЛЬКИМ именам, одно из которых имеет НЕСКОЛЬКО ID ---
    print("\n8. Фильтры: from_address_names=['Kraken (Cex)', 'Miner Fee']")
    test_filter.update(from_address_names=['Kraken (Cex)', 'Miner Fee'])
    params = test_filter.get_api_params()
    print(f"  Selected Names: ['Kraken (Cex)', 'Miner Fee']")
    print(f"  Expected IDs: {address_cache.find_identifiers_by_names(['Kraken (Cex)', 'Miner Fee'])}")
    print(f"  Generated Params: {params}")
    # Ожидаем: ID для Kraken (2 шт) и Miner Fee (1 шт), отсортированы, lowercase
    expected_from_multi = ",".join(sorted(["0xAnotherSender", "0xAnotherSender_2", "bc1qBitcoinSender1..."])).lower()
    assert params == {'limit': DEFAULT_LIMIT, 'from': expected_from_multi}
    
    print("\n--- Тестирование TransactionFilter.get_api_params Завершено ---")

if __name__ == "__main__":
    test_filter_param_generation() 