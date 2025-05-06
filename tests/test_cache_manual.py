#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

# Добавляем корень проекта в sys.path, чтобы можно было импортировать arkham
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from arkham.cache import AddressCache, TokenCache

def test_address_cache():
    print("\n--- Тестирование AddressCache ---")
    cache = AddressCache()

    # 1. Добавляем "реальные" записи
    print("1. Добавляем 'Binance' (real=True) и 'Kraken Deposit' (real=True)")
    cache.update("binance-cex-id", "Binance", True)
    cache.update("kraken-deposit-addr", "Kraken Deposit", True)
    print(f"  Cache state: {cache._cache}")
    print(f"  Name->IDs map: {dict(cache._name_to_ids)}") # Преобразуем defaultdict для печати

    # 2. Добавляем запись без метки (адрес), is_real=False
    print("\n2. Добавляем адрес '0x123...abc' (real=False)")
    cache.update("0x123abc", "0x123...abc", False) 
    print(f"  Cache state: {cache._cache}")
    print(f"  Name->IDs map: {dict(cache._name_to_ids)}")

    # 3. Добавляем еще один идентификатор для существующего имени
    print("\n3. Добавляем 'binance-cex-id-2' для имени 'Binance' (real=True)")
    cache.update("binance-cex-id-2", "Binance", True)
    print(f"  Cache state: {cache._cache}")
    print(f"  Name->IDs map: {dict(cache._name_to_ids)}")

    # 4. Получаем список "реальных" имен
    print("\n4. Получаем get_all_names() (должны быть только реальные):")
    real_names = cache.get_all_names()
    print(f"  Результат: {real_names}")
    assert "Binance" in real_names
    assert "Kraken Deposit" in real_names
    assert "0x123...abc" not in real_names # Проверяем, что адреса нет

    # 5. Ищем идентификаторы по имени
    print("\n5. Ищем get_identifiers_by_name('Binance'):")
    binance_ids = cache.get_identifiers_by_name("Binance")
    print(f"  Результат: {binance_ids}")
    assert binance_ids == {"binance-cex-id", "binance-cex-id-2"}

    print("\n6. Ищем find_identifiers_by_names(['Binance', 'Kraken Deposit', 'NonExistent']):")
    found_ids = cache.find_identifiers_by_names(['Binance', 'Kraken Deposit', 'NonExistent'])
    print(f"  Результат: {found_ids}")
    assert found_ids == {"binance-cex-id", "binance-cex-id-2", "kraken-deposit-addr"}
    
    print("--- Тестирование AddressCache Завершено ---")

def test_token_cache():
    print("\n--- Тестирование TokenCache ---")
    cache = TokenCache()

    # 1. Добавляем токены
    print("1. Добавляем BTC, ETH (mainnet), WETH (base), USDC (polygon), USDC (mainnet)")
    cache.update("bitcoin-id", "BTC")
    cache.update("ethereum", "ETH")
    cache.update("l2-standard-bridged-weth-base", "WETH") # WETH как синоним ETH
    cache.update("bridged-usdc-polygon-pos-bridge", "USDC")
    cache.update("usd-coin", "USDC") # Другой ID для USDC
    cache.update("unknown-token-id", None) # Токен без символа
    
    print(f"  ID->Symbol map: {cache._id_to_symbol}")
    print(f"  Symbol->IDs map: {dict(cache._symbol_to_ids)}")

    # 2. Получаем все символы
    print("\n2. Получаем get_all_symbols():")
    symbols = cache.get_all_symbols()
    print(f"  Результат: {symbols}")
    assert "BTC" in symbols
    assert "BITCOIN" in symbols # Синоним должен быть здесь
    assert "ETH" in symbols
    assert "WETH" in symbols # Синоним должен быть здесь
    assert "USDC" in symbols
    assert "N/A" in symbols # Для токена без символа

    # 3. Получаем ID по символам (с учетом регистра и синонимов)
    print("\n3. Получаем ID по символам:")
    print(f"  get_ids('btc'): {cache.get_ids('btc')}")
    assert cache.get_ids('btc') == {"bitcoin-id"}
    print(f"  get_ids('BITCOIN'): {cache.get_ids('BITCOIN')}")
    assert cache.get_ids('BITCOIN') == {"bitcoin-id"}
    print(f"  get_ids('eth'): {cache.get_ids('eth')}")
    assert cache.get_ids('eth') == {"ethereum", "l2-standard-bridged-weth-base"}
    print(f"  get_ids('WETH'): {cache.get_ids('WETH')}")
    assert cache.get_ids('WETH') == {"ethereum", "l2-standard-bridged-weth-base"}
    print(f"  get_ids('UsDc'): {cache.get_ids('UsDc')}")
    assert cache.get_ids('UsDc') == {"bridged-usdc-polygon-pos-bridge", "usd-coin"}
    print(f"  get_ids('N/A'): {cache.get_ids('N/A')}")
    assert cache.get_ids('N/A') == {'unknown-token-id'}

    # 4. Ищем ID по списку символов
    print("\n4. Ищем find_ids_by_symbols(['WETH', 'USDC', 'NonExistent']):")
    found_ids = cache.find_ids_by_symbols(['WETH', 'USDC', 'NonExistent'])
    print(f"  Результат: {found_ids}")
    assert found_ids == {"ethereum", "l2-standard-bridged-weth-base", "bridged-usdc-polygon-pos-bridge", "usd-coin"}

    # 5. Получаем карту Символ -> ID
    print("\n5. Получаем get_symbol_to_ids_map():")
    symbol_map = cache.get_symbol_to_ids_map()
    print("  Результат:")
    for sym, ids in symbol_map.items():
        print(f"    {sym}: {ids}")
        
    assert symbol_map['ETH'] == {"ethereum", "l2-standard-bridged-weth-base"}
    assert symbol_map['USDC'] == {"bridged-usdc-polygon-pos-bridge", "usd-coin"}

    print("--- Тестирование TokenCache Завершено ---")

if __name__ == "__main__":
    test_address_cache()
    test_token_cache() 