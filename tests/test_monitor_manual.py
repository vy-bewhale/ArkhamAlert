#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd

# Добавляем корень проекта в sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from arkham.arkham_monitor import ArkhamMonitor
from arkham.filter import DEFAULT_LIMIT

# --- Используем те же мок-данные, что и для теста DataProcessor ---
# (Скопировано из test_processor_manual.py для самодостаточности скрипта)
MOCK_RAW_TRANSACTIONS = [
    # 1. Стандартная транзакция ETH с entity и label
    {
        "blockNumber": 19789000,
        "blockTimestamp": "2025-05-05T10:30:00Z", "chain": "ethereum",
        "fromAddress": {"address": "0xSenderRealAddress", "arkhamEntity": {"type": "cex", "name": "Binance"}, "arkhamLabel": {"name": "Hot Wallet 7"}},
        "toAddress": {"address": "0xReceiverRealAddress", "arkhamEntity": {"type": "fund", "name": "Some Fund"}, "arkhamLabel": {"name": "Deposit Address"}},
        "historicalUSD": 1500000.55, "tokenId": "ethereum", "tokenSymbol": "ETH",
        "unitValue": "500000000000000000000" # 500 ETH
    },
    # 2. Транзакция USDC только с entity
    {
        "blockTimestamp": "2025-05-05T10:35:10Z", "chain": "polygon",
        "fromAddress": {"address": "0xAnotherSender", "arkhamEntity": {"type": "cex", "name": "Kraken"}},
        "toAddress": {"address": "0xAnotherReceiver"},
        "historicalUSD": 250000.1234, "tokenId": "usd-coin", "tokenSymbol": "USDC",
        "unitValue": "250000123456" # 250,000.123456 USDC
    },
    # 3. Транзакция без entity/label (только адреса)
    {
        "blockTimestamp": "2025-05-05T10:40:20Z", "chain": "arbitrum_one",
        "fromAddress": {"address": "0xPlainSender00112233445566778899aabbccddeeff"},
        "toAddress": {"address": "0xPlainReceiver99887766554433221100ffaabbcc"},
        "historicalUSD": 50000.0, "tokenId": "dai", "tokenSymbol": "DAI",
        "unitValue": "50000000000000000000000" # 50,000 DAI
    },
    # 4. Bitcoin-подобная транзакция
    {
        "blockTimestamp": "2025-05-05T10:45:30Z", "chain": "bitcoin",
        "fromAddresses": [{"address": {"address": "bc1qBitcoinSender1...", "arkhamLabel": {"name": "Miner Fee"}}}],
        "toAddresses": [{"address": {"address": "bc1qBitcoinReceiver1...", "arkhamEntity": {"name": "Exchange X"}}}],
        "historicalUSD": 1000000.0, "tokenId": "BITCOIN", "tokenSymbol": "BTC",
        "unitValue": "2000000000" # 20 BTC
    }
    # Пропускаем транзакции 5 и 6 для краткости этого теста
]

MOCK_API_RESPONSE = {'transfers': MOCK_RAW_TRANSACTIONS, 'count': len(MOCK_RAW_TRANSACTIONS)}

# --- Mock Класс для ArkhamClient ---
class MockArkhamClient:
    """Имитирует ArkhamClient, возвращая MOCK_API_RESPONSE."""
    def __init__(self, api_key: str, base_url: str):
        print(f"[MockArkhamClient] Инициализирован с api_key=***, base_url={base_url}")
        self.last_called_params = None # Сохраняем параметры последнего вызова

    def get_transfers(self, params: dict | None = None):
        print(f"[MockArkhamClient] Вызван get_transfers с параметрами: {params}")
        self.last_called_params = params
        # Имитируем ответ API
        # В реальном моке можно было бы даже фильтровать MOCK_RAW_TRANSACTIONS 
        # на основе params, но сейчас просто вернем все для проверки обработки.
        return MOCK_API_RESPONSE.copy() # Возвращаем копию, чтобы избежать модификации

def test_arkham_monitor():
    print("\n--- Тестирование ArkhamMonitor с Mock Client ---")

    # 1. Создаем Monitor (он создаст реальный клиент внутри, но мы его заменим)
    monitor = ArkhamMonitor(api_key="mock-key") # Ключ не важен, т.к. клиент будет заменен

    # 2. Создаем и подменяем клиент на мок
    mock_client = MockArkhamClient(api_key="mock-key", base_url=monitor.client.base_url)
    monitor.client = mock_client # Подмена!

    # --- Тест 3: initialize_cache --- 
    print("\n3. Тестируем initialize_cache():")
    init_lookback = '7d'
    init_usd = 50000
    init_limit = 50
    success = monitor.initialize_cache(lookback=init_lookback, usd_gte=init_usd, limit=init_limit)
    
    print(f"  initialize_cache вернул: {success}")
    assert success is True
    print("  Проверяем, что mock_client.get_transfers был вызван с правильными параметрами:")
    expected_init_params = {'timeLast': init_lookback, 'usdGte': init_usd, 'limit': init_limit}
    print(f"  Ожидалось: {expected_init_params}")
    print(f"  Реально было: {mock_client.last_called_params}")
    assert mock_client.last_called_params == expected_init_params
    
    print("  Проверяем состояние кешей после initialize_cache:")
    # Проверяем наличие некоторых ожидаемых элементов
    assert "Binance (Cex) - Hot Wallet 7" in monitor.get_known_address_names()
    assert "Kraken (Cex)" in monitor.get_known_address_names()
    assert "ETH" in monitor.get_known_token_symbols()
    assert "BTC" in monitor.get_known_token_symbols()
    print("  Кеши выглядят заполненными (базовая проверка).")

    # --- Тест 4: get_transactions --- 
    print("\n4. Тестируем get_transactions() с фильтрами:")
    # Устанавливаем фильтры (используем данные, которые точно есть в MOCK_RAW_TRANSACTIONS)
    filter_usd = 1000000
    filter_lookback = '1h'
    filter_tokens = ['BTC']
    filter_from = ['Miner Fee']
    filter_to = ['Exchange X']
    
    print(f"  Устанавливаем фильтры: usd>={filter_usd}, lookback={filter_lookback}, tokens={filter_tokens}, from={filter_from}, to={filter_to}")
    monitor.set_filters(
        min_usd=filter_usd, 
        lookback=filter_lookback, 
        token_symbols=filter_tokens,
        from_address_names=filter_from,
        to_address_names=filter_to
    )
    
    # Генерируем ожидаемые параметры API, которые должен создать фильтр
    expected_api_params = {
        'limit': DEFAULT_LIMIT, # Лимит по умолчанию для get_transactions
        'timeLast': filter_lookback,
        'usdGte': filter_usd,
        'tokens': 'bitcoin', # ID для BTC из кеша
        'from': 'bc1qbitcoinsender1...', # ID для Miner Fee из кеша
        'to': 'bc1qbitcoinreceiver1...' # ID для Exchange X из кеша
    }
    print(f"  Ожидаемые параметры для API: {expected_api_params}")
    
    # Вызываем get_transactions
    print("  Вызываем monitor.get_transactions()...")
    df = monitor.get_transactions(limit=DEFAULT_LIMIT)
    
    print("  Проверяем, что mock_client.get_transfers был вызван с ожидаемыми параметрами API:")
    print(f"  Реально было: {mock_client.last_called_params}")
    assert mock_client.last_called_params == expected_api_params

    print("  Проверяем результат DataFrame:")
    print(f"  Получено строк: {len(df)}")
    # Поскольку локальная фильтрация отключена, мы должны получить ВСЕ транзакции из MOCK_API_RESPONSE
    assert len(df) == len(MOCK_RAW_TRANSACTIONS)
    print("  DataFrame содержит ожидаемое количество строк (все из мока).")
    # Можно добавить более детальные проверки содержимого DataFrame, если нужно
    # Например, проверить наличие определенных колонок или значений
    expected_columns = ["Время", "Сеть", "Откуда", "Куда", "Символ", "Кол-во", "USD"]
    assert all(col in df.columns for col in expected_columns)
    print(f"  DataFrame содержит ожидаемые колонки: {list(df.columns)}")
    # print(df.to_string()) # Раскомментировать для просмотра содержимого DF

    print("\n--- Тестирование ArkhamMonitor Завершено ---")

if __name__ == "__main__":
    test_arkham_monitor() 