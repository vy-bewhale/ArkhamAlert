#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import json # Для красивого вывода словарей

# Добавляем корень проекта в sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from arkham.cache import AddressCache, TokenCache
from arkham.data_processor import DataProcessor

# --- Пример "сырых" данных транзакций (имитация ответа API) ---
MOCK_RAW_TRANSACTIONS = [
    # 1. Стандартная транзакция ETH с entity и label
    {
        "blockNumber": 19789000,
        "blockTimestamp": "2025-05-05T10:30:00Z",
        "chain": "ethereum",
        "fromAddress": {
            "address": "0xSenderRealAddress",
            "arkhamEntity": {"type": "cex", "name": "Binance", "id": "binance_entity"},
            "arkhamLabel": {"name": "Hot Wallet 7", "id": "binance_label_hw7"},
            "chain": "ethereum"
        },
        "toAddress": {
            "address": "0xReceiverRealAddress",
            "arkhamEntity": {"type": "fund", "name": "Some Fund", "id": "fund_entity"},
            "arkhamLabel": {"name": "Deposit Address", "id": "fund_label_dep"},
            "chain": "ethereum"
        },
        "historicalUSD": 1500000.55,
        "tokenId": "ethereum", # ID для ETH
        "tokenSymbol": "ETH",
        "tokenName": "Ethereum",
        "transactionHash": "0xtxhash1...",
        "unitValue": "500000000000000000000" # 500 ETH
    },
    # 2. Транзакция USDC только с entity
    {
        "blockTimestamp": "2025-05-05T10:35:10Z",
        "chain": "polygon",
        "fromAddress": {
            "address": "0xAnotherSender",
            "arkhamEntity": {"type": "cex", "name": "Kraken", "id": "kraken_entity"},
             # Нет arkhamLabel
            "chain": "polygon"
        },
        "toAddress": {
            "address": "0xAnotherReceiver",
             # Нет arkhamEntity, нет arkhamLabel
            "chain": "polygon"
        },
        "historicalUSD": 250000.1234,
        "tokenId": "usd-coin", # ID для USDC
        "tokenSymbol": "USDC",
        "tokenName": "USD Coin",
        "transactionHash": "0xtxhash2...",
        "unitValue": "250000123456" # 250,000.123456 USDC (6 decimals)
        # --- NB: Наш процессор не использует decimals из API /transfers сейчас --- 
        # --- Он форматирует unitValue как есть, т.к. decimals не передаются --- 
        # --- Поэтому форматирование Кол-во будет "неправильным" без знания decimals --- 
    },
    # 3. Транзакция без entity/label (только адреса)
    {
        "blockTimestamp": "2025-05-05T10:40:20Z",
        "chain": "arbitrum_one",
        "fromAddress": {
            "address": "0xPlainSender00112233445566778899aabbccddeeff",
            "chain": "arbitrum_one"
        },
        "toAddress": {
            "address": "0xPlainReceiver99887766554433221100ffaabbcc",
            "chain": "arbitrum_one"
        },
        "historicalUSD": 50000.0,
        "tokenId": "dai", # ID для DAI
        "tokenSymbol": "DAI",
        "transactionHash": "0xtxhash3...",
        "unitValue": "50000000000000000000000" # 50,000 DAI (18 decimals)
    },
    # 4. Bitcoin-подобная транзакция (fromAddresses/toAddresses)
    {
        "blockTimestamp": "2025-05-05T10:45:30Z",
        "chain": "bitcoin",
        # Нет fromAddress/toAddress
        "fromAddresses": [
            {"address": {"address": "bc1qBitcoinSender1...", "arkhamLabel": {"name": "Miner Fee"}, "chain": "bitcoin"}}
        ],
        "toAddresses": [
            {"address": {"address": "bc1qBitcoinReceiver1...", "arkhamEntity": {"name": "Exchange X"}, "chain": "bitcoin"}},
            {"address": {"address": "bc1qBitcoinChange...", "chain": "bitcoin"}} # Адрес сдачи
        ],
        "historicalUSD": 1000000.0,
        "tokenId": "BITCOIN", # Bitcoin обычно не имеет ID, используем символ
        "tokenSymbol": "BTC",
        "transactionHash": "btctxid1...",
        "unitValue": "2000000000" # 20 BTC (8 decimals)
    },
    # 5. Транзакция с токеном без символа
    {
        "blockTimestamp": "2025-05-05T10:50:40Z",
        "chain": "optimism",
        "fromAddress": {"address": "0xOptiSender", "chain": "optimism"},
        "toAddress": {"address": "0xOptiReceiver", "chain": "optimism"},
        "historicalUSD": 100.0,
        "tokenId": "some-weird-op-token-id",
        # Нет tokenSymbol
        "tokenName": "Obscure OP Token",
        "transactionHash": "0xtxhash5...",
        "unitValue": "100000000000000000000" # 100 токенов
    },
    # 6. Транзакция с очень маленьким значением
    {
        "blockTimestamp": "2025-05-05T10:55:50Z",
        "chain": "ethereum",
        "fromAddress": {"address": "0xDustSender", "chain": "ethereum"},
        "toAddress": {"address": "0xDustReceiver", "chain": "ethereum"},
        "historicalUSD": 0.001,
        "tokenId": "ethereum",
        "tokenSymbol": "ETH",
        "transactionHash": "0xtxhash6...",
        "unitValue": "1000000000" # 0.000000001 ETH 
    }
]

def test_data_processor():
    print("\n--- Тестирование DataProcessor ---")
    address_cache = AddressCache()
    token_cache = TokenCache()
    processor = DataProcessor(address_cache, token_cache)

    processed_results = []

    print("\n1. Обработка транзакций по одной:")
    for i, raw_tx in enumerate(MOCK_RAW_TRANSACTIONS):
        print(f"\n--- Обработка Транзакции #{i+1} ---")
        print("Сырые данные (начало):")
        print(json.dumps(raw_tx, indent=2)[:300] + "...") # Показываем начало сырых данных
        
        processed_tx = processor.process_transaction(raw_tx)
        
        print("\nОбработанный результат:")
        if processed_tx:
            print(json.dumps(processed_tx, indent=2))
            processed_results.append(processed_tx)
        else:
            print("  Не удалось обработать транзакцию.")
            
        print("\nСостояние кешей ПОСЛЕ обработки:")
        print(f"  Address Cache (_cache): {processor.address_cache._cache}")
        print(f"  Address Cache (_name_to_ids): {dict(processor.address_cache._name_to_ids)}")
        print(f"  Token Cache (_id_to_symbol): {processor.token_cache._id_to_symbol}")
        print(f"  Token Cache (_symbol_to_ids): {dict(processor.token_cache._symbol_to_ids)}")

    print("\n\n--- Проверка пакетной обработки (должна дать те же результаты кешей) ---")
    # Создаем новые пустые кеши и процессор для чистоты эксперимента
    address_cache_batch = AddressCache()
    token_cache_batch = TokenCache()
    processor_batch = DataProcessor(address_cache_batch, token_cache_batch)
    
    # Формируем имитацию ответа API
    mock_api_response = {'transfers': MOCK_RAW_TRANSACTIONS, 'count': len(MOCK_RAW_TRANSACTIONS)}
    
    processed_list_batch = processor_batch.process_transactions_response(mock_api_response)
    
    print("Сравнение количества обработанных транзакций (по одной vs пакет):")
    print(f"  По одной: {len(processed_results)}, Пакетом: {len(processed_list_batch)}")
    assert len(processed_results) == len(processed_list_batch)
    
    print("\nИтоговое состояние кешей ПОСЛЕ пакетной обработки:")
    print(f"  Address Cache (_cache): {processor_batch.address_cache._cache}")
    print(f"  Address Cache (_name_to_ids): {dict(processor_batch.address_cache._name_to_ids)}")
    print(f"  Token Cache (_id_to_symbol): {processor_batch.token_cache._id_to_symbol}")
    print(f"  Token Cache (_symbol_to_ids): {dict(processor_batch.token_cache._symbol_to_ids)}")
    
    # Сравним состояние кешей (должно быть идентичным)
    print("\nСравнение состояния кешей (по одной vs пакет):")
    print(f"  Address _cache match: {processor.address_cache._cache == processor_batch.address_cache._cache}")
    print(f"  Address _name_to_ids match: {dict(processor.address_cache._name_to_ids) == dict(processor_batch.address_cache._name_to_ids)}")
    print(f"  Token _id_to_symbol match: {processor.token_cache._id_to_symbol == processor_batch.token_cache._id_to_symbol}")
    print(f"  Token _symbol_to_ids match: {dict(processor.token_cache._symbol_to_ids) == dict(processor_batch.token_cache._symbol_to_ids)}")
    assert processor.address_cache._cache == processor_batch.address_cache._cache
    assert processor.token_cache._symbol_to_ids == processor_batch.token_cache._symbol_to_ids

    print("\n--- Тестирование DataProcessor Завершено ---")

if __name__ == "__main__":
    test_data_processor() 