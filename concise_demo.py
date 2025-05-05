import os
import pandas as pd
from dotenv import load_dotenv
from arkham.arkham_monitor import ArkhamMonitor

# --- 1. Инициализация ---
load_dotenv()
API_KEY = os.getenv("ARKHAM_API_KEY")
monitor = ArkhamMonitor(api_key=API_KEY)

# --- 2. Наполнение кеша ---
# Первоначальный запрос для получения данных и наполнения кешей адресов/токенов
# Используем широкие фильтры (5M USD, 24ч), запрашиваем до 1000 транзакций
monitor.set_filters(min_usd=5000000, lookback='24h')
initial_df = monitor.get_transactions(limit=1000)

# --- 3. Получение списков имен из кеша ---
all_known_names = monitor.address_cache.get_all_names()

# --- 4. Демонстрация фильтра "В CEX" ---
# Находим все имена сущностей, содержащие "Cex"
cex_names = [name for name in all_known_names if "Cex" in name]
target_tokens_cex = ['BTC', 'USDT', 'USDC']

if cex_names:
    # Устанавливаем фильтры: куда = все Cex, токены = BTC/USDT/USDC, 5M USD, 24ч
    monitor.set_filters(
        min_usd=5000000,
        lookback='24h',
        token_symbols=target_tokens_cex,
        to_address_names=cex_names
    )
    # Получаем отфильтрованные транзакции (до 100)
    df_to_cex = monitor.get_transactions(limit=100)
    # Дальнейшая обработка df_to_cex... (здесь можно добавить вывод или анализ)
    # print(f"Найдено транзакций 'В CEX': {len(df_to_cex)}")
    # if not df_to_cex.empty: print(df_to_cex.head())


# --- 5. Демонстрация фильтра "Из DEX" ---
# Находим все имена сущностей, содержащие "Dex"
dex_names = [name for name in all_known_names if "Dex" in name]

if dex_names:
    # Устанавливаем фильтры: откуда = все Dex, 5M USD, 24ч
    monitor.set_filters(
        min_usd=5000000,
        lookback='24h',
        from_address_names=dex_names
    )
    # Получаем отфильтрованные транзакции (до 100)
    df_from_dex = monitor.get_transactions(limit=100)
    # Дальнейшая обработка df_from_dex...
    # print(f"Найдено транзакций 'Из DEX': {len(df_from_dex)}")
    # if not df_from_dex.empty: print(df_from_dex.head())

# Скрипт завершен. Переменные df_to_cex и df_from_dex содержат результаты. 