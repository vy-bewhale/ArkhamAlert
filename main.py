import logging
import os
import pandas as pd
from dotenv import load_dotenv
from arkham.arkham_monitor import ArkhamMonitor

# Загрузка переменных окружения
load_dotenv()
API_KEY = os.getenv("ARKHAM_API_KEY")

# Настройка логирования (опционально, для отладки)
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Шаг 1: Инициализация (Упрощенная) ---
print("Шаг 1: Инициализация монитора...")
# Создаем монитор, передавая только API ключ. 
# Остальные компоненты (кеши, клиент, процессор, фильтр) создадутся автоматически внутри.
monitor = ArkhamMonitor(api_key=API_KEY)
print("Монитор инициализирован.")
# --- Конец Шага 1 ---

# --- Шаги 2+3: Установка базовых фильтров и получение транзакций ---
new_min_usd = 50000 * 100
new_lookback = '24h'
print(f"\nШаги 2+3: Установка базовых фильтров (USD >= {new_min_usd}, Lookback = {new_lookback}) и получение транзакций...")
monitor.set_filters(min_usd=new_min_usd, lookback=new_lookback)
# Запрашиваем до 1000 транзакций для наполнения кеша
df_basic = monitor.get_transactions(limit=1000)

print(f"Получено {len(df_basic)} транзакций от API.") # Выводим сколько реально получено

if not df_basic.empty:
    print(f"Отображаем первые {min(len(df_basic), 10)} транзакций:") # Уточняем вывод
    pd.set_option('display.width', 120)
    # Отображаем только первые 10 строк
    print(df_basic.head(10).to_string(index=False))
else:
    print("Транзакций по базовым фильтрам не найдено.")
# --- Конец Шагов 2+3 ---

# --- Шаг 4: Статистика кеша по элементам ---
print("\nШаг 4: Статистика кеша (количество ID на имя/символ)...")

# Адреса/Сущности
print("  Количество ID для каждого имени адреса/сущности:")
all_known_names = monitor.address_cache.get_all_names()
for name in sorted(all_known_names):
    ids_count = len(monitor.address_cache.get_identifiers_by_name(name))
    if ids_count > 0:
        print(f"    - {name}: {ids_count} ID")

# Токены
print("\n  Количество ID (контрактов/цепочек) для каждого символа токена:")
token_map = monitor.get_token_symbol_map()
for symbol in sorted(token_map.keys()):
    ids_count = len(token_map.get(symbol, set()))
    if ids_count > 0:
        print(f"    - {symbol}: {ids_count} ID")
# --- Конец Шага 4 ---

# --- Шаг 5: Демонстрация динамических фильтров (Все имена Cex/Dex) ---
print("\nШаг 5: Демонстрация динамических фильтров (Все имена Cex/Dex)...")

# Используем широкие базовые фильтры для демонстрации
demo_min_usd = 50000 * 100
demo_lookback = '24h'
print(f"(Для Шага 5 используем базовые фильтры: USD >= {demo_min_usd}, Lookback = {demo_lookback})")

# --- Демо 1: Фильтр "В Cex" (Все имена, BTC, USDT, USDC) ---
print("\n--- Демо 1: Фильтр \"В Cex\" (Все имена, BTC, USDT, USDC) ---")

cex_names = [name for name in all_known_names if "Cex" in name]
target_tokens_cex = ['BTC', 'USDT', 'USDC'] # Используем указанные токены

if cex_names:
    print(f"Найдено {len(cex_names)} имен с \"Cex\". Используем все для фильтра.")
    print(f"Устанавливаем фильтр: Токены={target_tokens_cex}, Куда В=[{len(cex_names)} имен Cex]...")

    monitor.set_filters(
        min_usd=demo_min_usd,
        lookback=demo_lookback,
        token_symbols=target_tokens_cex, # Фильтр по токенам
        to_address_names=cex_names # Используем ВСЕ имена Cex
    )

    # Выводим параметры перед запросом
    api_params_cex = monitor.filter.get_api_params(limit=100)
    print(f"Параметры запроса API (Демо 1): {api_params_cex}")

    # Запрашиваем до 100 транзакций
    df_cex = monitor.get_transactions(limit=100)
    print(f"Получено {len(df_cex)} транзакций от API.")

    if not df_cex.empty:
        print(f"Отображаем первые {min(len(df_cex), 10)} транзакций \"В Cex\":")
        print(df_cex.head(10).to_string(index=False))
    else:
        print(f"Транзакций \"В Cex\" с токенами {target_tokens_cex} не найдено (или произошла ошибка API)." )
else:
    print("Имена с \"Cex\" в кеше не найдены, Демо 1 пропущено.")

# --- Демо 2: Фильтр "Из Dex" (Все имена) ---
print("\n--- Демо 2: Фильтр \"Из Dex\" (Все имена) ---")

dex_names = [name for name in all_known_names if "Dex" in name]

if dex_names:
    print(f"Найдено {len(dex_names)} имен с \"Dex\". Используем все для фильтра.")
    print(f"Устанавливаем фильтр: Откуда Из=[{len(dex_names)} имен Dex]...")

    monitor.set_filters(
        min_usd=demo_min_usd,
        lookback=demo_lookback,
        from_address_names=dex_names # Используем ВСЕ имена Dex
        # Без фильтра по токенам
    )

    # Выводим параметры перед запросом
    api_params_dex = monitor.filter.get_api_params(limit=100)
    print(f"Параметры запроса API (Демо 2): {api_params_dex}")

    # Запрашиваем до 100 транзакций
    df_dex = monitor.get_transactions(limit=100)
    print(f"Получено {len(df_dex)} транзакций от API.")

    if not df_dex.empty:
        print(f"Отображаем первые {min(len(df_dex), 10)} транзакций \"Из Dex\":")
        print(df_dex.head(10).to_string(index=False))
    else:
        print(f"Транзакций \"Из Dex\" не найдено (или произошла ошибка API)." )
else:
    print("Имена с \"Dex\" в кеше не найдены, Демо 2 пропущено.")
# --- Конец Шага 5 ---

print("\nСкрипт завершен.")