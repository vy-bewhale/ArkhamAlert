import time
import os
import pandas as pd
import threading
from typing import Callable

from dotenv import load_dotenv

from .config import get_logger, ArkhamAPIError, BASE_API_URL
from .arkham_client import ArkhamClient
from .cache import AddressCache, TokenCache
from .data_processor import DataProcessor
from .filter import TransactionFilter

logger = get_logger(__name__)

class ArkhamMonitor:
    """Orchestrates fetching, processing, caching, and filtering Arkham transactions."""

    def __init__(self, 
                 api_key: str | None = None, 
                 api_base_url: str | None = None,
                 address_cache: AddressCache | None = None,
                 token_cache: TokenCache | None = None,
                 arkham_client: ArkhamClient | None = None,
                 data_processor: DataProcessor | None = None,
                 transaction_filter: TransactionFilter | None = None):
        """
        Initializes the monitor components. Dependencies (caches, client, processor, filter) 
        are created automatically if not provided.

        Args:
            api_key: Arkham API key. If None, attempts to load from ARKHAM_API_KEY in .env
            api_base_url: Base URL for Arkham API. Defaults to standard URL or ARKHAM_API_BASE_URL in .env.
            address_cache: Optional AddressCache instance.
            token_cache: Optional TokenCache instance.
            arkham_client: Optional ArkhamClient instance.
            data_processor: Optional DataProcessor instance.
            transaction_filter: Optional TransactionFilter instance.
        """
        load_dotenv()
        effective_api_key = api_key or os.getenv('ARKHAM_API_KEY')
        effective_base_url = api_base_url or os.getenv('ARKHAM_API_BASE_URL', BASE_API_URL)

        if not effective_api_key:
            raise ValueError("Ключ ARKHAM_API_KEY не найден в аргументах или .env файле.")

        # --- Instantiate Components (Create if not provided) ---
        self.address_cache = address_cache or AddressCache()
        self.token_cache = token_cache or TokenCache()
        self.client = arkham_client or ArkhamClient(api_key=effective_api_key, base_url=effective_base_url)
        # Ensure processor and filter use the *same* cache instances
        self.processor = data_processor or DataProcessor(self.address_cache, self.token_cache)
        self.filter = transaction_filter or TransactionFilter(self.address_cache, self.token_cache)
        # -----------------------------------------------------

        self._last_processed_transactions: list[dict] = []
        self._monitor_thread: threading.Thread | None = None
        self._stop_monitor_flag = threading.Event()
        
        logger.info(f"Arkham Monitor инициализирован. API URL: {effective_base_url}")

    def initialize_cache(self, lookback: str = '1d', usd_gte: float = 100000, limit: int = 100) -> bool:
        """Performs an initial fetch to populate the caches.
        
        Args:
            lookback: Time period for initial fetch (e.g., '1d', '7d').
            usd_gte: Minimum USD value for initial fetch.
            limit: Maximum number of transactions for initial fetch.
            
        Returns:
            True if successful, False otherwise.
        """
        logger.info(f"Инициализация кеша: lookback={lookback}, usd_gte={usd_gte}, limit={limit}")
        params = {
            'timeLast': lookback,
            'usdGte': int(usd_gte) if usd_gte == int(usd_gte) else str(usd_gte),
            'limit': limit
        }
        try:
            api_response = self.client.get_transfers(params=params)
            self.processor.process_transactions_response(api_response) # Updates caches internally
            logger.info(f"Кеш инициализирован. Адресов: {len(self.address_cache.get_all_names())}, Токенов: {len(self.token_cache.get_all_symbols())}")
            return True
        except ArkhamAPIError as e:
            logger.error(f"Ошибка API при инициализации кеша: {e}")
            return False
        except Exception as e:
            logger.exception(f"Непредвиденная ошибка при инициализации кеша: {e}")
            return False

    def set_filters(
        self,
        min_usd: float | None = None,
        lookback: str | None = None,
        token_symbols: list[str] | None = None,
        from_address_names: list[str] | None = None,
        to_address_names: list[str] | None = None
    ):
        """Sets the filtering criteria for subsequent get_transactions calls."""
        self.filter.update(
            min_usd=min_usd,
            lookback=lookback,
            token_symbols=token_symbols,
            from_address_names=from_address_names,
            to_address_names=to_address_names
        )

    def get_known_address_names(self) -> list[str]:
        """Returns a sorted list of all known address/entity display names."""
        return self.address_cache.get_all_names()

    def get_known_token_symbols(self) -> list[str]:
        """Returns a sorted list of all known token symbols."""
        return self.token_cache.get_all_symbols()
        
    def get_token_symbol_map(self) -> dict[str, set[str]]:
        """Returns a dictionary mapping token symbols to their known IDs."""
        return self.token_cache.get_symbol_to_ids_map()

    def _fetch_and_process(self, limit: int = 100) -> list[dict]:
        """Internal method to fetch, process, and update caches.
           Returns the list of *all* processed transactions from the fetch.
        """
        api_params = self.filter.get_api_params(limit=limit)
        try:
            logger.debug(f"Запрос транзакций с параметрами: {api_params}")
            api_response = self.client.get_transfers(params=api_params)
            # Process response updates caches via self.processor
            self._last_processed_transactions = self.processor.process_transactions_response(api_response)
            return self._last_processed_transactions
        except ArkhamAPIError as e:
            logger.error(f"Ошибка API при получении транзакций: {e}")
            self._last_processed_transactions = [] # Clear on error
            return []
        except Exception as e:
            logger.exception(f"Непредвиденная ошибка при получении транзакций: {e}")
            self._last_processed_transactions = []
            return []

    def get_transactions(self, limit: int = 100) -> pd.DataFrame:
        """Fetches transactions based on current filters and returns them as a DataFrame.
        
        Args:
            limit: Max number of transactions to fetch from the API *before* filtering.
            
        Returns:
            A pandas DataFrame containing filtered transactions, or an empty DataFrame.
        """
        processed_txs = self._fetch_and_process(limit=limit)
        
        # Apply filters
        # filtered_txs = [tx for tx in processed_txs if self.filter.matches(tx)] # Убрано локальное применение фильтров
        # Логика изменена: полагаемся на фильтрацию API, используем все обработанные транзакции
        filtered_txs = processed_txs 
        
        count = len(filtered_txs)
        logger.info(f"Найдено {count} транзакций после применения фильтров.")
        
        if not filtered_txs:
            return pd.DataFrame() # Return empty DataFrame

        # --- Create DataFrame (select columns for display) ---
        display_columns = ["Время", "Сеть", "Откуда", "Куда", "Символ", "Кол-во", "USD"]
        # Добавляем колонку с хешем транзакции
        if filtered_txs and '_txid' in filtered_txs[0]:
            display_columns.append('TxID') 
            # Переименовываем _txid в TxID для отображения
            for tx in filtered_txs:
                tx['TxID'] = tx.get('_txid', 'N/A')
                
        # Create DF from the list of dicts, selecting only necessary columns
        # Убедимся, что выбираем только существующие колонки, особенно если TxID не добавилась
        actual_columns = [col for col in display_columns if col in filtered_txs[0]] if filtered_txs else []
        df = pd.DataFrame(filtered_txs)[actual_columns]
        return df
        # ------------------------------------------------------

    # --- Background Monitoring --- 
    def _monitoring_loop(self, interval_seconds: int, callback: Callable[[dict], None]):
        """The actual loop running in the background thread."""
        logger.info(f"Запуск фонового мониторинга с интервалом {interval_seconds} сек.")
        processed_ids_in_last_batch = set()

        while not self._stop_monitor_flag.is_set():
            start_time = time.monotonic()
            logger.debug("Цикл мониторинга: получение транзакций...")
            
            # Fetch latest transactions
            current_processed_txs = self._fetch_and_process(limit=self.filter.get_api_params().get('limit', 100))
            newly_processed_ids = {tx.get('_raw_data', {}).get('txid') for tx in current_processed_txs}
            newly_processed_ids.discard(None) # Remove None IDs
            
            new_transactions_count = 0
            if current_processed_txs:
                # Apply filters
                # filtered_txs = [tx for tx in current_processed_txs if self.filter.matches(tx)] # Убрано локальное применение фильтров
                # Полагаемся на фильтрацию API, обрабатываем все полученные транзакции как релевантные (если новые)
                filtered_txs = current_processed_txs 
                
                # Identify transactions not seen in the *previous* exact batch 
                # (simple check, might miss things if tx appears across batches with lookback)
                # A more robust approach would track IDs over time or use timestamps.
                current_batch_filtered_ids = {tx.get('_raw_data', {}).get('txid') for tx in filtered_txs}
                current_batch_filtered_ids.discard(None)
                
                truly_new_ids = current_batch_filtered_ids - processed_ids_in_last_batch
                
                if truly_new_ids:
                    for tx in filtered_txs:
                        txid = tx.get('_raw_data', {}).get('txid')
                        if txid in truly_new_ids:
                             try:
                                 callback(tx) # Call user callback for new, filtered transactions
                                 new_transactions_count += 1
                             except Exception as e:
                                 logger.exception(f"Ошибка в callback-функции мониторинга: {e}")
            
            processed_ids_in_last_batch = newly_processed_ids # Update seen IDs for next iteration
            if new_transactions_count > 0:
                logger.info(f"Обнаружено {new_transactions_count} новых транзакций по фильтрам.")
                
            # Wait for the next interval
            elapsed_time = time.monotonic() - start_time
            wait_time = max(0, interval_seconds - elapsed_time)
            logger.debug(f"Цикл мониторинга завершен за {elapsed_time:.2f} сек. Ожидание {wait_time:.2f} сек.")
            self._stop_monitor_flag.wait(wait_time)
            
        logger.info("Фоновый мониторинг остановлен.")

    def start_background_monitoring(self, interval_seconds: int = 60, callback: Callable[[dict], None] = lambda tx: print(f"New TX: {tx.get('USD')}")):
        """Starts monitoring transactions in a background thread.

        Args:
            interval_seconds: How often to check for new transactions (in seconds).
            callback: A function to call when a new transaction matching filters is found. 
                      The function will receive the processed transaction dictionary.
        """
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.warning("Мониторинг уже запущен.")
            return

        self._stop_monitor_flag.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval_seconds, callback),
            daemon=True # Allows program to exit even if thread is running
        )
        self._monitor_thread.start()

    def stop_background_monitoring(self, timeout: float = 5.0):
        """Stops the background monitoring thread."""
        if not self._monitor_thread or not self._monitor_thread.is_alive():
            logger.info("Мониторинг не запущен.")
            return

        logger.info("Остановка фонового мониторинга...")
        self._stop_monitor_flag.set()
        self._monitor_thread.join(timeout=timeout)
        
        if self._monitor_thread.is_alive():
            logger.warning(f"Поток мониторинга не завершился за {timeout} сек.")
        else:
            logger.info("Поток мониторинга успешно завершен.")
        self._monitor_thread = None

    def get_full_cache_state(self) -> dict:
        """
        Returns a serializable state of all caches (address and token).
        The state can be used later with load_full_cache_state to restore the caches.
        """
        if not self.address_cache or not self.token_cache:
            # This should ideally not happen if __init__ ensures they are created
            logger.warning("Кеши не инициализированы, невозможно получить состояние.")
            return {}
        try:
            return {
                'address_cache': self.address_cache.get_state(),
                'token_cache': self.token_cache.get_state()
            }
        except Exception as e:
            logger.exception(f"Ошибка при получении состояния кешей: {e}")
            return {}

    def load_full_cache_state(self, full_state: dict | None):
        """
        Loads the state of all caches from a previously saved state.
        This will overwrite current cache contents.

        Args:
            full_state: A dictionary containing 'address_cache' and 'token_cache' states,
                        as obtained from get_full_cache_state().
                        If None or empty, caches might be cleared or not modified based on cache implementation.
        """
        if not full_state:
            logger.info("Не предоставлено состояние для загрузки кешей. Кеши не изменены.")
            return

        if not self.address_cache or not self.token_cache:
            logger.error("Экземпляры кешей отсутствуют в мониторе. Невозможно загрузить состояние.")
            return

        address_state = full_state.get('address_cache')
        if address_state is not None: # Check for presence, even if it's an empty dict (for clearing)
            try:
                self.address_cache.load_state(address_state)
                logger.info("Состояние кеша адресов успешно загружено.")
            except Exception as e:
                logger.exception(f"Ошибка при загрузке состояния кеша адресов: {e}")
        else:
            logger.info("Данные для кеша адресов не найдены в предоставленном состоянии.")

        token_state = full_state.get('token_cache')
        if token_state is not None: # Check for presence
            try:
                self.token_cache.load_state(token_state)
                logger.info("Состояние кеша токенов успешно загружено.")
            except Exception as e:
                logger.exception(f"Ошибка при загрузке состояния кеша токенов: {e}")
        else:
            logger.info("Данные для кеша токенов не найдены в предоставленном состоянии.") 