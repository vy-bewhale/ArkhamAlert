from .cache import AddressCache, TokenCache
from .config import get_logger
# import logging # <--- Удаляем или комментируем

logger = get_logger(__name__)
# logger.setLevel(logging.DEBUG) # <--- Удаляем или комментируем

DEFAULT_LIMIT = 100 # Default limit for API requests

class TransactionFilter:
    """Applies filtering criteria to processed transactions."""
    def __init__(self, address_cache: AddressCache, token_cache: TokenCache):
        self.address_cache = address_cache
        self.token_cache = token_cache
        
        # Initialize with no filters active
        self.min_usd: float | None = None
        self.lookback: str | None = None # e.g., '1d', '6h'
        self.token_symbols: list[str] | None = None
        self.from_address_names: list[str] | None = None
        self.to_address_names: list[str] | None = None
        
        # Store resolved IDs for efficiency during matching
        self._allowed_token_ids: set[str] | None = None
        self._allowed_from_ids: set[str] | None = None
        self._allowed_to_ids: set[str] | None = None
        
    def update(
        self,
        min_usd: float | None = None,
        lookback: str | None = None,
        token_symbols: list[str] | None = None,
        from_address_names: list[str] | None = None,
        to_address_names: list[str] | None = None
    ):
        """Updates the filter criteria and resolves corresponding IDs."""
        self.min_usd = min_usd
        self.lookback = lookback
        self.token_symbols = token_symbols
        self.from_address_names = from_address_names
        self.to_address_names = to_address_names
        
        # Resolve IDs based on the new criteria
        self._resolve_filter_ids()
        logger.info(f"Фильтры обновлены: USD>={min_usd}, Lookback={lookback}, Tokens={token_symbols}, From={from_address_names}, To={to_address_names}")

    def _resolve_filter_ids(self):
        """Resolves names/symbols to sets of IDs using the caches."""
        self._allowed_token_ids = self.token_cache.find_ids_by_symbols(self.token_symbols) if self.token_symbols else None
        self._allowed_from_ids = self.address_cache.find_identifiers_by_names(self.from_address_names) if self.from_address_names else None
        self._allowed_to_ids = self.address_cache.find_identifiers_by_names(self.to_address_names) if self.to_address_names else None

    def get_api_params(self, limit: int = DEFAULT_LIMIT) -> dict:
        """Constructs parameters suitable for the initial API request."""
        params = {'limit': limit}
        if self.lookback:
            params['timeLast'] = self.lookback
        if self.min_usd is not None:
            # API expects integer or string representation of number
            params['usdGte'] = int(self.min_usd) if self.min_usd == int(self.min_usd) else str(self.min_usd)
            
        # --- Attempt to add 'tokens' parameter if IDs are already known --- 
        # This is an optimization: if we filter by specific symbols AND we already know their IDs,
        # we can ask the API to pre-filter. If IDs are not known yet, we filter post-fetch.
        if self._allowed_token_ids is not None: # Check if token filter is active and resolved
            if self._allowed_token_ids: # Check if any IDs were actually found
                 # API expects lowercase, comma-separated string
                params['tokens'] = ",".join(sorted(list(self._allowed_token_ids))).lower()
                logger.debug(f"Добавляем параметр API 'tokens': {params['tokens']}")
            else:
                # If symbols were specified but no IDs found, prevent API call from returning anything
                # by setting a non-existent token ID (or handle differently). Or just let post-filtering handle it.
                # Let's rely on post-filtering for simplicity if no IDs resolved.
                logger.debug("Фильтр по токенам активен, но ID не найдены в кеше. Фильтрация будет после получения.")
        # ---------------------------------------------------------------------
        
        # Note: Filtering by from/to names happens *after* fetching, so they aren't API params.
        # --- Attempt to add 'from' and 'to' parameters if IDs are known --- 
        # Используем from/to вместо fromAddresses/toAddresses для строгой фильтрации
        if self._allowed_from_ids is not None:
            if self._allowed_from_ids:
                # Используем 'from' вместо 'fromAddresses'
                from_str = ",".join(sorted(list(self._allowed_from_ids)))
                params['from'] = from_str
            else:
                # Этот случай означает: from_address_names были указаны, но не разрешились ни в какие ID.
                logger.debug("Фильтр по 'Откуда' активен, но ID для указанных имен не найдены в кеше.")
                
        if self._allowed_to_ids is not None:
            if self._allowed_to_ids:
                # Используем 'to' вместо 'toAddresses'
                to_str = ",".join(sorted(list(self._allowed_to_ids)))
                params['to'] = to_str
            else:
                # Этот случай означает: to_address_names были указаны, но не разрешились ни в какие ID.
                logger.debug("Фильтр по 'Куда' активен, но ID для указанных имен не найдены в кеше.")
        # ------------------------------------------------------------------
        
        return params

    def matches(self, processed_tx: dict) -> bool:
        """Checks if a processed transaction matches the current filter criteria.
           NOTE: This method is no longer actively used for filtering as filtering is delegated 
           to the API via get_api_params. It remains for potential future use or debugging.
        """
        # Since filtering is now primarily done via API parameters constructed 
        # in get_api_params based on user selections from cached data, 
        # this local matching logic is disabled by default.
        return True # Assume anything received from API (after its filtering) is a match
        
        # --- Старая логика локальной проверки (закомментирована) ---
        # if not processed_tx:
        #     return False
        # ... (остальная логика проверки USD, токенов, адресов) ...
        # return True

        # 1. Check USD value
        if self.min_usd is not None:
            usd_numeric = processed_tx.get('USD_numeric')
            if usd_numeric is None or float(usd_numeric) < self.min_usd:
                return False

        # 2. Check Token Symbol (using resolved IDs)
        # If _allowed_token_ids is None, the filter is inactive -> match
        # If _allowed_token_ids is empty set, filter is active but no match possible -> no match
        # If _allowed_token_ids has IDs, check if tx token ID is present -> match if present
        if self._allowed_token_ids is not None:
            if not self._allowed_token_ids: # Empty set means filter is active but nothing matches
                return False 
            tx_token_id = processed_tx.get('_token_id')
            if tx_token_id is None or tx_token_id not in self._allowed_token_ids:
                return False

        # 3. Check From Address Name (using resolved IDs)
        if self._allowed_from_ids is not None:
            if not self._allowed_from_ids:
                 return False
            tx_from_id = processed_tx.get('_from_identifier')
            if tx_from_id is None or tx_from_id not in self._allowed_from_ids:
                return False
                
        # 4. Check To Address Name (using resolved IDs)
        if self._allowed_to_ids is not None:
            if not self._allowed_to_ids:
                 return False
            tx_to_id = processed_tx.get('_to_identifier')
            if tx_to_id is None or tx_to_id not in self._allowed_to_ids:
                return False

        # If all checks passed
        return True 