import datetime
from decimal import Decimal, ROUND_HALF_UP
import decimal as decimal_module
import hashlib
from .cache import AddressCache, TokenCache
from .config import get_logger

logger = get_logger(__name__)

class DataProcessor:
    """Processes raw transaction data from Arkham API into a structured format."""

    def __init__(self, address_cache: AddressCache, token_cache: TokenCache):
        self.address_cache = address_cache
        self.token_cache = token_cache

    # --- Static Formatting Helpers (mostly unchanged from original) ---
    @staticmethod
    def _format_timestamp(timestamp_str: str | None) -> str:
        """Formats ISO timestamp string to YYYY-MM-DD HH:MM:SS."""
        if not timestamp_str: 
            return "N/A"
        try:
            # Handle potential 'Z' timezone format
            dt_object = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt_object.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            logger.warning(f"Ошибка форматирования времени: {timestamp_str}") 
            return str(timestamp_str)

    @staticmethod
    def _format_value(value: str | int | float | None, decimals: int | None) -> str:
        """Formats a token value considering its decimals."""
        if value is None: 
            return "N/A"
        try:
            value_dec = Decimal(str(value))
            # Note: The original /transfers API doesn't seem to provide decimals directly.
            # If decimals were available (e.g. from token details endpoint), they'd be used here.
            if decimals is not None and decimals >= 0:
                 value_dec = value_dec / (Decimal(10) ** decimals)
            
            # Use original formatting logic for consistency
            is_zero_originally = value_dec.is_zero()
            # Quantize to 6 decimal places for consistent comparison/display
            quantized_value = value_dec.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
            formatted_str = "{:.6f}".format(quantized_value)
            
            # Avoid displaying 0.000000 for very small non-zero numbers
            if formatted_str == '0.000000' and not is_zero_originally: 
                 return '>0' # Or keep 0.000001? Let's use >0 for clarity
                
            # Strip trailing zeros and decimal point if possible
            if '.' in formatted_str:
                 stripped_str = formatted_str.rstrip('0').rstrip('.')
                 return stripped_str if stripped_str else "0"
            
            return formatted_str # Should not happen if quantize worked
        except (ValueError, TypeError, decimal_module.InvalidOperation) as e:
            logger.warning(f"Ошибка форматирования значения {value} с decimals {decimals}: {e}")
            return str(value)

    @staticmethod
    def _format_usd(usd_value: str | float | None) -> str:
        """Formats a USD value to $x,xxx.xx format."""
        if usd_value is None: 
            return "N/A"
        try:
            usd_dec = Decimal(str(usd_value))
            # Quantize to 2 decimal places for currency
            formatted_usd = usd_dec.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            return "${:,.2f}".format(formatted_usd)
        except (ValueError, TypeError, decimal_module.InvalidOperation) as e:
            logger.warning(f"Ошибка форматирования USD {usd_value}: {e}")
            return str(usd_value)

    # --- Static Address/Entity Extraction Helpers --- 
    @staticmethod
    def _extract_address_from_obj(addr_obj: dict | str | None) -> str | None:
        """Extracts the blockchain address string from various Arkham object formats."""
        if not addr_obj:
            return None
        if isinstance(addr_obj, str):
            return addr_obj # Already a string address
        if isinstance(addr_obj, dict):
            # Can be nested: { "address": { "address": "0x..." }} or just { "address": "0x..." }
            inner_addr = addr_obj.get('address')
            if isinstance(inner_addr, str):
                return inner_addr
            elif isinstance(inner_addr, dict):
                return inner_addr.get('address') # Extract from inner dict
        return None # Could not extract

    # --- Core Processing Logic --- 
    def _extract_address_info(self, addr_data: dict | str | None) -> tuple[str | None, str, bool]:
        """Processes address data, generates display name, updates cache, and returns info.
        
        Returns: 
            tuple: (identifier, display_name, is_real_name)
        """
        if not addr_data:
            return None, "N/A", False
        
        address_str = None
        entity_name = None
        label_name = None
        entity_type_display = "" 
        is_real_name = False
        chain_str = None 

        if isinstance(addr_data, dict):
            address_str = self._extract_address_from_obj(addr_data) 
            chain_str = addr_data.get('chain')
            
            entity_data = addr_data.get('arkhamEntity')
            if isinstance(entity_data, dict):
                entity_name = entity_data.get('name')
                entity_type = entity_data.get('type')
                if entity_type: entity_type_display = f" ({entity_type.capitalize()})"
                if entity_name: is_real_name = True
                
            label_data = addr_data.get('arkhamLabel')
            if isinstance(label_data, dict):
                label_name = label_data.get('name')
                if label_name: is_real_name = True 

        elif isinstance(addr_data, str):
            address_str = addr_data
            is_real_name = False
        
        # --- Determine Identifier (ONLY the actual address string) ---
        identifier = address_str # identifier - это ТОЛЬКО фактический адрес

        # Если identifier (т.е. address_str) отсутствует, мы не можем использовать 
        # entity_name или label_name в качестве идентификатора для фильтрации API.
        # display_name все равно будет сформирован ниже для отображения.
        # Вызов self.address_cache.update(None, display_name, ...) не добавит
        # некорректные "ID" в _name_to_ids.
        
        # Эта проверка, по сути, не нужна, если identifier = address_str, 
        # т.к. AddressCache.update() сам обработает None identifier.
        # Но оставим для ясности, что если нет address_str, то identifier = None.
        if not identifier: 
            pass 

        # --- Generate Display Name (логика остается прежней) --- 
        display_name = "N/A"
        if entity_name and label_name:
            display_name = f"{entity_name}{entity_type_display} - {label_name}"
        elif entity_name:
            display_name = f"{entity_name}{entity_type_display}"
        elif label_name:
            display_name = label_name
        elif address_str:
            if len(address_str) > 10:
                 display_name = f"{address_str[:5]}...{address_str[-5:]}"
            else:
                 display_name = address_str
        else: 
             # Эта ветка теперь менее вероятна, если identifier был только address_str.
             # Однако, если entity_name или label_name существуют при отсутствии address_str,
             # они все еще могут сформировать display_name.
             # Если и их нет, display_name останется "N/A" или будет сформирован из identifier, который теперь None.
             # Нужно убедиться, что display_name корректно обрабатывает None identifier.
             # Исходная логика здесь была: display_name = identifier. Если identifier=None, display_name будет None.
             # Это нужно проверить. Лучше явный "N/A", если все остальное None.
             if entity_name: # Повторяем логику, но уже зная, что address_str is None
                 display_name = f"{entity_name}{entity_type_display}"
             elif label_name:
                 display_name = label_name
             # Если display_name все еще "N/A" и identifier (address_str) был None, то так и остается.
        
        # --- Update the cache with the extracted info ---
        self.address_cache.update(identifier, display_name, is_real_name)
        
        return identifier, display_name, is_real_name

    def _extract_token_info(self, tx: dict) -> tuple[str | None, str | None]:
         """Extracts token ID and symbol, updates cache.
         
         Returns:
             tuple: (token_id, token_symbol)
         """
         # Prioritize tokenId, then symbol, then name, fallback to chain
         token_id = tx.get('tokenId')
         symbol = tx.get('tokenSymbol') or tx.get('tokenName')
         chain = tx.get('chain')
         
         # Use chain as fallback ID/Symbol if specific token info is missing
         if not token_id:
             token_id = chain.upper() if chain else "UNKNOWN_CHAIN"
         if not symbol:
             symbol = chain.upper() if chain else "N/A"
             
         # Update cache
         self.token_cache.update(token_id, symbol)
         
         return token_id, symbol

    def process_transaction(self, tx: dict) -> dict | None:
        """Processes a single raw transaction dictionary.
        
        Returns:
            A dictionary with formatted fields and internal identifiers, or None if invalid.
        """
        if not tx or not isinstance(tx, dict):
            logger.warning("Получена невалидная транзакция для обработки.")
            return None

        # --- Extract and Update Cache for Addresses --- 
        from_identifier, from_display, _ = self._extract_address_info(tx.get('fromAddress'))
        # Handle Bitcoin's fromAddresses if fromAddress is missing
        if not from_identifier and isinstance(tx.get('fromAddresses'), list) and tx['fromAddresses']:
             # Take the first one for simplicity, as in original code
             addr_obj = tx['fromAddresses'][0].get('address')
             from_identifier, from_display, _ = self._extract_address_info(addr_obj)

        to_identifier, to_display, _ = self._extract_address_info(tx.get('toAddress'))
        # Handle Bitcoin's toAddresses
        if not to_identifier and isinstance(tx.get('toAddresses'), list) and tx['toAddresses']:
             addr_obj = tx['toAddresses'][0].get('address')
             to_identifier, to_display, _ = self._extract_address_info(addr_obj)

        # --- Extract and Update Cache for Token ---
        token_id, token_symbol = self._extract_token_info(tx)

        # --- Format other fields --- 
        chain = tx.get('chain', 'N/A')
        # Decimals are usually not in /transfers, pass None
        raw_unit_value = tx.get('unitValue') # Получаем сырое значение
        formatted_value = self._format_value(raw_unit_value, None) 
        usd_numeric = tx.get('historicalUSD') # Keep numeric for filtering
        formatted_usd = self._format_usd(usd_numeric)
        block_timestamp = tx.get('blockTimestamp') # Получаем временную метку
        formatted_time = self._format_timestamp(block_timestamp)
        
        # --- Assign or Generate Transaction ID ---
        tx_id = tx.get('txid') or tx.get('transactionHash')
        if not tx_id:
            # Генерируем ID, если официальный отсутствует
            try:
                # Собираем строку из ключевых полей (используем repr для стабильного представления None)
                hash_input = "|".join([
                    repr(block_timestamp),
                    repr(from_identifier),
                    repr(to_identifier),
                    repr(token_id),
                    repr(raw_unit_value), # Используем сырое значение
                    repr(chain)
                ])
                # Вычисляем SHA-256 хеш
                hash_object = hashlib.sha256(hash_input.encode('utf-8'))
                tx_id = f"arkham_client_generated:{hash_object.hexdigest()}"
                logger.debug(f"Сгенерирован ID транзакции: {tx_id} для данных: {hash_input}")
            except Exception as e:
                logger.warning(f"Не удалось сгенерировать ID транзакции: {e}. Используется 'N/A'.")
                tx_id = "N/A" # Возвращаемся к N/A в случае ошибки генерации
        
        # --- Assemble Processed Dictionary --- 
        processed = {
            # Display fields (corresponds somewhat to original DataFrame columns)
            "Время": formatted_time,
            "Сеть": chain,
            "Откуда": from_display,
            "Куда": to_display,
            "Символ": token_symbol,
            "Кол-во": formatted_value,
            "USD": formatted_usd,
            
            # Internal fields for filtering and potential future use
            "_from_identifier": from_identifier,
            "_to_identifier": to_identifier,
            "_token_id": token_id,
            "USD_numeric": usd_numeric,
            "_txid": tx_id, # Теперь содержит либо официальный ID, либо сгенерированный, либо N/A при ошибке генерации
            "_raw_data": tx # Include raw data if needed later
        }
        return processed

    def process_transactions_response(self, api_response: dict | None) -> list[dict]:
        """Processes the full list of transfers from an API response."""
        if not api_response or not isinstance(api_response.get('transfers'), list):
            logger.warning("Ответ API не содержит списка транзакций.")
            return []

        processed_list = []
        for tx in api_response['transfers']:
            processed_tx = self.process_transaction(tx)
            if processed_tx:
                processed_list.append(processed_tx)
        
        count = len(processed_list)
        api_total_count = api_response.get('count', count)
        if count > 0:
            logger.info(f"Обработано {count} транзакций (API count: {api_total_count}).")
        # else: logger doesn't log INFO by default anymore
            
        return processed_list 