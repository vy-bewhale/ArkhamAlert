import os
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
import json
import datetime
from decimal import Decimal, ROUND_HALF_UP
import decimal as decimal_module
import re


class ArkhamMonitor:
    """
    Класс для мониторинга транзакций с использованием Arkham Intelligence API.
    Предоставляет функционал для получения, фильтрации и обработки транзакций.
    """
    
    # Статический кеш, который будет общим для всех экземпляров класса
    _global_address_cache = {}  # {identifier: {'name': display_name, 'type': entity_type, 'is_real': is_real_name}}
    _global_token_cache = {}    # {token_id: token_symbol}
    _global_symbol_to_ids = {}  # {symbol: set(token_ids)}
    
    # Синонимы токенов для поддержки различных вариантов написания
    _token_synonyms = {
        'BTC': 'BITCOIN',
        'BITCOIN': 'BITCOIN',
        'ETH': 'WETH',
        'WETH': 'WETH',
    }
    
    def __init__(self, api_key=None, api_base_url=None, config=None, shared_cache=True):
        """
        Инициализация монитора с API ключом и базовым URL.
        
        Args:
            api_key (str, optional): API ключ Arkham. Если None, будет загружен из .env
            api_base_url (str, optional): Базовый URL API. Если None, будет загружен из .env или использован стандартный
            config (dict, optional): Словарь с дополнительными параметрами конфигурации
            shared_cache (bool, optional): Использовать общий кеш для всех экземпляров класса
        """
        # Настройка логирования
        self._setup_logger()
        
        # Загрузка конфигурации из .env
        load_dotenv()
        
        # Установка API параметров
        self.api_key = api_key or os.getenv('ARKHAM_API_KEY')
        self.api_base_url = api_base_url or os.getenv('ARKHAM_API_BASE_URL', 'https://api.arkhamintelligence.com')
        
        # Проверка наличия ключа API
        if not self.api_key or self.api_key == 'YOUR_API_KEY_HERE':
            self.logger.error("Ключ ARKHAM_API_KEY не найден или некорректен! Выход.")
            raise ValueError("API ключ не найден или некорректен")
        
        # Конфигурация мониторинга по умолчанию
        self.config = {
            'poll_interval_seconds': 60,
            'initial_lookback': '1d',
            'initial_usd_gte': 10_000_000,
            'initial_limit': 100,
            'monitor_lookback': '1d',
            'monitor_usd_gte': 10_000_000,
            'monitor_limit': 100,
            'target_token_symbols': ['BITCOIN', 'USDC'],
            'target_entity_keyword': 'cex'  # Активируем ранее закомментированный фильтр
        }
        
        # Обновление конфигурации, если она предоставлена
        if config:
            self.config.update(config)
        
        # Инициализация локальных или глобальных кешей
        self.shared_cache = shared_cache
        if shared_cache:
            # Используем статические переменные класса как кеш
            self._address_cache = self.__class__._global_address_cache
            self._token_cache = self.__class__._global_token_cache
            self._symbol_to_ids = self.__class__._global_symbol_to_ids
        else:
            # Создаем локальные кеши для этого экземпляра
            self._address_cache = {}
            self._token_cache = {}
            self._symbol_to_ids = {}
        
        # Маппинг CEX адресов для фильтрации
        self._cex_addresses = set()
        
        self.logger.info(f"Инициализация монитора Arkham. API URL: {self.api_base_url}")
    
    def _setup_logger(self):
        """Настройка логгера."""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('ArkhamMonitor')
        logging.getLogger('urllib3').setLevel(logging.WARNING)  # Reduce requests library noise
    
    def _get_transfers(self, params=None):
        """
        Запрос данных о транзакциях из Arkham API.
        
        Args:
            params (dict): Параметры запроса
            
        Returns:
            dict: Данные о транзакциях или None в случае ошибки
        """
        headers = {"API-Key": self.api_key}
        endpoint = f"{self.api_base_url.rstrip('/')}/transfers"
        request_params = params or {}
        
        self.logger.debug(f"Запрос к Arkham API: URL={endpoint}, Params={request_params}")
        
        try:
            response = requests.get(endpoint, headers=headers, params=request_params, timeout=60)
            response.raise_for_status()
            
            self.logger.debug(f"Arkham API ответил статусом {response.status_code}")
            
            try:
                data = response.json()
                count = data.get('count', 'N/A')
                transfers_list = data.get('transfers')
                transfers_count = len(transfers_list) if isinstance(transfers_list, list) else 0
                
                self.logger.debug(f"Получено записей: {count}, transfers в ответе: {transfers_count}")
                return data
            except json.JSONDecodeError as json_err:
                self.logger.error(f"Ошибка декодирования JSON от Arkham API: {json_err}")
                self.logger.error(f"Текст ответа (начало): {response.text[:500]}")
                raise self.ArkhamAPIError(f"JSON Decode Error: {json_err}") from json_err
                
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f"HTTP ошибка при запросе к Arkham API: {http_err}")
            status_code = http_err.response.status_code
            reason = http_err.response.reason
            response_text = "<no text>"
            
            try: 
                response_text = http_err.response.text[:500]
            except Exception: 
                pass
                
            self.logger.error(f"Текст ответа (начало): {response_text}")
            
            if status_code == 401:
                raise self.ArkhamAPIError("Ошибка авторизации (401 Unauthorized). Проверьте API ключ.") from http_err
            elif status_code == 403:
                if 'throttled' in response_text.lower() or 'rate limit' in response_text.lower():
                    raise self.ArkhamAPIError("API Rate Limit Exceeded (403 Forbidden)") from http_err
                raise self.ArkhamAPIError(f"Доступ запрещен (403 Forbidden).") from http_err
            else:
                raise self.ArkhamAPIError(f"HTTP Error: {status_code} {reason}") from http_err
                
        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"Ошибка соединения с Arkham API: {req_err}")
            raise self.ArkhamAPIError(f"Request Failed: {req_err}") from req_err
            
        return None
    
    def _format_timestamp(self, timestamp_str):
        """Форматирует временную метку."""
        if not timestamp_str: 
            return "N/A"
        try:
            dt_object = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt_object.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError): 
            return str(timestamp_str)
    
    def _get_explorer_link(self, address, chain):
        """Получает ссылку на блокчейн-эксплорер для адреса."""
        if not address or not chain: 
            return None
            
        explorers = {
            "ethereum": "https://etherscan.io/address/{}",
            "bsc": "https://bscscan.com/address/{}",
            "polygon": "https://polygonscan.com/address/{}",
            "arbitrum_one": "https://arbiscan.io/address/{}",
            "avalanche": "https://snowtrace.io/address/{}",
            "optimism": "https://optimistic.etherscan.io/address/{}",
            "base": "https://basescan.org/address/{}"
        }
        
        formatter = explorers.get(chain)
        return formatter.format(address) if formatter else None
    
    def _extract_address_from_obj(self, addr_obj):
        """
        Извлекает адрес из объекта адреса.
        
        Args:
            addr_obj: Объект адреса
            
        Returns:
            str: Адрес или None
        """
        if not addr_obj:
            return None
        
        if isinstance(addr_obj, dict):
            # Случай когда адрес представлен объектом с полем address
            if 'address' in addr_obj:
                if isinstance(addr_obj['address'], str):
                    return addr_obj['address']
                elif isinstance(addr_obj['address'], dict) and 'address' in addr_obj['address']:
                    return addr_obj['address']['address']
        elif isinstance(addr_obj, str):
            # Случай когда адрес представлен строкой
            return addr_obj
        
        return None
    
    def _format_address_display(self, addr_data):
        """
        Форматирует данные адреса/сущности для отображения.
        
        Args:
            addr_data: Данные адреса
            
        Returns:
            tuple: (display text, explorer URL, original_identifier, is_real_name)
        """
        if not addr_data:
            return "N/A", None, None, False
        
        display_name = "N/A"
        explorer_url = None
        address_str = None
        chain_str = None
        entity_name = None
        label_name = None
        entity_type_display = ""
        original_identifier = None
        is_real_name = False
        
        # Обрабатываем случай когда addr_data объект с полным описанием
        if isinstance(addr_data, dict):
            # Извлекаем адрес, учитывая возможную вложенность
            address_str = self._extract_address_from_obj(addr_data)
            if not address_str and 'address' in addr_data:
                address_str = self._extract_address_from_obj(addr_data['address'])
            
            chain_str = addr_data.get('chain')
            original_identifier = address_str
            
            # Извлекаем данные arkhamEntity
            entity_data = addr_data.get('arkhamEntity')
            if isinstance(entity_data, dict):
                entity_name = entity_data.get('name')
                entity_type = entity_data.get('type')
                if entity_type and isinstance(entity_type, str):
                    entity_type_display = f" ({entity_type.capitalize()})"
                if entity_name: 
                    is_real_name = True
            
            # Извлекаем данные arkhamLabel
            label_data = addr_data.get('arkhamLabel')
            if isinstance(label_data, dict):
                label_name = label_data.get('name')
                if label_name: 
                    is_real_name = True
            
            # Если нет адреса, комбинируем доступную информацию
            if not address_str:
                if entity_name and label_name: 
                    display_name = f"{entity_name}{entity_type_display} - {label_name}"
                elif entity_name: 
                    display_name = f"{entity_name}{entity_type_display}"
                elif label_name: 
                    display_name = label_name
                else: 
                    display_name = "N/A"; is_real_name = False
                    
                return display_name, None, None, is_real_name
        
        # Обрабатываем случай когда addr_data это строка адреса
        elif isinstance(addr_data, str):
            address_str = addr_data
            original_identifier = address_str
            is_real_name = False
        else:
            return "N/A", None, None, False
        
        # Создаем сокращенную версию адреса
        short_address = "N/A"
        if address_str:
            if len(address_str) > 10: 
                short_address = f"{address_str[:5]}...{address_str[-5:]}"
            else: 
                short_address = address_str
        
        # Определяем финальное отображаемое имя
        if entity_name and label_name:
            display_name = f"{entity_name}{entity_type_display} - {label_name}"
        elif entity_name:
            display_name = f"{entity_name}{entity_type_display}"
        elif label_name:
            display_name = label_name
        else:
            display_name = short_address
            is_real_name = False
        
        # Создаем ссылку на блокчейн-эксплорер
        explorer_url = self._get_explorer_link(address_str, chain_str) if address_str and chain_str else None
        
        return display_name, explorer_url, original_identifier, is_real_name
    
    def _format_value(self, value, decimals):
        """Форматирует значение с учетом десятичных разрядов."""
        if value is None: 
            return "N/A"
            
        try:
            value_dec = Decimal(str(value))
            if decimals is not None and decimals >= 0:
                value_dec = value_dec / (Decimal(10) ** decimals)
                
            is_zero_originally = value_dec.is_zero()
            quantized_value = value_dec.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP)
            formatted_str = "{:.6f}".format(quantized_value)
            
            if formatted_str == '0.000000' and not is_zero_originally: 
                return '0.000001'
                
            if '.' in formatted_str:
                stripped_str = formatted_str.rstrip('0').rstrip('.')
                return stripped_str if stripped_str else "0"
                
            return formatted_str
            
        except (ValueError, TypeError, decimal_module.InvalidOperation) as e:
            self.logger.warning(f"Ошибка форматирования значения {value} с decimals {decimals}: {e}")
            return str(value)
    
    def _format_usd(self, usd_value):
        """Форматирует значение USD."""
        if usd_value is None: 
            return "N/A"
            
        try:
            usd_dec = Decimal(str(usd_value))
            formatted_usd = usd_dec.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            return "${:,.2f}".format(formatted_usd)
        except (ValueError, TypeError, decimal_module.InvalidOperation) as e:
            self.logger.warning(f"Ошибка форматирования USD {usd_value}: {e}")
            return str(usd_value)
    
    def _preprocess_transaction(self, tx):
        """
        Подготавливает данные транзакции для DataFrame и кэша.
        
        Args:
            tx (dict): Данные транзакции
            
        Returns:
            dict: Обработанные данные транзакции или None
        """
        if not tx or not isinstance(tx, dict): 
            return None
        
        value_to_format = tx.get('unitValue')
        decimals_for_value = None  # В API transfers нет decimals
        usd_to_format = tx.get('historicalUSD')
        tx_id_display = tx.get('txid') or tx.get('transactionHash') or 'N/A'
        chain = tx.get('chain', 'N/A')
        
        # --- Обработка From Address/Addresses ---
        from_addr_data_raw = tx.get('fromAddress')
        from_entity_type = None
        from_identifier = None
        from_display = "N/A"
        from_is_real = False
        
        # Проверка наличия fromAddress
        if from_addr_data_raw:
            if isinstance(from_addr_data_raw, dict):
                if isinstance(from_addr_data_raw.get('arkhamEntity'), dict):
                    from_entity_type = from_addr_data_raw['arkhamEntity'].get('type')
            
            # Получаем отображаемый адрес и идентификатор
            from_display, _, from_identifier, from_is_real = self._format_address_display(from_addr_data_raw)
        
        # Если нет fromAddress, но есть fromAddresses (случай Bitcoin)
        elif 'fromAddresses' in tx and isinstance(tx['fromAddresses'], list) and tx['fromAddresses']:
            # Для Bitcoin транзакций можно взять первый адрес для отображения
            # или собрать информацию о всех адресах
            first_from_item = tx['fromAddresses'][0]
            if isinstance(first_from_item, dict):
                # Получаем отображаемый адрес для первого адреса
                addr_obj = first_from_item.get('address')
                if addr_obj:
                    from_display, _, from_identifier, from_is_real = self._format_address_display(addr_obj)
                    
                    # Получаем тип сущности, если есть
                    if isinstance(addr_obj, dict) and isinstance(addr_obj.get('arkhamEntity'), dict):
                        from_entity_type = addr_obj['arkhamEntity'].get('type')
        
        # --- Обработка To Address/Addresses ---
        to_addr_data_raw = tx.get('toAddress')
        to_entity_type = None
        to_identifier = None
        to_display = "N/A"
        to_is_real = False
        
        # Проверка наличия toAddress
        if to_addr_data_raw:
            if isinstance(to_addr_data_raw, dict):
                if isinstance(to_addr_data_raw.get('arkhamEntity'), dict):
                    to_entity_type = to_addr_data_raw['arkhamEntity'].get('type')
            
            # Получаем отображаемый адрес и идентификатор
            to_display, _, to_identifier, to_is_real = self._format_address_display(to_addr_data_raw)
        
        # Если нет toAddress, но есть toAddresses (аналогично fromAddresses)
        elif 'toAddresses' in tx and isinstance(tx['toAddresses'], list) and tx['toAddresses']:
            first_to_item = tx['toAddresses'][0]
            if isinstance(first_to_item, dict):
                addr_obj = first_to_item.get('address')
                if addr_obj:
                    to_display, _, to_identifier, to_is_real = self._format_address_display(addr_obj)
                    
                    # Получаем тип сущности, если есть
                    if isinstance(addr_obj, dict) and isinstance(addr_obj.get('arkhamEntity'), dict):
                        to_entity_type = addr_obj['arkhamEntity'].get('type')
        
        # Получение символа токена и добавление числового значения USD
        token_symbol = tx.get('tokenSymbol') or tx.get('chain', 'N/A').upper()
        usd_string = self._format_usd(usd_to_format)
        usd_numeric = usd_to_format
        
        # Базовые данные для DataFrame
        processed = {
            "Время": self._format_timestamp(tx.get('blockTimestamp')),
            "Сеть": chain,
            "Откуда": from_display,
            "Куда": to_display,
            "Токен ID": tx.get('tokenId') or tx.get('tokenSymbol') or tx.get('tokenName') or chain.upper(),
            "Символ": token_symbol,
            "Кол-во": self._format_value(value_to_format, decimals_for_value),
            "USD": usd_string,
            "USD_numeric": usd_numeric,  # Добавляем числовое значение USD
            "_raw_data": tx,
            "_from_identifier": from_identifier,
            "_to_identifier": to_identifier,
            "_from_entity_type": from_entity_type,
            "_to_entity_type": to_entity_type,
            "_from_is_real_name": from_is_real,
            "_to_is_real_name": to_is_real,
        }
        
        return processed
    
    def _update_caches(self, transactions):
        """
        Обновляет кеши адресов/сущностей и токенов.
        
        Args:
            transactions (list): Список обработанных транзакций
            
        Returns:
            tuple: (new_addresses, new_tokens) - количество новых записей
        """
        new_addresses = 0
        new_tokens = 0
        
        for tx in transactions:
            if not tx: 
                continue
            
            # Обрабатываем "Откуда" и "Куда" отдельно
            for id_key, name_key, type_key, is_real_key in [
                ('_from_identifier', 'Откуда', '_from_entity_type', '_from_is_real_name'),
                ('_to_identifier', 'Куда', '_to_entity_type', '_to_is_real_name')
            ]:
                identifier = tx.get(id_key)
                display_name = tx.get(name_key)
                entity_type = tx.get(type_key)
                is_real = tx.get(is_real_key)
                
                if identifier and identifier != "N/A":
                    name_to_store = display_name if (display_name and display_name != "N/A") else identifier
                    
                    if identifier not in self._address_cache:
                        # Новая запись
                        self._address_cache[identifier] = {'name': name_to_store, 'type': entity_type, 'is_real': is_real}
                        new_addresses += 1
                    else:
                        # Существующая запись - обновляем
                        current_entry = self._address_cache[identifier]
                        updated = False
                        
                        if current_entry.get('name') != name_to_store:
                            current_entry['name'] = name_to_store
                            updated = True
                            
                        # Обновляем тип, если он появился
                        if current_entry.get('type') is None and entity_type is not None:
                            current_entry['type'] = entity_type
                            updated = True
                            
                        # Обновляем флаг is_real, если он стал True
                        if current_entry.get('is_real') is False and is_real is True:
                            current_entry['is_real'] = True
                            updated = True
            
            # Токены
            token_id = tx.get('Токен ID')
            token_symbol = tx.get('Символ')
            
            if token_id and token_id != "N/A" and token_id not in self._token_cache:
                self._token_cache[token_id] = token_symbol if token_symbol else ''
                new_tokens += 1
        
        # Обновляем маппинг символов к ID для фильтрации
        self._update_symbol_to_ids_map(verbose=False)
        
        # Обновляем список адресов CEX для фильтрации
        self._update_cex_addresses()
        
        if new_addresses > 0:
            self.logger.info(f"Добавлено {new_addresses} новых записей в кэш адресов.")
        if new_tokens > 0:
            self.logger.info(f"Добавлено {new_tokens} новых токенов в кэш.")
            
        return new_addresses, new_tokens
    
    def _update_symbol_to_ids_map(self, verbose=False):
        """
        Обновляет маппинг символов токенов к их ID.
        
        Args:
            verbose (bool): Выводить ли подробную информацию о состоянии кеша
        """
        # Очищаем маппинг перед обновлением
        if self.shared_cache:
            # Если используем общий кеш, обновляем статические переменные класса
            self.__class__._global_symbol_to_ids = {}
            token_cache = self.__class__._global_token_cache
            symbol_to_ids = self.__class__._global_symbol_to_ids
        else:
            # Иначе обновляем локальный кеш
            self._symbol_to_ids = {}
            token_cache = self._token_cache
            symbol_to_ids = self._symbol_to_ids
        
        # Заполняем маппинг
        for token_id, symbol in token_cache.items():
            s = symbol if symbol else "N/A"
            if s not in symbol_to_ids:
                symbol_to_ids[s] = set()
            symbol_to_ids[s].add(token_id)
            
            # Обрабатываем синонимы токенов
            if s in self.__class__._token_synonyms:
                normalized = self.__class__._token_synonyms[s]
                if normalized not in symbol_to_ids:
                    symbol_to_ids[normalized] = set()
                symbol_to_ids[normalized].add(token_id)
            
            # Обрабатываем обратные синонимы
            for syn, original in self.__class__._token_synonyms.items():
                if s == original and syn != original:
                    if syn not in symbol_to_ids:
                        symbol_to_ids[syn] = set()
                    symbol_to_ids[syn].add(token_id)
        
        # Отладочный вывод только если verbose=True
        if verbose and self.shared_cache:
            self.logger.info(f"Размер основного кеша токенов: {len(self.__class__._global_token_cache)}")
            self.logger.info(f"Токены в кеше: {list(self.__class__._global_token_cache.values())}")
            self.logger.info(f"Маппинг символ->ID: {self.__class__._global_symbol_to_ids}")
    
    def _update_cex_addresses(self):
        """Обновляет список адресов CEX для фильтрации."""
        self._cex_addresses = set()
        keyword = self.config.get('target_entity_keyword', '')
        
        if not keyword:
            return
            
        keyword = keyword.lower() if keyword else ''
        
        for identifier, entry in self._address_cache.items():
            # Проверяем, не None ли тип
            entity_type = entry.get('type')
            if entity_type is not None:
                entity_type = entity_type.lower()
                # Проверяем содержит ли тип сущности ключевое слово (например, 'cex')
                if keyword and keyword in entity_type:
                    self._cex_addresses.add(identifier)
    
    def _build_token_filter(self):
        """
        Строит фильтр по токенам на основе целевых символов.
        
        Returns:
            str: Строка фильтра для API или None
        """
        target_symbols = self.config.get('target_token_symbols')
        if not target_symbols or not isinstance(target_symbols, list):
            return None
        
        all_target_token_ids = set()
        found_any_id = False
        
        # Получаем доступ к правильному маппингу
        symbol_to_ids = self._symbol_to_ids
        if self.shared_cache:
            symbol_to_ids = self.__class__._global_symbol_to_ids
        
        # Выводим отладочную информацию (только в _build_token_filter, без дублирования)
        self.logger.info(f"Размер кеша токенов в тесте: {len(self._token_cache) if not self.shared_cache else len(self.__class__._global_token_cache)}")
        self.logger.info(f"Маппинг символ->ID: {symbol_to_ids}")
        
        # Нормализуем target_symbols к стандартным именам из _token_synonyms
        normalized_symbols = []
        for sym in target_symbols:
            normalized = self.__class__._token_synonyms.get(sym, sym)
            normalized_symbols.append(normalized)
            
            # Добавим также оригинальный символ, если он отличается от нормализованного
            if normalized != sym:
                normalized_symbols.append(sym)
        
        for target_symbol in normalized_symbols:
            ids_for_target = symbol_to_ids.get(target_symbol)
            if ids_for_target:
                self.logger.info(f"Найдены ID ({len(ids_for_target)} шт) для символа '{target_symbol}': {ids_for_target}")
                all_target_token_ids.update(ids_for_target)
                found_any_id = True
            else:
                self.logger.warning(f"Символ '{target_symbol}' не найден в кэше токенов.")
        
        if found_any_id:
            # Приводим все ID к нижнему регистру перед объединением
            lowercase_ids = [tid.lower() for tid in all_target_token_ids]
            token_filter = ",".join(sorted(list(lowercase_ids)))
            self.logger.info(f"Итоговый фильтр 'tokens' ({len(all_target_token_ids)} ID, lowercase): {token_filter}")
            return token_filter
        
        self.logger.warning(f"Не найдено ID ни для одного из целевых символов: {target_symbols}. Фильтр 'tokens' не будет применен.")
        return None
    
    def _build_cex_filter(self):
        """
        Строит фильтр по CEX на основе кеша адресов.
        
        Returns:
            str: Строка фильтра для API или None
        """
        if not self._cex_addresses:
            return None
            
        # Ограничиваем количество адресов в фильтре, чтобы не превысить лимиты API
        max_addresses = 10  # Можно настроить в зависимости от ограничений API
        addresses_to_use = list(self._cex_addresses)[:max_addresses]
        
        if addresses_to_use:
            cex_filter = ",".join(addresses_to_use)
            self.logger.info(f"Итоговый фильтр 'to' (CEX, {len(addresses_to_use)} адресов): {cex_filter}")
            return cex_filter
            
        return None
    
    def initialize_cache(self):
        """
        Инициализирует кеш первоначальными данными.
        
        Returns:
            bool: True если данные получены успешно, иначе False
        """
        self.logger.info(f"Инициализация кеша по данным API...")
        
        initial_params = {
            'timeLast': self.config.get('initial_lookback'),
            'usdGte': self.config.get('initial_usd_gte'),
            'limit': self.config.get('initial_limit')
        }
        
        try:
            initial_data = self._get_transfers(params=initial_params)
            
            if initial_data and isinstance(initial_data.get('transfers'), list):
                self.logger.info(f"Получено {len(initial_data['transfers'])} транзакций для инициализации.")
                
                # Обработка и обновление кеша (вызов _update_caches уже обновляет маппинг)
                processed_initial = [self._preprocess_transaction(tx) for tx in initial_data['transfers']]
                valid_processed_initial = [tx for tx in processed_initial if tx]
                self._update_caches(valid_processed_initial)
                
                # После обновления кеша покажем информацию один раз
                self._update_symbol_to_ids_map(verbose=True)
                
                self.logger.info(f"Кеш инициализирован. Найдено {len(self._address_cache)} адресов/сущностей и {len(self._token_cache)} токенов.")
                
                # Показываем информацию о токенах только один раз
                token_values = list(self._token_cache.values()) if not self.shared_cache else list(self.__class__._global_token_cache.values())
                self.logger.info(f"Токены в кеше: {token_values}")
                
                return True
                
            else:
                self.logger.warning("Не удалось получить данные для инициализации кеша.")
                
        except self.ArkhamAPIError as e:
            self.logger.error(f"Ошибка API при инициализации кеша: {e}")
        except Exception as e:
            self.logger.exception(f"Непредвиденная ошибка при инициализации кеша: {e}")
            
        return False
    
    def get_transactions(self, use_filters=True):
        """
        Получает текущие транзакции с учетом фильтров.
        
        Args:
            use_filters (bool): Применять ли фильтры
            
        Returns:
            pandas.DataFrame: DataFrame с транзакциями
        """
        monitor_params = {
            'timeLast': self.config.get('monitor_lookback'),
            'usdGte': self.config.get('monitor_usd_gte'),
            'limit': self.config.get('monitor_limit')
        }
        
        if use_filters:
            # Добавляем фильтр по токенам
            token_filter = self._build_token_filter()
            if token_filter:
                monitor_params['tokens'] = token_filter
                
            # Добавляем фильтр по CEX (если активирован в конфигурации)
            cex_filter = self._build_cex_filter()
            if cex_filter:
                monitor_params['to'] = cex_filter
        
        try:
            data = self._get_transfers(params=monitor_params)
            
            if data and isinstance(data.get('transfers'), list):
                transfers = data['transfers']
                api_count = data.get('count', len(transfers))
                self.logger.info(f"Получено {len(transfers)} транзакций (API count: {api_count}). Обработка...")
                
                # Обработка транзакций и обновление кеша
                processed_transfers = [self._preprocess_transaction(tx) for tx in transfers]
                valid_processed_transfers = [tx for tx in processed_transfers if tx]
                
                try:
                    self._update_caches(valid_processed_transfers)
                except Exception as e:
                    self.logger.error(f"Ошибка при обновлении кеша: {e}")
                
                if valid_processed_transfers:
                    df = pd.DataFrame(valid_processed_transfers)
                    df['Details'] = df['_raw_data']  # Добавляем колонку Details
                    return df
                    
            else:
                self.logger.warning("Не удалось получить данные от API или ответ не содержит список 'transfers'.")
                
        except self.ArkhamAPIError as e:
            self.logger.error(f"Ошибка API в запросе транзакций: {e}")
        except Exception as e:
            self.logger.exception(f"Непредвиденная ошибка в запросе транзакций: {e}")
            
        return None
    
    def get_tokens_dataframe(self):
        """
        Возвращает DataFrame с данными о токенах.
        
        Returns:
            pandas.DataFrame: DataFrame с токенами
        """
        token_data = []
        
        for token_id, symbol in self._token_cache.items():
            token_data.append({
                'ID': token_id,
                'Символ': symbol or 'N/A'
            })
        
        if token_data:
            df = pd.DataFrame(token_data)
            # Агрегация по символу
            symbol_counts = df.groupby('Символ').size().reset_index(name='Количество ID')
            return df, symbol_counts
        
        return pd.DataFrame(), pd.DataFrame()
    
    def get_entities_dataframe(self):
        """
        Возвращает DataFrame с данными о сущностях.
        
        Returns:
            pandas.DataFrame: DataFrame с сущностями
        """
        entity_data = []
        
        for identifier, entry in self._address_cache.items():
            entity_data.append({
                'Идентификатор': identifier,
                'Имя': entry.get('name', 'N/A'),
                'Тип': entry.get('type', 'N/A'),
                'Реальное имя': entry.get('is_real', False)
            })
        
        if entity_data:
            df = pd.DataFrame(entity_data)
            # Агрегация по типу
            type_counts = df.groupby('Тип').size().reset_index(name='Количество')
            # Агрегация по флагу реального имени
            real_name_counts = df.groupby('Реальное имя').size().reset_index(name='Количество')
            return df, type_counts, real_name_counts
        
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    def start_monitoring(self, interval=None):
        """
        Запускает цикл мониторинга, который периодически получает новые транзакции.
        
        Args:
            interval (int): Интервал опроса в секундах
            
        Note:
            Блокирующий метод, выполняется до прерывания пользователем.
        """
        if interval is None:
            interval = self.config.get('poll_interval_seconds')
            
        self.logger.info(f"Запуск мониторинга с интервалом {interval} секунд...")
        
        try:
            while True:
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.logger.info(f"[{current_time}] Начинаем цикл обновления...")
                
                df = self.get_transactions(use_filters=True)
                
                if df is not None:
                    # Убираем служебные колонки для отображения
                    display_columns = ["Время", "Сеть", "Откуда", "Куда", "Символ", "Кол-во", "USD"]
                    df_display = df[display_columns] if not df.empty else pd.DataFrame()
                    
                    print("\n" + "=" * 50)
                    print(f"Время: {current_time}")
                    print(f"Найдено транзакций: {len(df_display)}")
                    print("=" * 50)
                    
                    if not df_display.empty:
                        print(df_display.to_string(index=False))
                    else:
                        print("Нет данных по заданным фильтрам.")
                    
                    print("=" * 50 + "\n")
                
                self.logger.info(f"Ожидание {interval} секунд перед следующим обновлением...")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            self.logger.info("Мониторинг остановлен пользователем.")
        except Exception as e:
            self.logger.exception(f"Непредвиденная ошибка в цикле мониторинга: {e}")
    
    # Определение кастомного класса ошибки
    class ArkhamAPIError(Exception):
        """Исключение для ошибок API Arkham."""
        pass