from collections import defaultdict

class AddressCache:
    """Manages caching of address identifiers and their display names."""
    def __init__(self):
        # {identifier: {'name': display_name, 'is_real': is_real_name}}
        self._cache = {}
        # {display_name: set(identifiers)}
        self._name_to_ids = defaultdict(set)

    def update(self, identifier: str | None, display_name: str, is_real_name: bool):
        """Adds or updates an address entry in the cache."""
        if not identifier or identifier == "N/A":
            return
            
        name_to_store = display_name if display_name and display_name != "N/A" else identifier
        
        # Check if entry exists and update logic (simplified for brevity for now)
        # Original code had logic to preferentially update if name became real, etc.
        # For now, just store/update.
        existing_entry = self._cache.get(identifier)
        
        if existing_entry:
             # If name changes, remove old name mapping
            old_name = existing_entry['name']
            if old_name != name_to_store and old_name in self._name_to_ids:
                self._name_to_ids[old_name].discard(identifier)
                if not self._name_to_ids[old_name]: # Remove empty set
                    del self._name_to_ids[old_name]
            
            # Update entry
            existing_entry['name'] = name_to_store
            # Prefer keeping is_real=True if it was ever true
            existing_entry['is_real'] = existing_entry['is_real'] or is_real_name
        else:
            self._cache[identifier] = {'name': name_to_store, 'is_real': is_real_name}
        
        # Update name to id mapping
        self._name_to_ids[name_to_store].add(identifier)

    def get_name(self, identifier: str) -> str | None:
        """Gets the display name for a given identifier."""
        entry = self._cache.get(identifier)
        return entry['name'] if entry else None

    def get_identifiers_by_name(self, display_name: str) -> set[str]:
        """Gets the set of identifiers for a given display name."""
        return self._name_to_ids.get(display_name, set())

    def get_all_names(self) -> list[str]:
        """Returns a sorted list of all unique display names marked as 'real' in the cache."""
        real_names = set()
        for identifier, entry in self._cache.items():
            # Consider a name "real" if the flag is True
            if entry.get('is_real', False):
                 real_names.add(entry.get('name', identifier)) # Fallback to identifier if name somehow missing

        # Additionally, include names from the reverse map that might correspond to real entries
        # This handles cases where multiple identifiers might map to the same real name
        for name, ids in self._name_to_ids.items():
            for identifier in ids:
                 entry = self._cache.get(identifier)
                 if entry and entry.get('is_real', False):
                     real_names.add(name)
                     break # Found one real entry for this name, no need to check other IDs for the same name
                     
        return sorted(list(real_names))

    def find_identifiers_by_names(self, names: list[str]) -> set[str]:
        """Finds all identifiers corresponding to a list of display names."""
        if not names:
            return set() # Return empty set if no names provided
            
        all_ids = set()
        for name in names:
            all_ids.update(self.get_identifiers_by_name(name))
        return all_ids


class TokenCache:
    """Manages caching of token IDs and symbols, including synonyms."""
    # Class-level synonyms, same as original
    _token_synonyms = {
        'BTC': 'BITCOIN',
        'BITCOIN': 'BITCOIN',
        'ETH': 'WETH',
        'WETH': 'WETH',
        # Add more synonyms as needed
    }

    def __init__(self):
        # {token_id: symbol} 
        self._id_to_symbol = {}
        # {normalized_symbol: set(token_ids)}
        self._symbol_to_ids = defaultdict(set)
        self._update_synonyms() # Populate initial synonyms

    def _update_synonyms(self):
        """Helper to ensure synonym map is populated correctly."""
        # Add direct synonyms
        for syn, norm in self._token_synonyms.items():
            if syn != norm: # Avoid self-mapping if listed explicitly
                 # Ensure the normalized form exists if it's a target
                if norm not in self._symbol_to_ids: 
                    self._symbol_to_ids[norm] = set()
    
    def _get_normalized_symbol(self, symbol: str) -> str:
        """Returns the normalized symbol based on synonyms."""
        return self.__class__._token_synonyms.get(symbol.upper(), symbol.upper())

    def update(self, token_id: str | None, symbol: str | None):
        """Adds or updates a token entry in the cache."""
        if not token_id or token_id == "N/A":
            return

        symbol_to_store = symbol.upper() if symbol else "N/A"
        normalized_symbol = self._get_normalized_symbol(symbol_to_store)

        # Store ID -> Symbol mapping
        self._id_to_symbol[token_id] = symbol_to_store

        # --- Update Symbol -> IDs mapping --- 
        # 1. Add to normalized symbol's set
        self._symbol_to_ids[normalized_symbol].add(token_id)
        
        # 2. Add to original symbol's set if different from normalized
        if symbol_to_store != normalized_symbol:
            self._symbol_to_ids[symbol_to_store].add(token_id)
            
        # 3. Add to other synonyms that map to the same normalized form
        for syn, norm in self._token_synonyms.items():
            if norm == normalized_symbol and syn != normalized_symbol and syn != symbol_to_store:
                 self._symbol_to_ids[syn].add(token_id)
        # --- End Update Symbol -> IDs --- 

    def get_symbol(self, token_id: str) -> str | None:
        """Gets the symbol for a given token ID."""
        return self._id_to_symbol.get(token_id)

    def get_ids(self, symbol: str) -> set[str]:
        """Gets the set of token IDs for a given symbol (case-insensitive, uses synonyms)."""
        return self._symbol_to_ids.get(symbol.upper(), set())

    def get_all_symbols(self) -> list[str]:
        """Returns a sorted list of all unique symbols known to the cache (excluding synonyms that don't exist)."""
        # Return only symbols that actually have IDs associated
        return sorted([sym for sym, ids in self._symbol_to_ids.items() if ids])

    def find_ids_by_symbols(self, symbols: list[str]) -> set[str]:
        """Finds all token IDs corresponding to a list of symbols (case-insensitive, uses synonyms)."""
        if not symbols:
            return set() # Return empty set if no symbols provided
            
        all_ids = set()
        for symbol in symbols:
            all_ids.update(self.get_ids(symbol))
        return all_ids

    def get_symbol_to_ids_map(self) -> dict[str, set[str]]:
        """Returns a copy of the mapping from symbols to sets of token IDs."""
        # Return a copy to prevent external modification
        return {sym: ids.copy() for sym, ids in self._symbol_to_ids.items() if ids} 