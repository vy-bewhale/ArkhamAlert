import logging
import sys

# --- Constants ---
BASE_API_URL = 'https://api.arkhamintelligence.com'
DEFAULT_REQUEST_TIMEOUT = 60

# --- Logging Setup ---
# Keep logging minimal as requested
logging.basicConfig(
    level=logging.WARNING,  # Log only warnings and errors by default
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout  # Output logs to stdout
)

# Reduce noise from libraries like requests
logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_logger(name):
    """Gets a logger instance."""
    return logging.getLogger(name)

# --- Custom Exception ---
class ArkhamError(Exception):
    """Base exception for Arkham Monitor errors."""
    pass

class ArkhamAPIError(ArkhamError):
    """Exception for Arkham API specific errors."""
    pass 