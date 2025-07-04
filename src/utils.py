# src/utils.py

import time
from typing import List

def batch_list(items: List[str], batch_size: int):
    """Yield successive n-sized chunks from items."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]

def retry_with_backoff(func, *args, max_attempts=3, backoff=60, **kwargs):
    """Retry a function with exponential backoff."""
    for attempt in range(1, max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt <= max_attempts:
                time.sleep(backoff ** attempt)
            else:
                raise e
