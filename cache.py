BASE = {
    "CACHE_DEFAULT_TIMEOUT": 300
}

SIMPLE = {
    **BASE,
    'CACHE_TYPE': 'SimpleCache',
}

FILESYSTEM = {
    **BASE,
    'CACHE_TYPE': 'FileSystemCache',
    'CACHE_DIR': './cache',
}

