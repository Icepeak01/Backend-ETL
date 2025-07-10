# utils/proxies.py
import os

def get_proxies():
    """
    Returns a list of proxy dicts for requests. 
    Set TP_PROXIES="http://ip1:port,http://ip2:port,…" in your .env
    """
    raw = os.getenv("TP_PROXIES", "")
    proxies = []
    if raw:
        for p in raw.split(","):
            proxies.append({"http": p.strip(), "https": p.strip()})
    return proxies
