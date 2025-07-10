# utils/date_utils.py

from datetime import datetime

def parse_trustpilot_date(date_str: str):
    """
    Convert a string like "23 January 2025" into a Python date object.
    """
    try:
        # Define format: day (no leading zero), full month name, full year
        return datetime.strptime(date_str, "%d %B %Y").date()
    except ValueError:
        return None
