#!/usr/bin/env python3
# scripts/time_check.py
"""
Check current local and UTC time using datetime module.
"""

from datetime import datetime, timezone

now_local = datetime.now()
now_utc = datetime.now(timezone.utc)

print("Local time:", now_local)
print("UTC time:  ", now_utc)
