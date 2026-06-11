import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import _adb

async def test():
    client = await _adb()
    try:
        res = await client.table('tenants').select('*').limit(1).execute()
        if res.data:
            print("Columns in tenants:", list(res.data[0].keys()))
        else:
            print("No tenants found.")
    except Exception as e:
        print("Error fetching tenants:", e)

asyncio.run(test())
