from datetime import datetime
from subprocess import run
from numpy import product
from conn import call_products
from invoices import call_invoices
from tickets import call_tickets
import asyncio
import time
import schedule
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import os

async def chain():
    start = time.perf_counter()
    products_task = asyncio.create_task(call_products())
    invoice_task = asyncio.create_task(call_invoices())
    tickets_task = asyncio.create_task(call_tickets())

    await products_task
    await invoice_task
    await tickets_task
    time.sleep(10)
    end = time.perf_counter()-start
    print (f"chained result took {end:0.2f} seconds")



if __name__ == "__main__":
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(chain,'interval',seconds=10)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt,SystemExit):
        
        pass
    
