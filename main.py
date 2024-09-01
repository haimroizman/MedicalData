import asyncio
import logging
import aiohttp
from data_fetcher import DataFetcher
from database import Database

logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# async def view_database_contents(db):
#     tables = ['patients', 'encounters', 'conditions']
#     for table in tables:
#         columns, rows = await db.get_table_contents(table)
#         logger.info(f"\n{table.upper()} TABLE:")
#         logger.info(f"Columns: {columns}")
#         for i, row in enumerate(rows):
#             logger.info(row)


async def main():
    logger.debug("Starting main function")
    db = Database()
    data_fetcher = DataFetcher(db)

    fetch_task = asyncio.create_task(data_fetcher.start_periodic_fetch())

    try:
        # Run for a specific duration (e.g., 4 hours) or indefinitely
        await asyncio.sleep(4 * 3600)  # Run for 4 hours
        # To run indefinitely, you can use: await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    finally:
        # Cancel the fetch task
        fetch_task.cancel()
        try:
            await fetch_task
        except asyncio.CancelledError:
            logger.info("Fetch task cancelled")

        # View database contents after the run
    # await view_database_contents(db)

    logger.info("Medical data processing system shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())