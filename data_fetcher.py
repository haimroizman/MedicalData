import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, db):
        self.db = db
        self.base_url: str = "https://pphp-static.diagnosticrobotics.com/sample/"
        self.endpoints: Dict[str, Tuple[str, str]] = {
            "patient": ("Patient.ndjson", "New_Patient.ndjson"),
            "encounter": ("Encounter.ndjson", "New_Encounter.ndjson"),
            "condition": ("Condition.ndjson", "New_Condition.ndjson")
        }
        self.last_fetch: Dict[str, datetime] = {k: datetime.min for k in self.endpoints}

    async def fetch_data(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        logger.debug(f"Fetching data from {url}")
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.text()
                logger.debug(f"Fetched {len(content)} bytes from {url}")
                return content
        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP error occurred: {e}")
        except aiohttp.ClientError as e:
            logger.error(f"Network error occurred: {e}")
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
        return None

    async def process_data(self, data_type: str, content: Optional[str]) -> None:
        if content is None:
            logger.warning(f"No content to process for {data_type}")
            return

        count = 0
        total_lines = len(content.split('\n'))
        for line in content.split('\n'):
            if line.strip():
                try:
                    item = json.loads(line)
                    if data_type == "patient":
                        await self.db.upsert_patient(item)
                    elif data_type == "encounter":
                        await self.db.upsert_encounter(item)
                    elif data_type == "condition":
                        await self.db.upsert_condition(item)
                    count += 1
                    if count % 1000 == 0:
                        logger.info(f"Processed {count}/{total_lines} {data_type} records")
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in {data_type} data: {line}")
                except Exception as e:
                    logger.error(f"Error processing {data_type} data: {e}")
        logger.info(f"Completed processing {count}/{total_lines} {data_type} records")

    async def fetch_and_process(self, session: aiohttp.ClientSession, data_type: str) -> None:
        main_url = self.base_url + self.endpoints[data_type][0]
        update_url = self.base_url + self.endpoints[data_type][1]

        try:
            if (datetime.now() - self.last_fetch[data_type]) > timedelta(hours=1):
                logger.info(f"Fetching main data for {data_type}")
                content = await self.fetch_data(session, main_url)
                self.last_fetch[data_type] = datetime.now()
            else:
                logger.info(f"Fetching update data for {data_type}")
                content = await self.fetch_data(session, update_url)

            await self.process_data(data_type, content)
        except Exception as e:
            logger.error(f"Error in fetch_and_process for {data_type}: {e}")

    async def start_periodic_fetch(self) -> None:
        while True:
            logger.info("Starting periodic fetch")
            async with aiohttp.ClientSession() as session:
                tasks = [self.fetch_and_process(session, data_type) for data_type in self.endpoints]
                await asyncio.gather(*tasks)
            logger.info("Periodic fetch completed")
            await asyncio.sleep(600)  # Sleep for 10 minutes for allowing to fetch from both man and update urls