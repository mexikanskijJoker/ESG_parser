import asyncio
from datetime import datetime

from rambler_parser import main as rambler
from rbc_parser import main as rbc
from ria_parser import main as ria


async def main():
    tasks = [rambler(), rbc(), ria()]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    start_time = datetime.now()
    print(f"Начало парсинга: {start_time}")
    asyncio.run(main())
    end_time = datetime.now()
    print(f"Завершение парсинга: {end_time}")
