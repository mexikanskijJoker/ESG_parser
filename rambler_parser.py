import asyncio
import csv
import logging
import re
import time
from datetime import datetime

import aiohttp
from selectolax.parser import HTMLParser
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CSV_HEADERS = ["Заголовок", "Компания", "Дата публикации", "Текст"]
FILENAME = "rambler_data.csv"
RAMBLER_URL = "https://finance.rambler.ru/organization/sberbank-rossii/"
TARGET_DATE = "3 декабря 2020"


# Класс - инициализатор браузера Хрома для Selenium
class WebDriver:
    def __init__(self) -> None:
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--window-size=1920x1080")

    def __enter__(self):
        self.driver = webdriver.Chrome(options=self.options)
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()


# Класс - сборщик URL-адресов на странице после прогрузки всей ленты
class UrlsCollector:
    CSS_SELECTOR = "a._1uRkW"
    TARGET_DATE = TARGET_DATE

    def __init__(self, url: str) -> None:
        self.url = url

    def collect_urls(self) -> list[str]:
        urls = []
        with WebDriver() as driver:
            driver.get(self.url)
            driver.maximize_window()

            while not self._is_target_article_present(driver):
                self._scroll_to_bottom(driver)
                self._wait_for_load()
                self._scroll_to_top(driver)

            urls = [
                link.get_attribute("href")
                for link in driver.find_elements(By.CSS_SELECTOR, self.CSS_SELECTOR)
            ]

        return urls

    def _scroll_to_top(self, driver: webdriver.Chrome) -> None:
        driver.execute_script("window.scrollTo(0, -0.05);")

    def _scroll_to_bottom(self, driver: webdriver.Chrome) -> None:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def _wait_for_load(self) -> None:
        time.sleep(1)

    def _is_target_article_present(self, driver: webdriver.Chrome) -> bool:
        try:
            WebDriverWait(driver, 0.0).until(
                EC.text_to_be_present_in_element(
                    (By.CSS_SELECTOR, "body"), self.TARGET_DATE
                )
            )
            return True
        except TimeoutException:
            return False


# Класс - парсер страниц, находящихся по URL-адресам, полученным классом UrlsCollector
class DataParser:
    def __init__(self) -> None:
        self.session = aiohttp.ClientSession()

    async def close_session(self) -> None:
        await self.session.close()

    async def parse_data(self, urls: list[str]) -> list[dict[str, str]]:
        async with aiohttp.ClientSession() as session:
            tasks = [self._parse_page(session, url) for url in urls]
            return await asyncio.gather(*tasks)

    async def _parse_page(
        self, session: aiohttp.ClientSession, url: str
    ) -> dict[str, str]:
        try:
            async with session.get(url) as response:
                html_content = await response.text()
                html = HTMLParser(html_content)

                title = self._parse_title(html)
                publication_date = self._parse_date(html)
                article_text = self._parse_text(html)

                return {
                    "Заголовок": title,
                    "Компания": "Сбербанк",
                    "Дата публикации": publication_date,
                    "Текст": article_text,
                }
        except Exception as e:
            print(f"Error {e}")

            return {}

    def _parse_title(self, html: HTMLParser) -> str:
        try:
            title_element = html.css_first("h1#headline")

            if title_element:
                return title_element.text().replace("\xa0", " ")

        except Exception as e:
            print(f"Ошибка {e}")

            return ""

    def _parse_date(self, html: HTMLParser) -> str:
        try:
            date_element = html.css_first("time")

            if date_element:
                parsed_date = date_element.attributes.get("datetime")
                datetime_obj = datetime.fromisoformat(parsed_date)

                return datetime_obj.strftime("%d.%m.%Y")

        except Exception as e:
            print(f"Ошибка {e}")

            return ""

    def _parse_text(self, html: HTMLParser) -> str:
        try:
            content_block = html.css_first("._2mfTS")
            article_text = ""

            paragraphs = content_block.css("p")
            for paragraph in paragraphs:
                if len(paragraph.text()) > 0:
                    article_text += re.sub(
                        r"\s+|\xa0|\n|\u200b", " ", paragraph.text().strip()
                    )

            return article_text
        except Exception as e:
            print(f"Ошибка {e}")

            return ""


# Класс, выполняющий функцию записи данных в csv файл
class CSVWriter:
    def __init__(self, filename: str, fieldnames: list[str]) -> None:
        self.filename = filename
        self.fieldnames = fieldnames

    def write_data(self, data: list[dict]) -> None:
        with open(self.filename, mode="a+", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in data:
                if all(value is not None for value in row.values()):
                    writer.writerow(row)
                else:
                    print(f"Пропущена запись из-за None значений: {row}")


async def main():
    url_collector = UrlsCollector(RAMBLER_URL)
    urls = url_collector.collect_urls()
    logger.info("RAMBLER: Сбор URL завершён")

    data_parser = DataParser()
    try:
        parsed_data = await data_parser.parse_data(urls)
        csv_writer = CSVWriter(FILENAME, fieldnames=CSV_HEADERS)
        csv_writer.write_data(parsed_data)
    finally:
        await data_parser.close_session()

    await data_parser.close_session()
