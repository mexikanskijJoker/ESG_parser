import asyncio
import csv
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
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CSV_HEADERS = ["Заголовок", "Компания", "Дата публикации", "Текст"]
CURRENT_DATE = time.strftime("%d.%m.%Y", time.gmtime(time.time()))
FILENAME = "rbc_data.csv"
RBC_URL = "https://www.rbc.ru/tags/?tag=%D0%A1%D0%B1%D0%B5%D1%80%D0%B1%D0%B0%D0%BD%D0%BA&dateFrom=01.03.2012&dateTo=01.11.2015&project=rbcnews"
TARGET_DATE = "20 мар 2012,"


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


class UrlsCollector:
    def __init__(self, url: str) -> None:
        self.url = url

    def collect_urls(self) -> list[str]:
        article_urls = []

        with WebDriver() as driver:
            driver.get(self.url)
            driver.maximize_window()

            scroll_complete = False
            try:
                while not scroll_complete:
                    self._scroll_to_bottom(driver)
                    try:
                        target_article = WebDriverWait(driver, 0.0).until(
                            EC.text_to_be_present_in_element(
                                (By.CSS_SELECTOR, "body"),
                                TARGET_DATE,
                            )
                        )
                        scroll_complete = True
                    except TimeoutException:
                        continue

                    if target_article:
                        elements = driver.find_elements(
                            By.CLASS_NAME, "search-item.js-search-item"
                        )

                        for element in elements:
                            link = element.find_element(
                                By.CSS_SELECTOR,
                                "a.search-item__link.js-search-item-link",
                            )
                            href = link.get_attribute("href")
                            article_urls.append(href)
                        break

            except KeyboardInterrupt:
                pass

        return article_urls

    def _scroll_to_bottom(self, driver: webdriver.Chrome) -> None:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")


class DataParser:
    def __init__(self) -> None:
        self.session = aiohttp.ClientSession()

    async def parse_data(self, urls: list[str]) -> list[dict[str, str]]:
        parsed_data = []
        last = len(urls)
        async with aiohttp.ClientSession() as session:

            for url in urls:
                await asyncio.sleep(3)
                data = await self._parse_page(session, url)
                parsed_data.append(data)

                print(f"Осталось {last - 1} страниц")
                last -= 1

        return parsed_data

    async def close_session(self) -> None:
        await self.session.close()

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

                if article_text == "":
                    return

                return {
                    "Заголовок": title,
                    "Компания": "Сбербанк",
                    "Дата публикации": publication_date,
                    "Текст": article_text,
                }
        except Exception as e:
            logger.error(f"Error {e}")
            return {}

    def _parse_title(self, html: HTMLParser) -> str:
        try:
            title_element = html.css_first('[itemprop="headline"]')

            if title_element:
                return title_element.text().strip()

        except Exception as e:
            logger.error(f"Error {e}")
            return ""

    def _parse_date(self, html: HTMLParser) -> str:
        try:
            date_element = html.css_first("time")

            if date_element:
                datetime_obj = date_element.attributes.get("datetime")
                parsed_date = datetime.fromisoformat(datetime_obj).strftime("%d.%m.%Y")

                return parsed_date

        except Exception as e:
            logger.error(f"Error {e}")
            return ""

    def _parse_text(self, html: HTMLParser) -> str:
        try:
            article_body = html.css_first(
                'div.article__text.article__text_free[itemprop="articleBody"]'
            )
            article_text = ""

            if article_body:
                paragraphs = article_body.css("p")

                for paragraph in paragraphs:
                    article_text += re.sub(
                        r"\s+|\xa0|\n|\u200b", " ", paragraph.text().strip()
                    )

            return article_text

        except Exception as e:
            logger.error(f"Error {e}")
            return ""


class CSVWriter:
    def __init__(self, filename: str, fieldnames: list[str]) -> None:
        self.filename = filename
        self.fieldnames = fieldnames

    def write_data(self, data: list[dict]) -> None:
        with open(self.filename, mode="a+", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in data:
                if row != None:
                    writer.writerow(row)
                else:
                    logger.warning(f"Пропущена запись из-за None значений: {row}")


async def main():
    url_collector = UrlsCollector(RBC_URL)
    urls = url_collector.collect_urls()
    logger.info("RBC: Сбор URL завершён")

    data_parser = DataParser()
    try:
        parsed_data = await data_parser.parse_data(urls)
        csv_writer = CSVWriter(FILENAME, fieldnames=CSV_HEADERS)
        csv_writer.write_data(parsed_data)
    finally:
        await data_parser.close_session()

    await data_parser.close_session()
