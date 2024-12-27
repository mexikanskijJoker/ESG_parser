import asyncio
import csv
import logging
import re

import aiohttp
from selectolax.parser import HTMLParser
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BUTTON_XPATH = '//div[@class="list-more color-btn-second-hover"'
BUTTON = f'{BUTTON_XPATH} and contains(text(), "Еще 20 материалов")]'
CSV_HEADERS = ["Заголовок", "Компания", "Дата публикации", "Текст"]
FILENAME = "ria_data.csv"
RIA_URL = "https://ria.ru/organization_Sberbank_Rossii/"
TARGET_DATE = "3 января 2019"


# Класс - инициализатор браузера Хрома для Selenium
class WebDriver:
    def __init__(self) -> None:
        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--window-size=1920x1080")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")

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
        urls = []

        with WebDriver() as driver:
            driver.get(self.url)
            driver.maximize_window()

            scroll_complete = False
            button_clicked = False
            try:
                while not scroll_complete:
                    self._scroll_to_bottom(driver)

                    if not button_clicked:
                        try:
                            self._click_more_button(driver)
                            button_clicked = True
                        except TimeoutException:
                            continue

                    try:
                        target_article = WebDriverWait(driver, 0.0).until(
                            EC.text_to_be_present_in_element(
                                (By.CSS_SELECTOR, "body"), TARGET_DATE
                            )
                        )
                        scroll_complete = True
                    except TimeoutException:
                        continue

                    if target_article:
                        links = driver.find_elements(
                            By.CSS_SELECTOR, "a.list-item__image"
                        )

                        for link in links:
                            href = link.get_attribute("href")
                            urls.append(href)
                        break

            except KeyboardInterrupt:
                pass

        return urls

    def _scroll_to_bottom(self, driver: webdriver.Chrome) -> None:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def _click_more_button(self, driver: webdriver.Chrome) -> None:
        try:
            more_button = WebDriverWait(driver, 0).until(
                EC.visibility_of_element_located((By.XPATH, BUTTON))
            )
            more_button.click()
        except WebDriverException as e:
            logger.error(f"Возникла ошибка при нажатии кнопки: {e}")


# Класс - парсер страниц, находящихся по URL-адресам, полученным классом UrlsCollector
class DataParser:
    def __init__(self) -> None:
        self.session = aiohttp.ClientSession()

    async def close_session(self) -> None:
        await self.session.close()

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
            logger.error(f"Error {e}")
            return {}

    def _parse_title(self, html: HTMLParser) -> str:
        try:
            title_element = html.css_first(".article__title")

            if title_element:
                return title_element.text().replace("\xa0", " ")

        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return ""

    def _parse_date(self, html: HTMLParser) -> str:
        try:
            date_element = html.css_first(".article__info-valign").text().strip()

            if date_element:
                parsed_date = re.findall(r"\d{2}.\d{2}.\d{4}", date_element)

                return parsed_date[0]

        except Exception as e:
            logger.error(f"Ошибка: {e}")
            return ""

    def _parse_text(self, html: HTMLParser) -> str:
        try:
            content_block = html.css_first(".layout-article__main-over")
            article_text = ""

            paragraphs = content_block.css(".article__text")
            for paragraph in paragraphs:
                if len(paragraph.text()) > 0:
                    article_text += re.sub(r"\s+|\xa0|\n|\u200b", " ", paragraph.text())

            # Откидывается первое предложение текста, тк оно несодержательное
            return article_text.split(".", maxsplit=1)[1].strip()

        except Exception as e:
            logger.error(f"Ошибка: {e}")
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
                    logger.warning(f"Пропущена запись из-за None значений: {row}")


async def main():
    url_collector = UrlsCollector(RIA_URL)
    urls = url_collector.collect_urls()
    logger.info("RIA: Сбор URL завершён")

    data_parser = DataParser()
    try:
        parsed_data = await data_parser.parse_data(urls)
        csv_writer = CSVWriter(FILENAME, fieldnames=CSV_HEADERS)
        csv_writer.write_data(parsed_data)
    finally:
        await data_parser.close_session()
