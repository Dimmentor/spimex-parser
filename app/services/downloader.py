import os
import asyncio
import aiohttp
from datetime import date
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup
from ..config import REPORTS_DIR
from ..utils.logger import logger

BASE_URL = "https://spimex.com"
PAGE_URL = "https://spimex.com/markets/oil_products/trades/results/"
LINK_CSS_CLASS = "accordeon-inner__item-title link xls"
PATH_PREFIX = "/upload/reports/oil_xls/"
FILE_EXTENSION = ".xls"
FILENAME_DATE_PREFIX = "oil_xls_"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'


class ReportDownloader:
    def __init__(self):
        self.user_agent = USER_AGENT

    def _get_headers(self) -> dict:
        return {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def _strip_query_string(self, href: str) -> str:
        return href.split("?")[0]

    def _is_valid_href(self, href: str) -> bool:
        return href and PATH_PREFIX in href and href.endswith(FILE_EXTENSION)

    def _extract_date_from_href(self, href: str) -> date:
        try:
            date_str = href.split(FILENAME_DATE_PREFIX)[1][:8]
            return date.fromisoformat(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")
        except (IndexError, ValueError) as e:
            raise ValueError(f"Не удалось извлечь данные из ссылки: {href}") from e

    def _get_absolute_url(self, href: str) -> str:
        return href if href.startswith(('http://', 'https://')) else f"{BASE_URL}{href}"

    def _parse_page_links(self, html: str, start_date: date, end_date: date) -> List[Tuple[str, date]]:
        results = []
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all("a", class_=LINK_CSS_CLASS)

        for link in links:
            href = link.get("href")
            clean_href = self._strip_query_string(href)

            if not self._is_valid_href(clean_href):
                continue

            try:
                file_date = self._extract_date_from_href(clean_href)
            except ValueError as e:
                logger.warning(f"Ошибка при извлечении данных из ссылки: {href}: {e}")
                continue

            if start_date <= file_date <= end_date:
                full_url = self._get_absolute_url(clean_href)
                results.append((full_url, file_date))
            else:
                logger.debug(f"Ссылка {clean_href} вне диапазона дат")

        return results

    async def download_resource(self, url: str, retries: int = 3, delay: float = 1.5) -> Optional[bytes]:
        headers = self._get_headers()
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(url, timeout=30) as response:
                        if response.status == 200:
                            return await response.read()
                        logger.warning(f"HTTP {response.status} for {url}")
            except Exception as e:
                logger.error(f"{attempt + 1} попыток завершились неудачей для {url}: {str(e)}")
                await asyncio.sleep(delay * (attempt + 1))
        logger.error(f"Все попытки завершились неудачей для {url}")
        return None

    async def get_all_bulletins(self, start_date: date, end_date: date) -> List[Tuple[str, date]]:
        page = 1
        all_links = []
        max_pages = 65
        while page <= max_pages:
            url = f"{BASE_URL}/markets/oil_products/trades/results/?page=page-{page}"
            try:
                logger.info(f"Загрузка страницы {page}...")
                html = await self.download_resource(url)
                if not html:
                    logger.warning(f"Не удалось загрузить страницу {page}")
                    break
                links = self._parse_page_links(html.decode('utf-8'), start_date, end_date)
                if not links:
                    logger.info(f"На странице {page} нет подходящих ссылок. Прерывание.")
                    break
                all_links.extend(links)
                logger.info(f"Найдено {len(links)} ссылок на странице {page}")
                page += 1
            except Exception as e:
                logger.error(f"Ошибка при загрузке страницы {page}: {e}")
                break
        logger.info(f"Всего найдено {len(all_links)} ссылок за {page - 1} страниц")
        return all_links

    async def download_and_save(self, url: str, report_date: date, semaphore: asyncio.Semaphore) -> Optional[str]:
        async with semaphore:
            try:
                file_name = f"oil_xls_{report_date.strftime('%Y%m%d')}.xls"
                file_path = os.path.join(REPORTS_DIR, file_name)
                if os.path.exists(file_path):
                    logger.info(f"Файл уже существует: {file_path}")
                    return file_path
                content = await self.download_resource(url)
                if not content:
                    return None
                with open(file_path, 'wb') as f:
                    f.write(content)
                logger.info(f"Успешно сохранен отчёт в {file_path}")
                return file_path
            except Exception as e:
                logger.error(f"Ошибка при загрузке отчёта {url}: {str(e)}")
                return None

    async def get_and_save_reports(self, start_date: date, end_date: date, max_concurrent: int = 10) -> List[str]:
        try:
            reports = await self.get_all_bulletins(start_date, end_date)
            if not reports:
                logger.warning("Не обнаружено отчётов в данном диапазоне дат")
                return []
            os.makedirs(REPORTS_DIR, exist_ok=True)
            semaphore = asyncio.Semaphore(max_concurrent)
            tasks = [
                self.download_and_save(url, report_date, semaphore)
                for url, report_date in reports
            ]
            results = await asyncio.gather(*tasks)
            return [r for r in results if r]
        except Exception as e:
            logger.error(f"Ошибка в методе get_and_save_reports: {str(e)}")
            raise
