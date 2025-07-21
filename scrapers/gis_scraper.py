from __future__ import annotations

import re
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional, Union
import pandas as pd
from playwright.sync_api import Page, Download, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError, sync_playwright, BrowserContext
import logging
from contextlib import contextmanager

# Setup logging
logger = logging.getLogger(__name__)

# --- Browser Path from Environment Variable ---
# Set GIS_BROWSER_PATH in your environment to specify the browser executable location.
# Example for Windows PowerShell:
#   $env:GIS_BROWSER_PATH = "C:\\Path\\To\\Browser\\chrome.exe"
# Example for Linux/macOS:
#   export GIS_BROWSER_PATH="/usr/bin/google-chrome"
# BROWSER_PATH = os.environ.get("GIS_BROWSER_PATH")
# if not BROWSER_PATH:
#     raise RuntimeError("GIS_BROWSER_PATH environment variable must be set to the browser executable path.")

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_persistent_context_dir = os.path.abspath(
    os.path.join(_project_root, "organization_files")
)

# Global debug flag, controlled by an environment variable
# If GIS_SCRAPER_DEBUG_PAUSE_ON_ERROR is "true", script will pause on certain errors.
DEBUG_PAUSE_ON_ERROR = os.environ.get("GIS_SCRAPER_DEBUG_PAUSE_ON_ERROR", "False").lower() == "true"

# Removed GisStatistic dataclass - will return dicts

# --- Normalization Functions ---
def normalize_name(raw: str) -> str:
    if raw is None:
        return ""
    name = raw.strip()
    name = re.sub(r"\s+", " ", name) # Consolidate multiple spaces
    name = name.rstrip(",.").strip() # Remove trailing commas/periods
    return name.lower()

def convert_gis_date_format(date_str: str) -> Optional[str]:
    """Converts date string like '3 июня 2025' or 'Сегодня, 15:30' to 'dd.mm.yyyy'."""
    if not date_str:
        return None

    month_mapping = {
        'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
        'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
        'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
    }
    try:
        if "Сегодня" in date_str or "Вчера" in date_str: # Handle relative dates if needed, for now, assume absolute
            # For now, let's return None or the original string if it's not in the expected "day month year" format.
            return None # Or handle appropriately

        parts = date_str.split()
        if len(parts) == 3: # Expected "day month year"
            day = parts[0].zfill(2)
            month_ru = parts[1].lower()
            year = parts[2]
            month_en = month_mapping.get(month_ru)
            if month_en:
                return f"{day}.{month_en}.{year}"
        return None # Return None if format is not as expected
    except Exception as e:
        logger.warning(f"Could not parse date string '{date_str}': {e}")
        return None

# --- Playwright Helper Functions ---
def handle_ads_by_clicking(page: Page) -> bool:
    """
    Tries to close various pop-ups or ad blocks on the page.
    Uses a list of predefined CSS selectors for close buttons.
    """
    close_selectors = [
        "button[aria-label='Close']", "button[aria-label*='close']",
        "button:has-text('Закрыть')", "button:has-text('X')", "span[aria-label='Close']",
        "button:has-text('Закрыть')", "button:has-text('X')", "svg[aria-hidden='true']",
        "div[data-keyword='close']",  # This is the most reliable for your example
        "div[class*='close'][role='button']", "div[class*='popup__close']",
        "div[class*='content__close']", # Corrected from div.content__close
        "div[class*='content__close']",  # Corrected from div.content__close
        "a:has-text('Пропустить')",
        "text=No Thanks", "text=Close",
        "div[class*='modal-close']", "div[data-keyword='close']", "div.wat-kit-image",
        "div.content__close"
    ]

    for sel in close_selectors:
        try:
            page.wait_for_selector(sel, timeout=100) # Short timeout for quick check
            page.locator(sel).click(timeout=200) # Short click timeout
            logger.info(f"Closed pop-up with selector: {sel}")
            return True
        except PlaywrightError: # More specific exception
            continue
    return False

def wait_for_no_overlay(page: Page, selector: str = "div.ECHVcS1o", timeout: int = 10000):
    """
    Waits for an overlay element to disappear.
    """
    try:
        page.wait_for_selector(selector, state="detached", timeout=timeout)
        logger.info(f"Overlay '{selector}' detached.")
    except PlaywrightTimeoutError:
        logger.warning(f"Overlay '{selector}' did not detach within {timeout}ms.")
    except Exception as e:
        logger.error(f"Unexpected error waiting for overlay '{selector}': {e}")

# --- Scraping Core Functions ---
def get_rating_and_reviews(page: Page, digits: str) -> List[Dict[str, Any]]:
        """
        Собирает рейтинг и количество отзывов для компании и/или ее филиалов.

        Логика:
        1. Переходит на страницу отзывов для данной компании.
        2. Проверяет наличие "переключателя филиалов" (branch toggle).
        3. Если филиалы существуют:
           a. Кликает по переключателю, чтобы раскрыть список филиалов.
           b. Получает список ссылок на каждый филиал.
           c. Итерирует по каждой ссылке филиала:
              i. Извлекает название филиала.
              ii. Кликает по ссылке, чтобы перейти на страницу отзывов конкретного филиала.
              iii. Собирает рейтинг и количество отзывов с этой страницы.
              iv. Возвращается на страницу отзывов головной компании и снова раскрывает список филиалов,
                  чтобы сохранить контекст для следующей итерации.
           d. Сохраняет собранные данные (название филиала, рейтинг, отзывы) в список результатов.
        4. Если филиалов нет (переключатель отсутствует):
           a. Собирает рейтинг и количество отзывов непосредственно со страницы головной компании.
           b. Сохраняет эти данные в список результатов.

        Args:
            page (Page): Текущая страница Playwright.
            digits (str): Идентификатор компании (числовая часть из URL).
        Returns:
            List[Dict[str, Union[str, float, int]]]: Список словарей, каждый из которых содержит
            'branch_name' (название филиала), 'rating' (рейтинг) и 'reviews' (количество отзывов).
        """
        results: List[Dict[str, Union[str, float, int]]] = []
        # Переходим на страницу отзывов для текущей компании
        page.goto(f"https://account.2gis.com/orgs/{digits}/reviews")
        handle_ads_by_clicking(page)
        # 1) Обнаружение "переключателя филиалов" (branch toggle)
        page.wait_for_timeout(2000) # Allow page to settle
        try:
            # Ищем элемент, который является переключателем для списка филиалов.
            toggle = page.locator(".mLSzlnkE")
            # Ждем его появления с таймаутом в 3 секунды.
            toggle.wait_for(timeout=3000)
            branches_exist = True
            logger.info(f"Branch toggle detected for company {digits}.")
        except PlaywrightTimeoutError:
            # Если переключатель не появился за 3 секунды, считаем, что филиалов нет.
            branches_exist = False
            logger.info(f"Branch toggle not found for company {digits}.")
        
        if branches_exist:
            # Если филиалы существуют, раскрываем список филиалов, кликнув по переключателю
            try:
                toggle.click()
                # Небольшая задержка, чтобы список успел раскрыться.
                page.wait_for_timeout(200) # Reduced timeout
            except PlaywrightError as e:
                logger.warning(f"Could not click branch toggle for {digits}: {e}")
                branches_exist = False # Если не удалось кликнуть, возможно, список не активен или что-то пошло не так.
        
        if branches_exist:
            # 2) Внутри контейнера <div class="llGvdsTc">, находим все ссылки на филиалы <a class="OHk9PGG3">
            # Локатор ищет ссылки `<a>` с классом `OHk9PGG3`, которые находятся внутри `div._1wkBbEoy`,
            # который, в свою очередь, находится внутри `div.llGvdsTc`.
            page.wait_for_timeout(1000) # Likely redundant due to subsequent .wait_for()
            branch_links = page.locator("div.llGvdsTc div._1wkBbEoy > a.OHk9PGG3")
            # Получаем количество найденных ссылок.
            count = branch_links.count()
            logger.info(f"Found {count} branches for company {digits}.")

            # Итерируем по каждой ссылке филиала. `range(-1, count-1)` позволяет начать с -1,
            # чтобы при `max(i,0)` первый элемент (головная компания, если ее ссылка совпадает с первой ссылкой филиала)
            # был обработан корректно, а затем последовательно все остальные.
            m = re.search(r"/reviews/(\d+)$", page.url)
            is_on_first = m.group(1) in page.url if m else False
            count += 1 if is_on_first else 0
            for i in range(-1, count - 1):
                link = branch_links.nth(max(i, 0))
                # Получаем текущую ссылку. max(i,0) гарантирует, что индекс не будет отрицательным.
                
                # a) Извлекаем необработанное название филиала из текстового содержимого ссылки
                #    and the branch ID from the href
                raw_branch_name_text = link.text_content().strip() if link.text_content() else ""
                norm_branch_text = normalize_name(raw_branch_name_text) # Keep for logging/reference if needed
                
                branch_id_from_href = None
                href = link.get_attribute("href") if not is_on_first else page.url
                if href:
                    match = re.search(r"/reviews/(\d+)$", href)
                    if match:
                        branch_id_from_href = match.group(1)
                
                # Use the extracted ID as the primary identifier for the branch
                # If ID can't be extracted, fall back to normalized text name, or a placeholder
                branch_identifier = branch_id_from_href if branch_id_from_href else norm_branch_text
                if not branch_identifier: # If both are empty
                    branch_identifier = f"unknown_branch_{i}"

                # b) Кликаем по ссылке, чтобы перейти на страницу отзывов этого филиала
                try:
                    if not is_on_first:
                        
                        link.click()
                    
                    # Ждем некоторое время, пока страница загрузится после клика.
                    handle_ads_by_clicking(page)
                except PlaywrightError as e:
                    logger.warning(f"Could not click branch link #{i} ('{raw_branch_name_text}'): {e}")
                    # Если не удалось кликнуть, переходим к следующему филиалу.
                    continue
                
                # c) Собираем рейтинг для текущего филиала
                try:
                    # Локатор для элемента, содержащего рейтинг.
                    page.wait_for_timeout(4000)
                    locator = page.locator(".Hy749fkp")
                    # Ждем его появления.
                    # Извлекаем текстовое содержимое (например, "4.5")
                    rating_text = locator.text_content()
                    # Преобразуем текст в число с плавающей точкой, обрабатывая запятые.
                    rating = float(rating_text.strip().replace(",", ".")) if rating_text else 0.0
                    logger.info(f"Rating for branch '{branch_identifier}': {rating}")
                except PlaywrightError:
                    # Если рейтинг не найден или произошла ошибка, устанавливаем его в 0.0.
                    rating = 0.0
                    logger.warning(f"Could not get rating for branch '{branch_identifier}', set to 0.0.")
                
                # d) Собираем количество отзывов для текущего филиала
                try:
                    # Регулярное выражение для поиска текста, похожего на "123 отзывов"
                    reviews_pattern = re.compile(r"^\d{1,7} (?:отзывов|отзыва|отзыв)$")
                    # Находим первый элемент, соответствующий этому паттерну.
                    rev_locator = page.get_by_text(reviews_pattern).first
                    # Ждем его видимости.
                    rev_locator.wait_for(state="visible", timeout=15000)
                    # Извлекаем числа из найденного текста.
                    match = re.search(r"\d+", rev_locator.text_content() or "")
                    # Преобразуем найденное число в целое, если найдено, иначе 0.
                    reviews = int(match.group()) if match else 0
                    logger.info(f"Reviews for branch '{branch_identifier}': {reviews}")
                except PlaywrightError:
                    # Если количество отзывов не найдено или произошла ошибка, устанавливаем его в 0.
                    reviews = 0
                    logger.warning(f"Could not get reviews for branch '{branch_identifier}', set to 0.")
                
                # Добавляем собранные данные в список результатов.
                results.append({
                    "branch_id": branch_identifier, # Using the extracted ID
                    "rating": rating,
                    "reviews": reviews
                })

                # e) Возвращаемся на страницу отзывов головной компании.
                # Это необходимо, чтобы снова иметь доступ к списку филиалов,
                # так как при клике на филиал URL меняется.
                # Вместо page.goto(f"https://account.2gis.com/orgs/{digits}/reviews")
                # лучше использовать page.go_back() для возврата,
                # но если контекст может быть потерян, goto более надежен.
                # page.goto(f"https://account.2gis.com/orgs/{digits}/reviews")
                page.wait_for_timeout(1000) # Ждем, пока страница загрузится.
                # handle_ads_by_clicking(page) # May not be needed if page.goto is not used

                # f) Снова раскрываем "переключатель филиалов" и обновляем список ссылок.
                # Это критично, потому что после перехода на страницу филиала и возврата,
                # старые локаторы branch_links могут стать "устаревшими" (stale).
                try:
                    # Ищем и кликаем по переключателю.
                    if not is_on_first:
                        page.locator(".mLSzlnkE").click()
                    else:
                        is_on_first = False
                    page.wait_for_timeout(500) # Небольшая задержка.
                    # Заново получаем локаторы для всех ссылок на филиалы.
                    branch_links = page.locator("div.llGvdsTc div._1wkBbEoy > a.OHk9PGG3")
                    logger.info(f"Re-opened branch list for company {digits}.")
                except PlaywrightError:
                    logger.warning(f"Could not re-open branch list for {digits}. Breaking branch loop.")
                    # Если не удается переоткрыть, выходим из цикла по филиалам,
                    # чтобы избежать бесконечных ошибок.
                    break
        
        else:
            # 3) Если филиалов нет (т.е. компания рассматривается как один "филиал" или без филиалов)
            logger.info(f"No branches detected, collecting data for main company {digits}.")
            # Собираем рейтинг для всей компании
            try:
                locator = page.locator(".Hy749fkp")
                locator.wait_for(timeout=5000)
                rating_text = locator.text_content()
                rating = float(rating_text.strip().replace(",", ".")) if rating_text else 0.0
                logger.info(f"Company rating: {rating}")
            except PlaywrightError:
                rating = 0.0
                logger.warning(f"Could not get company rating for {digits}, set to 0.0.")
            
            # Собираем количество отзывов для всей компании
            try:
                reviews_pattern = re.compile(r"^\d{1,7} (?:отзывов|отзыва|отзыв)$")
                rev_locator = page.get_by_text(reviews_pattern).first
                rev_locator.wait_for(state="visible", timeout=15000)
                match = re.search(r"\d+", rev_locator.text_content() or "")
                reviews = int(match.group()) if match else 0
                logger.info(f"Company reviews: {reviews}")
            except PlaywrightError:
                reviews = 0
                logger.warning(f"Could not get company reviews for {digits}, set to 0.")
            
            # Добавляем данные в результаты. branch_name остается пустым,
            # позже scrape_all заполнит его названием компании.
            results.append({
                "branch_id": digits, # For the main company, use the primary digits as its ID
                "rating": rating,
                "reviews": reviews
            })

        return results

def get_reviewss(page: Page, digits: str) -> List[Dict[str, Any]]:
    """
    Scrapes all review details (text, date, sender, rating, source) for a given company.
    """
    results: List[Dict[str, Any]] = []
    page.goto(f"https://account.2gis.com/orgs/{digits}/reviews")
    handle_ads_by_clicking(page)
    page.wait_for_timeout(8000) # Allow page to settle

    # Selector for individual review cards
    review_card_selector = "div.aYDODrXf._9tLQnNX3"

    # Scroll/click "Load More" to load all reviews
    logger.info(f"Scrolling to load all reviews for company {digits}...")
    load_more_button_selector = "button.button__basic-1agAe:has-text('Загрузить ещё')"
    max_attempts = 15 # Max attempts to load more, to prevent infinite loops
    
    for attempt in range(max_attempts):
        initial_review_count = page.locator(review_card_selector).count()
        
        load_more_button = page.locator(load_more_button_selector)
        if load_more_button.is_visible(timeout=1500):
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: Clicking 'Загрузить ещё' button.")
            try:
                load_more_button.click(timeout=5000)
                page.wait_for_timeout(3000) # Wait for content to load
            except PlaywrightError as e:
                logger.warning(f"Could not click 'Загрузить ещё' button or timed out: {e}. Trying to scroll.")
                page.keyboard.press("End")
                page.wait_for_timeout(2500)
        else:
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: Load more button not visible, scrolling to bottom.")
            page.keyboard.press("End")
            page.wait_for_timeout(2500)

        current_review_count = page.locator(review_card_selector).count()
        if current_review_count == initial_review_count:
            logger.info("No new reviews loaded. Assuming all are loaded.")
            break
        if attempt == max_attempts - 1:
            logger.warning("Reached max attempts to load more reviews.")
            
    review_cards = page.locator(review_card_selector).all()
    logger.info(f"Found {len(review_cards)} review cards for company {digits}.")

    for card in review_cards:
        review_data = {}
        try:
            review_data["sender_name"] = card.locator(".DaMPj2-X").first.text_content(timeout=500).strip()
            
            raw_date = card.locator(".XRSXmsMZ").first.text_content(timeout=500).strip()
            review_data["date"] = convert_gis_date_format(raw_date)
            
            review_data["source"] = card.locator(".qyojshn0").text_content(timeout=500).strip()
            
            # More specific selector for the main review text
            # It targets the div._44uMQjyS that is a child of the div with "overflow: hidden"
            main_review_text_element = card.locator("div[style*='overflow: hidden'] > ._44uMQjyS")
            if main_review_text_element.count() > 0:
                review_data["text"] = main_review_text_element.first.text_content(timeout=1000).strip()
            else:
                review_data["text"] = None # Or handle as an error/warning
            # Extract branch address if present
            branch_address_element = card.locator("a.YUUmvmnL")
            if branch_address_element.count() > 0:
                branch_address_text = branch_address_element.text_content(timeout=500).strip()
                review_data["branch_address"] = branch_address_text
                href = branch_address_element.get_attribute("href")
                if href:
                    # Regex to find two numbers in the path, e.g., /orgs/NUMBER1/reviews/NUMBER2
                    match = re.search(r"/orgs/(\d+)/reviews/(\d+)", href)
                    if match:
                        review_data["branch_id"] = match.group(2) # The second captured group is the branch_id
                    else:
                        review_data["branch_id"] = None
            else:
                review_data["branch_address"] = None
                review_data["branch_id"] = None
            
            # Extract rating from style attribute (width of stars)
            try:
                rating_front_element = card.locator(".rating__front-5nKiy")
                style_attr = rating_front_element.get_attribute("style", timeout=500)
                if style_attr:
                    width_match = re.search(r"width:\s*(\d+)px;", style_attr)
                    if width_match:
                        # Assuming 1 star = 18px width (5 stars = 90px)
                        # This might need adjustment based on actual star rendering
                        rating_value = int(width_match.group(1)) / 18.0 
                        review_data["rating"] = round(rating_value, 1)
                    else:
                        review_data["rating"] = None
                else:
                    review_data["rating"] = None
            except PlaywrightError:
                review_data["rating"] = None

            reply_container = card.locator("div._2ppV02M7")
            if reply_container.count() > 0:
                company_reply = {}
                try:
                    company_reply["replier_name"] = reply_container.locator(".DaMPj2-X").text_content(timeout=500).strip()
                    raw_reply_date = reply_container.locator(".XRSXmsMZ").text_content(timeout=500).strip()
                    company_reply["reply_date"] = convert_gis_date_format(raw_reply_date)
                    company_reply["reply_text"] = reply_container.locator("._44uMQjyS").text_content(timeout=1000).strip()
                    review_data["company_reply"] = company_reply
                except PlaywrightError as e:
                    logger.warning(f"Could not extract full company reply details: {e}")
                    review_data["company_reply"] = None
            else:
                review_data["company_reply"] = None

            # Only append if the source is 2GIS
            if "2GIS" in review_data.get("source", ""):
                review_data["scraped_at"] = datetime.now(timezone.utc).isoformat()
                results.append(review_data)
            else:
                logger.info(f"Skipping review from source: {review_data.get('source')}")
        except PlaywrightError as e:
            logger.warning(f"Could not extract full details for a review: {e}")
            # Instead of continuing the outer loop, log the error and potentially append partial data
            # or ensure default values are set for missing fields.
            # For now, we'll let it skip this specific card if a critical error occurs during basic field extraction.
            
    return results

def download_and_process_table(page: Page, digits: int, period: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Загружает XLSX файл со статистикой отображений компании,
        парсит его и извлекает ключевые метрики.

        Args:
            page (Page): Текущая страница Playwright.
            digits (int): Идентификатор компании.
            period (Optional[str]): Период в формате "DD.MM.YYYY-DD.MM.YYYY". 
                                     Если None, используется дефолтный период (последние 30 дней).

        Returns:
            Optional[Dict[str, Any]]: Словарь с данными статистики
            ('company_name', 'total_displays', 'min_position_overall', 'daily_statistics')
            или None в случае ошибки.
        """
        if period:
            try:
                start_date_str, end_date_str = period.split('-')
            except ValueError:
                logger.error(f"Invalid period format: {period}. Expected DD.MM.YYYY-DD.MM.YYYY. Falling back to default.")
                today = datetime.now(timezone.utc)
                start_date_obj = today - pd.Timedelta(days=30)
                start_date_str = start_date_obj.strftime('%d.%m.%Y')
                end_date_str = today.strftime('%d.%m.%Y')
        else:
            today = datetime.now(timezone.utc)
            start_date_obj = today - pd.Timedelta(days=30) # Данные за последние 30 дней
            start_date_str = start_date_obj.strftime('%d.%m.%Y')
            end_date_str = today.strftime('%d.%m.%Y')

        logger.info(f"Attempting to download stats XLSX for company {digits} for period {start_date_str} - {end_date_str}")

        try:
            # Ожидаем начала загрузки файла. Playwright автоматически перехватывает загрузки.
            with page.expect_download(timeout=60000) as dl_info: # Таймаут до 60 секунд на загрузку.
                try:
                    page.goto(f"https://account.2gis.com/orgs/{digits}/statistics/appearance", wait_until="domcontentloaded")
                    handle_ads_by_clicking(page)

                    # 1. Click the date range picker display element
                    logger.info("Clicking on the date range picker...")
                    date_picker_element_selector = "div.datepicker__datepicker-tC947" # Selector for the main date display
                    page.locator(date_picker_element_selector).click()
                    
                    # Wait for the date input fields to become visible
                    start_date_input_selector = "div.Footer__inputs-2BxW1 input#input-1"
                    end_date_input_selector = "div.Footer__inputs-2BxW1 input#input-2"
                    page.wait_for_selector(start_date_input_selector, timeout=10000)
                    
                    # 2. Fill the start date
                    logger.info(f"Filling start date: {start_date_str}")
                    page.locator(start_date_input_selector).clear() # Clear existing value
                    page.locator(start_date_input_selector).fill(start_date_str)
                    
                    # 3. Fill the end date
                    logger.info(f"Filling end date: {end_date_str}")
                    page.locator(end_date_input_selector).clear() # Clear existing value
                    page.locator(end_date_input_selector).fill(end_date_str)

                    # 4. Click the "Apply" button for the date range.
                    # Common texts are "Применить", "Apply", "Готово", "OK". Adjust selector if needed.
                    # Updated to click "Выбрать" button as per new requirement
                    # The previous "Применить" button might have been for an intermediate step or a different UI version.
                    # apply_button_selector = "button:has-text('Применить')" # Old selector
                    apply_button_selector = "button.button__basic-1agAe.button__blue-2kGLR.button__medium-1rtvH:has-text('Выбрать')"
                    # This selector targets the button with specific classes and text "Выбрать".
                    # Using page.get_by_role("button", name="Выбрать").locator(".button__basic-1agAe") could also be an option.

                    logger.info(f"Clicking apply date range button with selector: {apply_button_selector}")
                    page.locator(apply_button_selector).click()
                    page.wait_for_timeout(1000) # Allow time for the date range to apply and UI to update

                    # 5. Click the main export dropdown
                    logger.info("Clicking export dropdown...")
                    # Try clicking the link with text "Экспортировать" first
                    try:
                        export_link_selector = "a.dropdownSelect__title-2m36K"
                        # Locate the original element, then its parent, then click the parent
                        parent_locator = page.locator(export_link_selector).first
                        parent_locator.click(timeout=3000) # Short timeout
                        logger.info(f"Clicked parent of export link with selector: {export_link_selector}")
                    except PlaywrightError:
                        # Fallback to the original dropdown selector if the link is not found or clickable
                        logger.warning("Export link not found or clickable, falling back to dropdown selector.")
                        page.locator(".dropdownSelect__dropdown-1aCQk").click()
                    # 6. Click the "Export to XLSX" option in the dropdown
                    logger.info("Clicking export to XLSX option...")
                    page.locator(".dropdownSelect__option-38Ghe").first.click()

                    logger.info(f"Initiated stats file download for company {digits}.")
                except PlaywrightError as e: # More specific exception
                    logger.warning(f"Navigation or clicks to initiate download failed for {digits}: {e}")
            download: Download = dl_info.value # Получаем объект загруженного файла.
        except PlaywrightError as e: # More specific exception
            logger.error(f"Failed to intercept file download for {digits}: {e}")
            return None # Возвращаем None, если загрузка не состоялась.

        temp_file_path = None # Инициализация переменной для пути временного файла
        try:
            # Создаем временный файл для сохранения загруженного XLSX.
            # `delete=False` позволяет нам вручную управлять удалением файла.
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                # Сохраняем загруженный файл во временное место.
                page.wait_for_timeout(2000)
                download.save_as(tmp.name)
                temp_file_path = tmp.name # Сохраняем путь к временному файлу
            # Читаем XLSX файл с помощью pandas. `header=None` указывает, что у файла нет заголовка,
            # и мы будем обращаться к данным по индексам строк/столбцов.
            df = pd.read_excel(temp_file_path, header=None, engine='openpyxl')
            logger.info(f"XLSX file successfully downloaded and read for company {digits}.")
        except Exception as e:
            logger.error(f"Error processing XLSX file for {digits}: {e}")
            raise  # Reraise critical exception
        finally:
            # Блок finally гарантирует, что временный файл будет удален,
            # даже если произошла ошибка.
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.info(f"Temporary file {temp_file_path} deleted.")
                except ImportError as e:
                    logger.error(f"Error deleting temporary file {temp_file_path}: {e}")
                    raise
                except Exception as e:
                    logger.warning(f"Could not delete temporary file {temp_file_path}: {e}")
                    pass

        # Парсинг данных из DataFrame
        # Извлекаем сырое название компании из ячейки (1, 1) (вторая строка, второй столбец)
        raw_name = df.iloc[1, 1]
        # Извлекаем название компании до первой запятой и убираем пробелы.
        company_name = raw_name.split(",", 1)[0].strip() if isinstance(raw_name, str) else ""
        logger.info(f"Company name from XLSX: '{company_name}'")

        total_displays, all_positions, daily_statistics = 0, [], {}
        # Итерируем по строкам DataFrame, начиная с 6-й строки (индекс 5),
        # так как предыдущие строки содержат метаданные.
        for _, row in df.iloc[5:].iterrows():
            # Извлекаем количество отображений (displays) из 3-го столбца (индекс 2).
            # Проверяем, что значение не NaN и является числом.
            disp = int(row[2]) if pd.notna(row[2]) and str(row[2]).isdigit() else 0

            # Извлекаем позицию из 4-го столбца (индекс 3).
            # Позиция может быть числом или строкой, содержащей число.
            pos_val = None
            if isinstance(row[3], (int, float)): # Если числовой тип
                pos_val = int(row[3])
            elif isinstance(row[3], str) and row[3].strip().isdigit(): # Если строка, содержащая число
                pos_val = int(row[3])

            total_displays += disp # Суммируем общее количество отображений.

            # Обрабатываем дату из 2-го столбца (индекс 1) для ежедневной статистики.
            if pd.notna(row[1]):
                if isinstance(row[1], pd.Timestamp): # Если это объект временной метки Pandas
                    date_key = row[1].strftime("%d.%m.%Y") # Форматируем дату в строку "дд.мм.гггг"
                else:
                    date_key = str(row[1]).strip() # Иначе берем как строку.
                # Сохраняем ежедневную статистику: дата -> [отображения, позиция]
                daily_statistics[date_key] = [disp, pos_val]

            if pos_val is not None:
                all_positions.append(pos_val) # Добавляем позицию в список всех позиций для поиска минимума.

        # Find the last recorded non-null position
        last_position = 0
        for pos in reversed(all_positions):
            if pos is not None:
                last_position = pos
                break

        # Возвращаем словарь с извлеченными данными.
        return {
            "company_name": company_name,
            "total_displays": total_displays,
            "last_recorded_position": last_position,
            "daily_statistics": daily_statistics
        }

# --- Browser Context Manager ---
@contextmanager
def browser_context(headless: bool = False):
    playwright_instance = None
    browser: BrowserContext | None = None
    page: Page | None = None
    try:
        logger.info(f"[browser_context] Starting Playwright (headless={headless})")
        playwright_instance = sync_playwright().start()
        browser_args = ["--start-minimized"]
        browser = playwright_instance.chromium.launch_persistent_context(_persistent_context_dir, headless=headless, args=browser_args)
        page = browser.new_page()
        logger.info("[browser_context] Browser context and page created")
        yield page
    finally:
        if page and not page.is_closed():
            try:
                page.close()
                logger.info("[browser_context] Page closed")
            except Exception as e:
                logger.warning(f"[browser_context] Exception during page close: {e}", exc_info=True)
        if browser and hasattr(browser, 'close'):
            try:
                browser.close()
                logger.info("[browser_context] Browser context closed")
            except Exception as e:
                logger.warning(f"[browser_context] Exception during browser_context close: {e}", exc_info=True)
        if playwright_instance:
            try:
                playwright_instance.stop()
                logger.info("[browser_context] Playwright stopped")
            except Exception as e:
                logger.warning(f"[browser_context] Exception during Playwright stop: {e}", exc_info=True)

# --- Main Functions ---
def get_statistics(job_data: dict) -> dict | None:
    target_id = job_data.get('target_id')
    
    period = job_data.get('period')
    
    headless = job_data.get('headless', False)
    
    if target_id is None:
        logger.error("[get_statistics] 'target_id' is required in job_data")
        raise ValueError("'target_id' is required in job_data for get_statistics.")
    logger.info(f"[get_statistics] Scraping statistics for target_id={target_id}, period={period}")
    
    with browser_context(headless=headless) as page:
        stats = download_and_process_table(page, target_id, period)
        if not stats:
            logger.error(f"[get_statistics] No statistics data found for company ID {target_id}.")
            raise ValueError(f"No statistics data found for company ID {target_id}.")
        logger.info(f"[get_statistics] Scraping complete for target_id={target_id}")
        return stats

def get_reviews_data(job_data: dict) -> list[dict]:
    target_id = job_data.get('target_id')
    
    headless = job_data.get('headless', False)
    
    if target_id is None:
        logger.error("[get_reviews] 'target_id' is required in job_data")
        raise ValueError("'target_id' is required in job_data for get_reviews.")
    logger.info(f"[get_reviews] Scraping reviews for target_id={target_id}")
    
    with browser_context(headless=headless) as page:
        branch_data_list = get_rating_and_reviews(page, str(target_id))
        logger.info(f"[get_reviews] Scraping complete for target_id={target_id}")
        return branch_data_list

def get_reviews(job_data: dict) -> list[dict]:
    target_id = job_data.get('target_id')
    headless = job_data.get('headless', False)
    
    if target_id is None:
        logger.error("[get_reviews_only] 'target_id' is required in job_data")
        raise ValueError("'target_id' is required in job_data for get_reviews_only.")
    logger.info(f"[get_reviews_only] Scraping all reviews for target_id={target_id}")
    
    with browser_context(headless=headless) as page:
        reviews_data = get_reviewss(page, str(target_id))
        logger.info(f"[get_reviews_only] Scraping complete for target_id={target_id}")
        return reviews_data