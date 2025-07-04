from typing import Any, Dict, Tuple, List
from werkzeug.exceptions import HTTPException, NotFound, BadRequest, InternalServerError
from playwright.sync_api import Page, Locator, sync_playwright, BrowserContext
import regex  # Используем модуль regex вместо re
from datetime import datetime
import os
import atexit
import logging
from unidecode import unidecode
import re
# Setup logging
logger = logging.getLogger(__name__)

# --- Browser Path from Environment Variable ---
# Set GIS_BROWSER_PATH in your environment to specify the browser executable location.
# Example for Windows PowerShell:
#   $env:GIS_BROWSER_PATH = "C:\\Path\\To\\Browser\\chrome.exe"
# Example for Linux/macOS:
#   export GIS_BROWSER_PATH="/usr/bin/google-chrome"
BROWSER_PATH = os.environ.get("GIS_BROWSER_PATH")
if not BROWSER_PATH:
    raise RuntimeError("GIS_BROWSER_PATH environment variable must be set to the browser executable path.")

# Define a directory for persistent context.
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_persistent_context_dir = os.path.abspath(
    os.path.join(_project_root, "organizations_files")
)

# Setup logging
logger = logging.getLogger(__name__)

class CaptchaRequired(Exception):
    """Custom exception to indicate a CAPTCHA challenge."""
    def __init__(self, captcha_url: str, message: str = "CAPTCHA challenge encountered. Please solve it at the provided URL."):
        self.captcha_url = captcha_url
        self.message = message
        super().__init__(self.message)

def check_for_captcha(page: Page):
    url = page.url.lower()
    if any(keyword in url for keyword in ("captcha", "showcaptcha", "smartcaptcha")):
        raise CaptchaRequired(page.url)
    if page.locator("iframe[title='SmartCaptcha']").count() > 0:
        raise CaptchaRequired(page.url)
    if page.locator("img[src*='captcha']").count() and page.locator("input[name*='captcha']").count():
        raise CaptchaRequired(page.url)



def locate_target_company(page: Page, id: int) -> Locator:
    id_str = str(id)
    for link in page.locator("a[href][tabindex]").all():
        href = link.get_attribute("href")
        if href and id_str in href:
            return link
    check_for_captcha(page)
    raise NotFound(f"Company link containing ID '{id}' not found on {page.url}.")

def transliterate_cyrillic(text: str) -> str:
    """
    Транслитерирует строку с кириллическими символами в латиницу.
    
    1. Заменяет неразрывный пробел (\xa0) на нижнее подчеркивание.
    2. Выполняет основную транслитерацию с помощью unidecode.
    3. Заменяет все последовательности пробелов на одно нижнее подчеркивание.
    4. Сохраняет все остальные специальные символы, как есть.

    Args:
        text (str): Входная строка с кириллицей.

    Returns:
        str: Транслитерированная строка в латинице, с нижними подчеркиваниями вместо
             пробелов и сохранением спецсимволов.
    """
    if not isinstance(text, str):
        text = str(text)

    # 1. Заменяем неразрывный пробел (\xa0) на нижнее подчеркивание
    # Важно выполнить это до unidecode, так как unidecode может обрабатывать пробелы по-своему.
    text = text.replace('\xa0', '_')

    # 2. Основная транслитерация с помощью unidecode
    transliterated_text = unidecode(text)

    # 3. Заменяем все последовательности пробелов (включая те, что могли остаться после unidecode)
    # на одно нижнее подчеркивание. Спецсимволы здесь не трогаем.
    transliterated_text = re.sub(r'\s+', '_', transliterated_text)

    # 4. Удаляем начальные/конечные подчеркивания, если они появились из-за пробелов,
    # и приводим к нижнему регистру (опционально, но полезно для "slugs")
    transliterated_text = transliterated_text.strip('_').lower()
    
    return transliterated_text



def get_child_texts(page: Page, locator: Locator) -> List[str]:
    handle = locator.element_handle()
    if not handle:
        check_for_captcha(page)
        raise NotFound(f"Element for text extraction not found on {page.url}.")
    texts = handle.evaluate("(el) => { const out=[]; function recurse(n){ if(n.nodeType===3){const t=n.textContent.trim(); if(t) out.push(t);} else if(n.nodeType===1){for(const c of n.childNodes) recurse(c);} } recurse(el); return out; }")
    return texts


def get_all_digits(text: str) -> int | None:
    m = regex.search(r"\d+", text)
    return int(m.group()) if m else None

def get_all_letters(text: str) -> str:
    return ''.join(regex.findall(r'\p{L}', text, regex.U))

# --- Scraper pieces ---
def fill_period(page: Page, period: str | None = None):
    try:
        span = page.get_by_text("Период", exact=True)
        span.wait_for(state="visible", timeout=5000)
    except Exception:
        check_for_captcha(page)
        raise NotFound("Period selector not found.")
    inp = span.locator("..").locator("..").locator("input")
    today = datetime.now().strftime("%d.%m.%Y")
    if period:
        inp.fill(str(period)) # Ensure period is a string
    else:
        inp.fill(f"{today} - {today}")

def check_connection(page: Page):
    while True:
        if "auth" in page.url:
            page.wait_for_timeout(30000)
        else:
            break



def select_grouping(page: Page):
    try:
        span = page.get_by_text("Группировка", exact=True)
        span.wait_for(state="visible", timeout=5000)
    except Exception:
        check_for_captcha(page)
        raise NotFound("Grouping selector not found.")
    btn = span.locator("..").locator("..")
    if btn.evaluate("el=>el.tagName.toLowerCase()") != "button":
        raise BadRequest("Grouping parent is not a button.")
    btn.click()
    page.wait_for_timeout(2000)
    parent_element = page.locator('[data-name="data-menu-item-1"]')
    span_to_click = parent_element.locator("span").get_by_text("По дням", exact=True)
    span_to_click.click()
    page.wait_for_timeout(2000)

def find_and_click_profile_link(page: Page) -> bool:
    try:
        parent_locator = page.locator('div.statistic-nav-view__inner')
        all_a_elements = parent_locator.locator("a")
        count =  all_a_elements.count()
        if count == 0:
            return False
        for i in range(count):
            a_locator = all_a_elements.nth(i)
            text_content = a_locator.text_content()
            if text_content and "профиль" in text_content.lower():
                a_locator.click()
                page.wait_for_timeout(1724)
                return True
            return False
    except:
        pass
# --- Main logic ---
def get_branch_statistics(page: Page, branch_id: int, period: str | None) -> List[Tuple[str, Any]]:
    page.goto(f"https://yandex.ru/business/statistic/company/{branch_id}/audience")
    check_connection(page)
    page.wait_for_timeout(8000)
    if f"https://yandex.ru/business/statistic/company/{branch_id}/audience" not in page.url :
        page.goto(f"https://yandex.ru/business/statistic/company/{branch_id}/audience")
    check_for_captcha(page)
    Nikis_var = find_and_click_profile_link(page)
    if Nikis_var:
        pass
    check_for_captcha(page)
    fill_period(page, period)
    check_for_captcha(page)
    select_grouping(page)
    check_for_captcha(page)
    page.wait_for_timeout(514) #тут прям нужен небольшой вэит
    is_filial = page.get_by_text("Переходы в профиль филиала")
    
    if is_filial.is_visible():
        stats = {}
        # for section in ("Переходы в профиль филиала", "Что делали в профиле филиала"):
        #     base = page.get_by_text(section, exact=True).locator("..").locator("..").locator("..")
        #     for box in base.locator('div.stat-box-kind').all():
        #         texts = get_child_texts(page, box)
        #         key = get_all_letters(texts[0])
        #         val = get_all_digits(texts[-1])
        #         stats[key] = val
        stats["direct transitions"] = 'empty_value'
        stats["discovery in maps"] = 'empty_value'
        stats["routes"] = 'empty_value'
        stats["review views"] = 'empty_value'
        stats["photo views"] = 'empty_value'
        stats["transitions to the site"] = 'empty_value'
        stats["clicks on the phone"] = 'empty_value'
        stats["opening hours views"] = 'empty_value'
        stats["entry views"] = 'empty_value'
        counter_routes = 0
        counter_transitions_to_the_site = 0
        counter_phone_clicks = 0
        for i in page.locator('div.stat-box-kind').all():
            
            texts = get_child_texts(page, i)
            if  "переходы" in get_all_letters(texts[0]):
                key = "direct transitions"
                val = get_all_digits(texts[-1])
                stats[key] = val
            if "артах" in get_all_letters(texts[0]) :
                key = "discovery in maps"
                val = get_all_digits(texts[-1])
                stats[key] = val
            if "маршрутов" in get_all_letters(texts[0]) :
                if counter_routes ==0:
                    key = "routes"
                    val = get_all_digits(texts[-1])
                    stats[key] = val
                    counter_routes+=1263
                else:
                    pass
            if "Просмотры" in get_all_letters(texts[0]) :
                key = "review views"
                val = get_all_digits(texts[-1])
                stats[key] = val
            if "фото" in get_all_letters(texts[0]) :
                key = "photo views"
                val = get_all_digits(texts[-1])
                stats[key] = val
            if "сайт" in get_all_letters(texts[0]):
                if counter_transitions_to_the_site == 0:

                    key = "transitions to the site"
                    val = get_all_digits(texts[-1])
                    stats[key] = val
                    counter_transitions_to_the_site+=1
                else:
                    pass
            if "телефону" in get_all_letters(texts[0]):
                if counter_phone_clicks == 0:

                    key = "clicks on the phone"
                    val = get_all_digits(texts[-1])
                    stats[key] = val
                    counter_phone_clicks +=1
                else:
                    pass
            if "работы" in get_all_letters(texts[0]):
                key = "opening hours views"
                val = get_all_digits(texts[-1])
                stats[key] = val
            if "входов" in get_all_letters(texts[0]):
                key = "entry views"
                val = get_all_digits(texts[-1])
                stats[key] = val
            if "яндекс" in get_all_letters(texts[0]).lower():
                break

        return list(stats.items())
    else:
        stats = {}
        stats["Y_poisk"] = 'empty_value'
        stats["Y_maps"] = 'empty_value'
        stats["Y_nav"] = 'empty_value'
        stats["transitions to the site"] = 'empty_value'
        stats["routes"] = 'empty_value'
        stats["clicks on the phone"] = 'empty_value'
        for i in page.locator('div.stat-box-kind').all():
            texts = get_child_texts(page, i)
            if "поиск" in get_all_letters(texts[0]).lower():
                key = "Y_poisk"
                value = texts[-1]
                stats[key] = value
            if "карты" in get_all_letters(texts[0]).lower():
                key = "Y_maps"
                value = texts[-1]
                stats[key] = value
            if "навигатор" in get_all_letters(texts[0]).lower():
                key = "Y_nav"
                value = texts[-1]
                stats[key] = value
            if "сайт" in get_all_letters(texts[0]).lower():
                key = "transitions to the site"
                value = texts[-1]
                stats[key] = value
            if "марш" in get_all_letters(texts[0]).lower():
                key = "routes"
                value = texts[-1]
                stats[key] = value
            if "нажа" in get_all_letters(texts[0]).lower():
                key = "clicks on the phone"
                value = texts[-1]
                stats[key] = value
        return list(stats.items())

def get_company_statistic(page: Page, company_id: int, period: str | None) -> List[Tuple[str, Any]]:
    """
    Placeholder for fetching company-level statistics.
    This function needs to be implemented with the specific logic
    for scraping company statistics, which might differ from branch statistics.
    """
    page.goto(f"https://yandex.ru/business/statistic/company/{company_id}/audience")
    check_connection(page)
    if f"https://yandex.ru/business/statistic/company/{company_id}/audience" not in   page.url  :
        page.goto(f"https://yandex.ru/business/statistic/company/{company_id}/audience")
    page.wait_for_timeout(15000)

    check_for_captcha(page)
    Nikis_var = find_and_click_profile_link(page) # This might be different for company pages - Да нихуя,  там ищет элемент с таким же классом, а потом "профиль" так что вариант такой что вариантов нет
    if Nikis_var:
        pass
    fill_period(page, period)
    select_grouping(page) # Grouping might also be different or not applicable - да там тот же html по сути только ветвление добавлено в DOOM параллельно тому что было, наш код пашет стабильно
    # logger.warning(f"Company statistics scraping for {company_id} is not fully implemented. Returning placeholder.")
    # Actual scraping logic for company statistics would go here.
    # For now, returning an empty list or a placeholder.
    # return [("placeholder_company_stat", "value")]
    stats = {}
    stats["Y_poisk"] = 'empty_value'
    stats["Y_maps"] = 'empty_value'
    stats["Y_nav"] = 'empty_value'
    stats["transitions to the site"] = 'empty_value'
    stats["routes"] = 'empty_value'
    stats["clicks on the phone"] = 'empty_value'
    for i in page.locator('div.stat-box-kind').all():
        texts = get_child_texts(page, i)
        if "поиск" in get_all_letters(texts[0]).lower():
            key = "Y_poisk"
            value = texts[-1]
            stats[key] = value
        if "карты" in get_all_letters(texts[0]).lower():
            key = "Y_maps"
            value = texts[-1]
            stats[key] = value
        if "навигатор" in get_all_letters(texts[0]).lower():
            key = "Y_nav"
            value = texts[-1]
            stats[key] = value
        if "сайт" in get_all_letters(texts[0]).lower():
            key = "transitions to the site"
            value = texts[-1]
            stats[key] = value
        if "марш" in get_all_letters(texts[0]).lower():
            key = "routes"
            value = texts[-1]
            stats[key] = value
        if "нажа" in get_all_letters(texts[0]).lower():
            key = "clicks on the phone"
            value = texts[-1]
            stats[key] = value
    all_childs = get_child_texts(page.locator("div.audience-summary-events__summary-events"))
    stats["statistic_from_cells"] = all_childs
    return list(stats.items())


def get_branch_competitors(page: Page, branch_id: int) -> List[Tuple[str, Any]]:
    page.goto(f"https://yandex.ru/business/competitors/company/{branch_id}")
    check_connection(page)
    page.wait_for_timeout(15000)
    if   f"https://yandex.ru/business/competitors/company/{branch_id}" not in page.url:
        page.goto(f"https://yandex.ru/business/competitors/company/{branch_id}")
    check_for_captcha(page)
    texts = get_child_texts(page, page.locator('div.company-competitors-table__own-company'))
    logger.debug(f"Own company competitor block texts: {texts}")
    if len(texts) < 6:
        raise NotFound("Own company block missing data.")
    out = []
    out.append((transliterate_cyrillic("Позиция"), transliterate_cyrillic(get_all_digits(texts[0])  )))

    for i in range(len(texts)):
        if texts[i] == "%":
            out.append((transliterate_cyrillic("Доля трафика"), (texts[i-1])))
            out.append((transliterate_cyrillic("Название филиала"), transliterate_cyrillic(texts[i+2])))
            out.append((transliterate_cyrillic("Тип бизнеса"), transliterate_cyrillic(texts[i+3])))

        if len(texts[i]) == 3 and "." in texts[i]:
            out.append((transliterate_cyrillic("Рейтинг"), texts[i]))
        if "оцен" in texts[i].lower():
            if "нет оценок" in texts[i].lower():
                out.append((transliterate_cyrillic("Количество оценок"), (0)))
            else:
                out.append((transliterate_cyrillic("Количество оценок"), (texts[i-1])))
        if "отз" in texts[i].lower():
            out.append((transliterate_cyrillic("Количество отзывов"), (texts[i-1])))
        main_params_locator = page.get_by_text("Похожие компании в Картах и Навигаторе").locator("..")
    main_params_texts = get_child_texts(page, main_params_locator)
    logger.debug(f"Main competitor params texts: {main_params_texts}")

    for i in range(len(main_params_texts)):
        if "запросов" in main_params_texts[i]:
            out.append((transliterate_cyrillic("Запросов по вашим категориям"), (main_params_texts[i+1])))
        if "рядом" in main_params_texts[i]:
            out.append((transliterate_cyrillic("Всего похожих компаний рядом"), (main_params_texts[i+1])))
        if "в эти" in main_params_texts[i]:
            out.append((transliterate_cyrillic("Дискавери-переходов в эти компании"), (main_params_texts[i+1])))
        if "из них" in main_params_texts[i]:
                out.append((transliterate_cyrillic("Колиество переходов в компанию-лидер"), (main_params_texts[i+1])))
        if "в вашу" in main_params_texts[i]:
                out.append((transliterate_cyrillic("Дискавери-переходов в вашу компанию"), (main_params_texts[i+1])))
        if "% от всех" in main_params_texts[i]:
                out.append((transliterate_cyrillic("Процент Дискавери-переходов в вашу компанию от всех"), (main_params_texts[i-1])))
        if "% от лидера" in main_params_texts[i]:
                out.append((transliterate_cyrillic("Процент Дискавери-переходов в вашу компанию от лидера"), (main_params_texts[i-1])))

    
    
    # if len(texts) ==17:
    #     del texts[1]
    
    # out.append((transliterate_cyrillic("Название филиала"), transliterate_cyrillic(texts[4])))
    # out.append((transliterate_cyrillic("Тип бизнеса"), transliterate_cyrillic(texts[5])))
    # labels = [("оценок", -2, "Количество оценок"),
    #           ("фото", -1, "Количество фото"),
    #           ("товар", -1, "Количество товаров или услуг"),
    #           ("акци", -1, "Количество акций")]
    # for term, idx, name in labels:
    #     for i, t in enumerate(texts):
    #         if term in t:
    #             val = get_all_digits(texts[i+idx])
    #             if val:
    #                 out.append((transliterate_cyrillic(name), transliterate_cyrillic(val)))
    return out


def convert_date_format(date_str):
    # Словарь для преобразования названий месяцев с русского на английский
    month_mapping = {
        'января': 'January', 'февраля': 'February', 'марта': 'March', 'апреля': 'April',
        'мая': 'May', 'июня': 'June', 'июля': 'July', 'августа': 'August',
        'сентября': 'September', 'октября': 'October', 'ноября': 'November', 'декабря': 'December'
    }

    # Разбиваем строку на части и заменяем русское название месяца на английское
    parts = date_str.split()
    day = parts[0]
    month_ru = parts[1]
    year = parts[2]

    month_en = month_mapping.get(month_ru.lower()) # Используем .lower() на случай разных регистров
    if not month_en:
        raise ValueError(f"Неизвестное название месяца: {month_ru}")

    # Собираем строку в формат, который datetime может парсить
    parsed_date_str = f"{day} {month_en} {year}"

    # Парсим строку в объект datetime
    date_object = datetime.strptime(parsed_date_str, '%d %B %Y')

    # Форматируем объект datetime в нужный выходной формат
    return date_object.strftime('%d.%m.%Y')

def extract_url_from_background_image(css_string: str) -> str :
    """
    Извлекает URL-адрес из строки CSS 'background-image'.

    Args:
        css_string (str): Строка CSS, содержащая свойство background-image.
                          Пример: 'background-image: url("https://example.com/image.jpg");'
                          или 'background-image: url(\"https://example.com/image.jpg\");'

    Returns:
        str | "": Извлеченный URL-адрес или "", если URL не найден.
    """
    # Паттерн регулярного выражения:
    # url\( - соответствует "url(" буквально
    # ["']? - соответствует опциональной кавычке (одинарной или двойной)
    # (.*?) - захватывает любой символ (нежадный режим) до следующей кавычки/скобки
    # ["']? - соответствует опциональной кавычке
    # \) - соответствует ")" буквально
    # re.DOTALL позволяет . соответствовать символам новой строки, хотя в данном случае это не критично.
    pattern = r'url\(["\']?(.*?)["\']?\)'
    
    match = re.search(pattern, css_string)
    
    if match:
        # match.group(1) содержит содержимое первой захватывающей группы, то есть сам URL
        return match.group(1)
    else:
        return "error_in_parser, ask programmer"


def get_reviews(page: Page, target_id: int, page_num: int =1) -> List[Dict[str, Any]]:
    """
    Mock function to return sample review data for Yandex.
    In a real implementation, this would scrape review details from the page.
    """
    page.goto(f"https://yandex.ru/sprav/{target_id}/p/edit/reviews/?ranking=by_time&page={page_num}&type=company")
    check_connection(page)
    if f"https://yandex.ru/sprav/{target_id}/p/edit/reviews/?ranking=by_time&page={page_num}&type=company" not in page.url:
        page.goto(f"https://yandex.ru/sprav/{target_id}/p/edit/reviews/?ranking=by_time&page={page_num}&type=company")
    page.wait_for_timeout(15000)

    num_of_reviews_text = page.locator("div.ReviewsPage-HeadingReviewsCount").text_content()
    num_of_reviews_int = get_all_digits( num_of_reviews_text)
    page_max_num = 1
    reviews_per_page = 20 # Assuming 20 reviews per page

    if num_of_reviews_int is not None and num_of_reviews_int > reviews_per_page:
        pagination_pages_locator = page.locator("div.Pagination-Pages")
        if pagination_pages_locator.is_visible(timeout=3000):
            pagination_locator_texts = get_child_texts(page, pagination_pages_locator)
            list_of_paginations_nums = []
            for i_text in pagination_locator_texts:
                num = get_all_digits(i_text)
                if num is not None:
                    list_of_paginations_nums.append(num)
            if list_of_paginations_nums:
                page_max_num = max(list_of_paginations_nums)
        else:
            logger.warning("Pagination controls not visible, assuming page_max_num=1 or relying on page_num argument.")
    
    # The output structure is a dictionary, not a list of tuples for the main result
    out_dict = {"pagination_page_max_num": page_max_num, "id": target_id, "reviews_info_list": []}

    reviews_box_locator = page.locator("div.Review").all()
    logger.debug(f"Found {len(reviews_box_locator)} review boxes on page {page_num}.")

    for review in reviews_box_locator:
        review_class_attr = review.get_attribute("class") or ""
        review_is_read = "Review_unread" not in review_class_attr
        try:
            review.locator("span.Review-ReadMoreLink").click(force = True, timeout=500) # Increased timeout
        except:
            pass
        avatar_link = review.locator("div.Review-InfoWrapper").locator("img").get_attribute("src")
        nickname_of_the_review_author = review.locator("div.Review-UserName").text_content()
        rating = get_all_digits(review.locator("span.StarsRating").get_attribute("class"))/2
        date_of_review = convert_date_format(review.locator("span.Review-Date").text_content())
        review_text = review.locator("div.Review-Text").text_content()
        data_of_answer = "has not answer"
        text_of_answer = "has not answer"

        if review_is_read:
            # Only click to expand if it's not already expanded or if necessary
            # This logic might need adjustment based on how "read" reviews are displayed
            hide_button = review.locator("span.BusinessResponseSaved-HideButton_top")
            if hide_button.is_visible(timeout=500): # Check if it's collapsible (i.e., expanded)
                try:
                    hide_button.click(timeout=500) # Click to potentially reveal or ensure state
                except Exception as e:
                    logger.debug(f"Could not click hide button for review answer: {e}")
            
            response_timestamp_locator = review.locator("span.BusinessResponseSaved-ResponseTimestamp")
            if response_timestamp_locator.is_visible(timeout=1000): # Increased timeout
                data_of_answer = convert_date_format(response_timestamp_locator.text_content())
                text_of_answer = review.locator("div.ResponseTextContent, div.BusinessResponseSaved-ResponseTextContent").text_content(timeout=1000)
        
        all_photoes_src_list = []
        # Simpler selector for review tiles, assuming they are direct children or identifiable
        photo_tile_selector = "div.Review-Tile" # Adjusted selector
        if review.locator(photo_tile_selector).first.is_visible(timeout=500): # Check visibility of the first potential tile
            all_photoes_div_locators = review.locator(photo_tile_selector).all()
            for i in all_photoes_div_locators:
                all_photoes_src_list.append(extract_url_from_background_image(i.get_attribute("style")).replace("&quot;", ""))
        
        out_dict["reviews_info_list"].append({
            "nickname": nickname_of_the_review_author,
            "link_to_avatar": avatar_link,
            "data_of_review": date_of_review,
            "review_is_readed": review_is_read,
            "rating": rating,
            "review_text": review_text,
            "all_photoes_src_list": all_photoes_src_list, # Will be an empty list if no photos
            "data_of_answer": data_of_answer,
            "text_of_answer": text_of_answer
        })
    return out_dict

def run(target_id: int, task_type: str, period: str | None = None, cookies: list | None = None, page_num: int = 1, headless: bool = False) -> dict:
    """
    Execute scraping and return raw dict. Raises exceptions on errors or CaptchaRequired.
    task_type can be "statistics" or "competitors".
    """
    playwright_instance = None
    browser: BrowserContext | None = None # Renamed for clarity
    page: Page | None = None
    try:
        playwright_instance = sync_playwright().start()
        if not playwright_instance:
            raise RuntimeError("Failed to start Playwright.")

        # Add --start-minimized argument for Chromium
        browser_args = ["--start-minimized"]
        browser = playwright_instance.chromium.launch_persistent_context(BROWSER_PATH, headless=headless, args=browser_args)
        if cookies: 
            browser.add_cookies(cookies)

        page = browser.new_page()
        page.goto("https://yandex.ru/sprav/companies")

        result_payload = {}
        if task_type == "statistics":
            stats = get_branch_statistics(page, target_id, period)
            result_payload["statistics"] = stats
        elif task_type == "competitors":
            comps = get_branch_competitors(page, target_id)
            result_payload["competitors"] = comps
        elif task_type == "reviews":
            result_payload["reviews"] = get_reviews(page, target_id, page_num)
        else:
            raise ValueError(f"Unknown task_type for yandex_scraper: {task_type}")

        return {"target_id": target_id, **result_payload}
    finally:
        if page and not page.is_closed():
            try:
                page.close()
            except Exception as e:
                logger.warning(f"Exception during page close: {e}", exc_info=True)
        if browser and hasattr(browser, 'close'): # Check if context was assigned and is closable
            try: # browser_context is the actual context object from Playwright
                browser.close()
            except Exception as e:
                logger.warning(f"Exception during browser_context close: {e}", exc_info=True)
        if playwright_instance:
            try:
                playwright_instance.stop()
            except Exception as e:
                logger.warning(f"Exception during Playwright stop: {e}", exc_info=True)

