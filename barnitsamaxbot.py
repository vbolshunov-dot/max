#перед запуском бота, скрыть все API и ID
"""
Бот «Банного двора» для мессенджера MAX.

Это тот же бот, что и barnitsatgbot.py, но под другой мессенджер. Логика работы
с YCLIENTS и все правила бронирования повторены один в один, а вот всё, что
касается общения с гостем, написано заново: у MAX своё API, и половины
привычных удобств Telegram в нём просто нет.

Чем пришлось заняться руками:
  * Диалоги. В MAX нет ничего похожего на ConversationHandler, поэтому шаг
    каждого гостя хранится сам, в словаре STATE.
  * Расписание рассылок. Нет JobQueue — крутим собственный цикл.
  * Хранение состояния. Нет персистентности — пишем JSON рядом с ботом.
  * Клавиатуры. Есть только inline-кнопки, поэтому ответы «Всё верно /
    Изменить» стали кнопками, а не клавиатурой под полем ввода.
  * Альбом фотографий. Отдельных медиагрупп нет, зато в одно сообщение можно
    вложить несколько картинок — этим и пользуемся.

Документация API: https://dev.max.ru/docs-api
Токен бота выдаёт @MasterBot по команде /newbot.

ВАЖНО про таблицы услуг: BATHS, SERVICE_IDS, SERVICE_DURATIONS и SEANCE_STARTS
здесь продублированы из телеграм-бота. Меняешь цены или расписание заходов —
правь в обоих файлах, иначе боты разъедутся.
"""
import asyncio
import json
import logging
import os
import re
import sys
from collections import Counter
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv

# Загружаем переменные из файла .env (он НЕ должен попадать в git)
load_dotenv()


def _int_env(name: str) -> int:
    """Читает целочисленную переменную окружения. Если её нет — вернёт 0,
    а check_config() найдёт это и остановит бот с понятной ошибкой."""
    value = os.getenv(name, "")
    return int(value) if value else 0


# ────────────────────────────── конфигурация ──────────────────────────────

# Токен бота MAX. Выдаётся @MasterBot по команде /newbot — это НЕ токен
# телеграм-бота, они разные.
MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "")

# Адрес API. Именно platform-api2, а не platform-api: в документации сказано
# слать запросы сюда, и токен передавать заголовком, а не параметром в URL.
MAX_API_BASE = os.getenv("MAX_API_BASE", "https://platform-api2.max.ru")

YCLIENTS_TOKEN = os.getenv("YCLIENTS_TOKEN", "")  # partner token
# Пользовательский токен YCLIENTS (User token). Некоторые методы API требуют
# заголовок вида "Bearer <partner_token>, User <user_token>".
YCLIENTS_USER_TOKEN = os.getenv("YCLIENTS_USER_TOKEN", "")
YCLIENTS_COMPANY_ID = os.getenv("YCLIENTS_COMPANY_ID", "")

# Контакты менеджера для индивидуальных бронирований
MANAGER_PHONE = os.getenv("MANAGER_CONTACT_URL", "")
MANAGER_NAME = os.getenv("MANAGER_NAME", "")
# Ссылка на чат с менеджером для кнопок-ссылок. У MAX свои ссылки на профили,
# но обычная https-ссылка в кнопке работает всегда, поэтому оставляем её.
MANAGER_CHAT_URL = os.getenv("MANAGER_MAX_URL", "") or os.getenv("MANAGER_TG_URL", "")

# Категория YCLIENTS, из которой берутся процедуры банного меню.
PROCEDURES_CATEGORY_ID = int(os.getenv("PROCEDURES_CATEGORY_ID", "26014494"))
# Насколько доверяем закэшированному прайсу.
SERVICES_CACHE_MINUTES = 10

# Сколько ждём ответа YCLIENTS и MAX на обычный запрос.
REQUEST_TIMEOUT = 7.0
# Долгий опрос обновлений держит соединение открытым — ему нужен свой запас,
# заведомо больший, чем UPDATES_TIMEOUT, иначе httpx оборвёт живой запрос.
UPDATES_TIMEOUT = 30
UPDATES_REQUEST_TIMEOUT = UPDATES_TIMEOUT + 15

# Папки с фотографиями: прайс уходит первым, банное меню следом.
MENU_PHOTOS_DIR = Path(__file__).with_name("menu")
PRICE_PHOTOS_DIR = Path(__file__).with_name("price")

# Файл состояния: шаги диалогов, данные гостей и очередь напоминаний.
# На хостинге его место — в папке, которая переживает перезапуск контейнера.
STATE_FILE = os.getenv("MAX_STATE_FILE", "max_bot_state.json")

# Часовой пояс филиала. Все даты и время — местные, а сервер вполне может жить
# в UTC, поэтому расписание рассылок привязано к этой зоне явно.
LOCAL_TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))

# Когда уходят автоматические сообщения гостям (по местному времени).
REMINDER_TIME = dt_time(12, 0)
FEEDBACK_TIME = dt_time(10, 0)
# Через сколько дней после визита бронь выбрасывается из очереди рассылок.
BOOKINGS_KEEP_DAYS = 7

#конфигурация бань: staff_id для каждой бани, лимиты гостей и времени
BATHS = {
    "birch": {
        "name": "Берёзовая",
        "staff_id": _int_env("BIRCH_STAFF_ID"),
        "max_guests": 4,
        "min_hours": 3,
        "desc": "до 4 гостей, от 3 часов"
    },
    "pine": {
        "name": "Хвойная",
        "staff_id": _int_env("PINE_STAFF_ID"),
        "max_guests": 8,
        "min_hours": 4,
        "desc": "до 8 гостей, от 4 часов"
    }
}

# матрица service_id: баня -> тип дня -> с/без процедур
SERVICE_IDS = {
    "birch": {
        "weekday": {
            "no_proc": _int_env("BIRCH_WEEKDAY_NO_PROC_SERVICE_ID"),
            "with_proc": _int_env("BIRCH_WEEKDAY_WITH_PROC_SERVICE_ID"),
        },
        "weekend": {
            "no_proc": _int_env("BIRCH_WEEKEND_NO_PROC_SERVICE_ID"),
            "with_proc": _int_env("BIRCH_WEEKEND_WITH_PROC_SERVICE_ID"),
        }
    },
    "pine": {
        "weekday": {
            "no_proc": _int_env("PINE_WEEKDAY_NO_PROC_SERVICE_ID"),
            "with_proc": _int_env("PINE_WEEKDAY_WITH_PROC_SERVICE_ID"),
        },
        "weekend": {
            "no_proc": _int_env("PINE_WEEKEND_NO_PROC_SERVICE_ID"),
            "with_proc": _int_env("PINE_WEEKEND_WITH_PROC_SERVICE_ID"),
        }
    }
}

# Длительность сеанса для каждой услуги, В ЧАСАХ. Значения должны совпадать
# со столбцом «Длительность» у услуги в YCLIENTS.
SERVICE_DURATIONS = {
    "birch": {
        "weekday": {"no_proc": 3, "with_proc": 3},
        "weekend": {"no_proc": 3, "with_proc": 3},
    },
    "pine": {
        "weekday": {"no_proc": 4, "with_proc": 4},
        "weekend": {"no_proc": 4, "with_proc": 4},
    }
}

# Фиксированное начало сеансов в каждой бане: баня топится под конкретный
# заход, между заходами нужен перерыв на уборку.
SEANCE_STARTS = {
    "birch": {
        "weekday": ["10:00", "14:00", "18:00"],
        "weekend": ["10:00", "14:00", "18:00"],
    },
    "pine": {
        "weekday": ["10:00", "16:00"],
        "weekend": ["10:00", "16:00"],
    },
}

# На сколько дней вперёд предлагать даты и по сколько кнопок ставить в ряд.
DAYS_AHEAD = 21
DATE_COLUMNS = 3

WEEKDAYS_RU = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]

# настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Шаги диалога. В MAX нет ConversationHandler, поэтому шаг гостя — обычная
# строка в его состоянии, а диспетчер ниже смотрит на неё и решает, кому отдать
# сообщение. Строки, а не числа: их видно в JSON-файле состояния глазами.
STEP_NAME = "name"
STEP_PHONE = "phone"
STEP_CONFIRM = "confirm"
STEP_GUEST_COUNT = "guest_count"

# ключи временных данных бронирования (сбрасываются при /cancel и /start)
BOOKING_KEYS = ("bath_id", "day_type", "with_procedures", "guest_count", "book_date", "procedures")

# регулярка для валидации российского номера телефона
PHONE_REGEX = re.compile(r'^\+7\d{10}$')

# Состояние гостей: {user_id: {"step": ..., "name": ..., "chat_id": ...}}
STATE: dict = {}
# Очередь напоминаний: список оформленных броней.
BOOKINGS: list = []
# Токены загруженных в MAX фотографий: {ключ альбома: [токены]}. Загружать
# одни и те же картинки на каждый показ — лишние секунды ожидания.
PHOTO_TOKENS: dict = {}
# Прайс целиком, одним запросом на все услуги: {id: {"title": ..., "price": ...}}
SERVICES_CACHE: dict = {"at": None, "index": {}}


def check_config() -> None:
    """
    Проверяет, что все обязательные переменные окружения заданы.
    Лучше упасть сразу с понятной ошибкой, чем ловить 401 в середине работы.
    """
    required = {
        "MAX_BOT_TOKEN": MAX_BOT_TOKEN,
        "YCLIENTS_TOKEN": YCLIENTS_TOKEN,
        "YCLIENTS_COMPANY_ID": YCLIENTS_COMPANY_ID,
        "MANAGER_CONTACT_URL": MANAGER_PHONE,
        "MANAGER_NAME": MANAGER_NAME,
        "BIRCH_STAFF_ID": BATHS["birch"]["staff_id"],
        "PINE_STAFF_ID": BATHS["pine"]["staff_id"],
        "BIRCH_WEEKDAY_NO_PROC_SERVICE_ID": SERVICE_IDS["birch"]["weekday"]["no_proc"],
        "BIRCH_WEEKDAY_WITH_PROC_SERVICE_ID": SERVICE_IDS["birch"]["weekday"]["with_proc"],
        "BIRCH_WEEKEND_NO_PROC_SERVICE_ID": SERVICE_IDS["birch"]["weekend"]["no_proc"],
        "BIRCH_WEEKEND_WITH_PROC_SERVICE_ID": SERVICE_IDS["birch"]["weekend"]["with_proc"],
        "PINE_WEEKDAY_NO_PROC_SERVICE_ID": SERVICE_IDS["pine"]["weekday"]["no_proc"],
        "PINE_WEEKDAY_WITH_PROC_SERVICE_ID": SERVICE_IDS["pine"]["weekday"]["with_proc"],
        "PINE_WEEKEND_NO_PROC_SERVICE_ID": SERVICE_IDS["pine"]["weekend"]["no_proc"],
        "PINE_WEEKEND_WITH_PROC_SERVICE_ID": SERVICE_IDS["pine"]["weekend"]["with_proc"],
    }
    if not YCLIENTS_USER_TOKEN:
        print(
            "ВНИМАНИЕ: не задан YCLIENTS_USER_TOKEN. Без него YCLIENTS не даёт "
            "искать клиентов по базе, и те, кто уже есть в базе салона, не смогут "
            "зарегистрироваться в боте.",
            file=sys.stderr,
        )

    if not MANAGER_CHAT_URL:
        print(
            "ВНИМАНИЕ: не задан MANAGER_MAX_URL — кнопки «Написать менеджеру» "
            "показываться не будут, останутся имя и телефон текстом.",
            file=sys.stderr,
        )

    missing = [name for name, value in required.items() if not value]
    if missing:
        print(
            "ОШИБКА: не заданы переменные окружения: " + ", ".join(missing) +
            "\nСоздайте файл .env рядом с этим скриптом (образец — .env.example)",
            file=sys.stderr,
        )
        sys.exit(1)


# ──────────────────────────── общий HTTP-клиент ────────────────────────────

# Один клиент на весь бот: иначе каждый запрос заново проходит TLS-рукопожатие,
# а за одну бронь запросов набирается с полдесятка.
HTTP: dict = {"client": None}


def http_client() -> httpx.AsyncClient:
    """Отдаёт общий HTTP-клиент, при необходимости создавая его."""
    client = HTTP.get("client")
    if client is None or client.is_closed:
        client = httpx.AsyncClient(timeout=REQUEST_TIMEOUT)
        HTTP["client"] = client
    return client


async def close_http_client() -> None:
    """Закрывает общий клиент при остановке бота."""
    client = HTTP.get("client")
    if client is not None and not client.is_closed:
        await client.aclose()
        logger.info("HTTP-клиент закрыт")


# ─────────────────────────────── клиент MAX ────────────────────────────────

def max_headers() -> dict:
    """Заголовки для API MAX. Токен идёт заголовком: передавать его параметром
    в URL документация не рекомендует, да и в логах он так не светится."""
    return {"Authorization": MAX_BOT_TOKEN, "Content-Type": "application/json"}


async def max_request(method: str, path: str, params: dict = None, payload: dict = None,
                      timeout: float = REQUEST_TIMEOUT) -> dict:
    """
    Один запрос к API MAX.

    Returns:
        dict: разобранный ответ; пустой словарь, если запрос не удался. Наверх
              ошибки не поднимаем — бот не должен падать от одного неудачного
              сообщения, а разбираться потом можно по логу.
    """
    url = f"{MAX_API_BASE}{path}"
    try:
        response = await http_client().request(
            method, url, params=params, json=payload, headers=max_headers(), timeout=timeout
        )
    except Exception as e:
        logger.error(f"MAX {method} {path}: запрос не прошёл — {e}")
        return {}

    if response.status_code != 200:
        logger.error(f"MAX {method} {path} вернул {response.status_code}: {response.text[:300]}")
        return {}

    try:
        return response.json()
    except ValueError:
        logger.error(f"MAX {method} {path}: ответ не разобрался как JSON")
        return {}


async def send_message(chat_id, text: str, buttons: list = None, images: list = None) -> dict:
    """
    Отправляет сообщение гостю или в чат.

    Args:
        buttons: ряды кнопок, как их собирают функции ниже
        images: токены загруженных картинок — несколько штук уходят одним
                сообщением, это и заменяет альбом из телеграма
    """
    body = {"text": text[:4000]}  # MAX режет сообщения на четырёх тысячах символов
    attachments = []
    if images:
        attachments += [{"type": "image", "payload": {"token": token}} for token in images]
    if buttons:
        attachments.append({"type": "inline_keyboard", "payload": {"buttons": buttons}})
    if attachments:
        body["attachments"] = attachments

    return await max_request("POST", "/messages", params={"chat_id": chat_id}, payload=body)


async def answer_callback(callback_id: str, text: str = None, buttons: list = None,
                          notification: str = None) -> dict:
    """
    Отвечает на нажатие кнопки.

    Два режима, как в телеграме:
      * notification — короткая всплывашка поверх экрана, сообщение не трогаем;
      * text — заменяет сообщение, на котором была кнопка (аналог правки).

    На нажатие нужно ответить в любом случае, иначе у гостя кнопка так и
    останется «нажатой».
    """
    body = {}
    if notification:
        body["notification"] = notification
    if text is not None:
        message = {"text": text[:4000]}
        if buttons:
            message["attachments"] = [{"type": "inline_keyboard", "payload": {"buttons": buttons}}]
        else:
            # Пустой список именно затирает старые кнопки; None оставил бы их висеть
            message["attachments"] = []
        body["message"] = message

    return await max_request("POST", "/answers", params={"callback_id": callback_id}, payload=body)


async def set_bot_commands() -> None:
    """Прописывает список команд, который MAX показывает в меню бота."""
    commands = [
        {"name": "start", "description": "Меню бота и регистрация"},
        {"name": "book", "description": "Забронировать баню"},
        {"name": "price", "description": "Прайс-лист"},
        {"name": "help", "description": "Помощь и контакты"},
        {"name": "cancel", "description": "Отменить текущее действие"},
    ]
    result = await max_request("PATCH", "/me", payload={"commands": commands})
    if result:
        logger.info("Меню команд бота обновлено")


async def upload_image(path: Path) -> str:
    """
    Загружает картинку в MAX и возвращает токен для вложения.

    Загрузка двухшаговая: сперва просим у API адрес, потом кладём файл туда.
    Пустая строка в ответе означает, что загрузить не вышло.
    """
    endpoint = await max_request("POST", "/uploads", params={"type": "image"})
    upload_url = endpoint.get("url")
    if not upload_url:
        logger.error(f"MAX не дал адрес для загрузки {path.name}")
        return ""

    try:
        files = {"data": (path.name, path.read_bytes())}
        response = await http_client().post(upload_url, files=files, timeout=60.0)
        data = response.json()
    except Exception as e:
        logger.error(f"Не удалось загрузить {path.name} в MAX: {e}")
        return ""

    # Ответ приходит в двух видах: либо сразу токен, либо словарь photos,
    # внутри которого токен лежит под непредсказуемым ключом.
    if data.get("token"):
        return data["token"]
    photos = data.get("photos") or {}
    for item in photos.values():
        if item.get("token"):
            return item["token"]

    logger.error(f"В ответе на загрузку {path.name} не нашлось токена: {str(data)[:200]}")
    return ""


# ──────────────────────────────── кнопки ───────────────────────────────────

def button(text: str, payload: str) -> dict:
    """Обычная кнопка: нажатие прилетает обратно боту как message_callback."""
    return {"type": "callback", "text": text, "payload": payload}


def link_button(text: str, url: str) -> dict:
    """Кнопка-ссылка. Нажатие открывает ссылку и боту ничего не присылает."""
    return {"type": "link", "text": text, "url": url}


def book_button(text: str = "🌿 Забронировать баню") -> dict:
    """Кнопка вместо подсказки «/book» — команду не надо набирать руками."""
    return button(text, "go_book")


def start_button(text: str = "📝 Зарегистрироваться") -> dict:
    """Кнопка вместо подсказки «/start»."""
    return button(text, "go_start")


def manager_rows(text: str = "📞 Написать менеджеру") -> list:
    """
    Ряд с кнопкой менеджера — или пустота, если ссылка не задана.

    Возвращает именно список рядов, чтобы его можно было просто прибавить к
    клавиатуре: без ссылки кнопка молча исчезнет, а не сломает сообщение.
    """
    return [[link_button(text, MANAGER_CHAT_URL)]] if MANAGER_CHAT_URL else []


# ────────────────────────── состояние и хранилище ──────────────────────────

def user_state(user_id: int) -> dict:
    """Состояние гостя, создавая пустое при первой встрече."""
    return STATE.setdefault(int(user_id), {})


def clear_booking_data(data: dict) -> None:
    """Удаляет временные данные бронирования, не трогая регистрацию."""
    for key in BOOKING_KEYS:
        data.pop(key, None)
    data.pop("step", None)


def save_state() -> None:
    """
    Сохраняет состояние на диск.

    Пишем через временный файл и подменяем его целиком: если бота остановят
    ровно во время записи, старый файл останется целым, а не обрежется на
    середине.
    """
    path = Path(STATE_FILE)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "users": {str(user_id): data for user_id, data in STATE.items()},
        "bookings": BOOKINGS,
        "photo_tokens": PHOTO_TOKENS,
    }
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        logger.error(f"Не удалось сохранить состояние в {STATE_FILE}: {e}")


def load_state() -> None:
    """Читает состояние с диска. Нет файла или он битый — начинаем с чистого."""
    path = Path(STATE_FILE)
    if not path.exists():
        logger.info(f"Файла состояния {STATE_FILE} нет — начинаем с пустого")
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Файл состояния {STATE_FILE} не читается ({e}) — начинаем с пустого")
        return

    # Ключи в JSON всегда строки, а user_id у нас число
    STATE.update({int(user_id): data for user_id, data in (payload.get("users") or {}).items()})
    BOOKINGS.extend(payload.get("bookings") or [])
    PHOTO_TOKENS.update(payload.get("photo_tokens") or {})
    logger.info(f"Состояние загружено: гостей {len(STATE)}, броней в очереди {len(BOOKINGS)}")


# ──────────────────────────── общие мелочи ─────────────────────────────────

def yclients_auth_header() -> str:
    """Собирает значение заголовка Authorization для YCLIENTS с учётом User-токена"""
    if YCLIENTS_USER_TOKEN:
        return f"Bearer {YCLIENTS_TOKEN}, User {YCLIENTS_USER_TOKEN}"
    return f"Bearer {YCLIENTS_TOKEN}"


def yclients_headers(with_content_type: bool = False) -> dict:
    """Заголовки для запросов к YCLIENTS."""
    headers = {
        "Authorization": yclients_auth_header(),
        "Accept": "application/vnd.yclients.v2+json",
    }
    if with_content_type:
        headers["Content-Type"] = "application/json"
    return headers


def _today_local() -> date:
    """Сегодняшняя дата по часовому поясу филиала, без времени.

    Именно по поясу филиала, а не сервера: хостинг может стоять в другой зоне,
    и его дата переключается в другой момент, чем в бане."""
    return datetime.now(LOCAL_TZ).date()


def is_weekend(date_obj) -> bool:
    """Проверяет, выходной ли это день (суббота или воскресенье)"""
    return date_obj.weekday() >= 5


def format_date_ru(date_iso: str) -> str:
    """2026-08-26 -> '26.08.2026, среда'. Для администратора день недели
    понятнее, чем «будни»: сразу видно, о каком дне речь."""
    day = datetime.strptime(date_iso, "%Y-%m-%d")
    return f"{day.strftime('%d.%m.%Y')}, {WEEKDAYS_RU[day.weekday()]}"


def money(value) -> str:
    """12000 -> '12 000 ₽'. Неразрывный пробел, чтобы цена не рвалась на строки."""
    return f"{int(value):,}".replace(",", " ") + " ₽"


def get_service_id(bath_id: str, day_type: str, with_proc: bool) -> int:
    """Определяет service_id на основе выбранных параметров"""
    proc_key = "with_proc" if with_proc else "no_proc"
    service_id = SERVICE_IDS[bath_id][day_type][proc_key]
    logger.info(f"Определён service_id={service_id} для бани={bath_id}, день={day_type}, процедуры={with_proc}")
    return service_id


def get_seance_length(bath_id: str, day_type: str, with_proc: bool) -> int:
    """Длительность сеанса В СЕКУНДАХ — YCLIENTS ожидает именно секунды."""
    proc_key = "with_proc" if with_proc else "no_proc"
    return SERVICE_DURATIONS[bath_id][day_type][proc_key] * 3600


def seance_end(start: str, bath_id: str, day_type: str, with_proc: bool) -> str:
    """Считает конец сеанса из длительности услуги: "14:00" -> "17:00"."""
    hours = get_seance_length(bath_id, day_type, with_proc) // 3600
    return (datetime.strptime(start, "%H:%M") + timedelta(hours=hours)).strftime("%H:%M")


def _phone_digits(phone: str) -> str:
    """Оставляет от номера только цифры: +7 (999) 123-45-67 -> 79991234567."""
    return re.sub(r"\D", "", phone or "")


def _parse_local(value: str) -> datetime:
    """YCLIENTS отдаёт время вида 2026-08-26T10:00:00+03:00. Часовой пояс
    у филиала один и тот же, поэтому берём первые 19 символов."""
    return datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")


# ─────────────────────────────── YCLIENTS ──────────────────────────────────

async def get_services_index() -> dict:
    """
    Прайс целиком, одним запросом на все услуги.

    Цены берём из YCLIENTS, а не из кода: поменял прайс в кабинете — бот
    подхватил сам, деплой не нужен.

    Returns:
        dict: {service_id: {"title", "price", "category_id", "is_online"}}
    """
    now = datetime.now()
    cached_at = SERVICES_CACHE["at"]
    if cached_at and (now - cached_at) < timedelta(minutes=SERVICES_CACHE_MINUTES):
        return SERVICES_CACHE["index"]

    url = f"https://api.yclients.com/api/v1/company/{YCLIENTS_COMPANY_ID}/services/"
    try:
        response = await http_client().get(url, headers=yclients_headers())
        if response.status_code != 200:
            logger.error(f"services вернул {response.status_code}: {response.text[:300]}")
            return SERVICES_CACHE["index"]
        services = response.json().get('data') or []
    except Exception as e:
        logger.error(f"Ошибка получения прайса: {e}")
        return SERVICES_CACHE["index"]

    index = {
        int(service["id"]): {
            "title": service.get("title") or "",
            "price": service.get("price_min") or 0,
            "category_id": service.get("category_id"),
            "is_online": bool(service.get("is_online")),
        }
        for service in services if service.get("id")
    }
    SERVICES_CACHE.update({"at": now, "index": index})
    logger.info(f"Прайс обновлён: услуг {len(index)}")
    return index


async def get_procedures() -> list:
    """
    Процедуры банного меню, доступные для онлайн-записи.

    Returns:
        list: [{"id", "title", "price"}], отсортированы по цене — дешёвые сверху
    """
    index = await get_services_index()
    procedures = [
        {"id": service_id, "title": item["title"], "price": item["price"]}
        for service_id, item in index.items()
        if item["category_id"] == PROCEDURES_CATEGORY_ID and item["is_online"]
    ]
    procedures.sort(key=lambda p: (p["price"], p["title"]))
    logger.info(f"Процедур доступно: {len(procedures)}")
    return procedures


def price_breakdown(bath_title: str, bath_price: int, chosen: list, index: dict) -> tuple:
    """
    Собирает расшифровку стоимости брони.

    Args:
        chosen: id выбранных процедур, повторы значимы (двое взяли одно и то же)

    Returns:
        tuple: (строки расшифровки, итоговая сумма)
    """
    lines = [f"🌿 {bath_title} — {money(bath_price)}"]
    total = int(bath_price)

    for service_id, count in Counter(chosen).items():
        item = index.get(service_id) or {}
        price = int(item.get("price") or 0)
        title = item.get("title") or f"услуга {service_id}"
        suffix = f" × {count}" if count > 1 else ""
        lines.append(f"💆 {title}{suffix} — {money(price * count)}")
        total += price * count

    return lines, total


async def find_client_by_phone(phone: str) -> int:
    """
    Ищет клиента в базе салона по телефону.

    Returns:
        int: id клиента или 0, если не нашёлся
    """
    digits = _phone_digits(phone)
    if not digits:
        return 0

    search_url = f"https://api.yclients.com/api/v1/company/{YCLIENTS_COMPANY_ID}/clients/search"
    payload = {"fields": ["id", "name", "phone"], "page": 1, "page_size": 50,
               "filters": [{"type": "quick_search", "state": {"value": digits}}]}
    try:
        resp = await http_client().post(search_url, json=payload, headers=yclients_headers(True))
        if resp.status_code == 200:
            for item in resp.json().get("data") or []:
                if _phone_digits(item.get("phone", "")) == digits:
                    return int(item["id"])
    except Exception as e:
        logger.error(f"Поиск клиента не удался: {e}")

    # Запасной путь: обычный список клиентов с фильтром по телефону
    list_url = f"https://api.yclients.com/api/v1/clients/{YCLIENTS_COMPANY_ID}"
    try:
        resp = await http_client().get(list_url, headers=yclients_headers(), params={"phone": digits})
        if resp.status_code == 200:
            for item in resp.json().get("data") or []:
                if _phone_digits(item.get("phone", "")) == digits:
                    return int(item["id"])
    except Exception as e:
        logger.error(f"Список клиентов не получен: {e}")

    return 0


async def register_in_yclients(name: str, phone: str, max_user_id: int) -> tuple:
    """
    Находит клиента в базе YCLIENTS по телефону, а если его там нет — создаёт.

    Сначала ищем, потом создаём: клиент может уже быть в базе салона, хотя в боте
    он впервые. Повторное создание YCLIENTS отклонит, и человек упрётся в ошибку.

    Returns:
        tuple: (успех, yclients_id)
    """
    url = f"https://api.yclients.com/api/v1/clients/{YCLIENTS_COMPANY_ID}"
    payload = {"name": name, "phone": phone, "comment": f"MAX ID: {max_user_id}"}

    try:
        existing_id = await find_client_by_phone(phone)
        if existing_id:
            logger.info(f"Клиент {phone} уже есть в базе, используем id={existing_id}")
            return True, existing_id

        logger.info(f"Отправляем запрос регистрации в YCLIENTS: {name}, {phone}")
        response = await http_client().post(url, json=payload, headers=yclients_headers(True))

        if response.status_code in (200, 201):
            client_id = (response.json().get('data') or {}).get('id')
            if client_id:
                logger.info(f"Клиент успешно зарегистрирован в YCLIENTS: id={client_id}")
                return True, int(client_id)

        # Создать не вышло. Частый случай — клиент всё-таки есть в базе, но
        # поиск его не увидел. Пробуем найти ещё раз, прежде чем сдаваться.
        logger.error(f"Yclients register error: {response.status_code} {response.text[:300]}")
        existing_id = await find_client_by_phone(phone)
        if existing_id:
            logger.info(f"Клиент найден после неудачного создания: id={existing_id}")
            return True, existing_id

        return False, 0
    except Exception as e:
        logger.error(f"Yclients exception: {e}")
        return False, 0


async def get_working_intervals(staff_id: int, date_iso: str) -> list:
    """
    Возвращает рабочие интервалы бани на дату по графику YCLIENTS.

    Returns:
        list: пары (начало, конец) как datetime; пустой список — выходной
              или запрос не удался
    """
    url = (f"https://api.yclients.com/api/v1/schedule/{YCLIENTS_COMPANY_ID}"
           f"/{staff_id}/{date_iso}/{date_iso}")
    try:
        response = await http_client().get(url, headers=yclients_headers())
        if response.status_code != 200:
            logger.error(f"schedule вернул {response.status_code}: {response.text[:300]}")
            return []
        days = response.json().get('data') or []
    except Exception as e:
        logger.error(f"Ошибка получения графика: {e}")
        return []

    intervals = []
    for day in days:
        if not day.get('is_working'):
            continue
        for slot in day.get('slots') or []:
            try:
                start = datetime.strptime(f"{date_iso} {slot['from']}", "%Y-%m-%d %H:%M")
                end = datetime.strptime(f"{date_iso} {slot['to']}", "%Y-%m-%d %H:%M")
                intervals.append((start, end))
            except (KeyError, ValueError):
                logger.warning(f"Странный слот графика: {slot}")

    logger.info(f"График бани {staff_id} на {date_iso}: "
                f"{[(s.strftime('%H:%M'), e.strftime('%H:%M')) for s, e in intervals]}")
    return intervals


async def get_busy_intervals(staff_id: int, date_iso: str) -> tuple:
    """
    Возвращает занятые интервалы бани на дату по журналу записей.

    Returns:
        tuple: (успех, список пар (начало, конец)). Успех False означает, что
               журнал прочитать не удалось — предлагать заходы в этом случае
               нельзя, иначе можно посадить двоих на один сеанс.
    """
    url = f"https://api.yclients.com/api/v1/records/{YCLIENTS_COMPANY_ID}"
    params = {"staff_id": staff_id, "start_date": date_iso, "end_date": date_iso, "count": 200}
    try:
        response = await http_client().get(url, headers=yclients_headers(), params=params)
        if response.status_code != 200:
            logger.error(f"records вернул {response.status_code}: {response.text[:300]}")
            return False, []
        records = response.json().get('data') or []
    except Exception as e:
        logger.error(f"Ошибка получения записей: {e}")
        return False, []

    busy = []
    for record in records:
        if record.get('deleted'):
            continue
        start = _parse_local(record['datetime'])
        busy.append((start, start + timedelta(seconds=int(record.get('seance_length') or 0))))

    logger.info(f"Занято в бане {staff_id} на {date_iso}: "
                f"{[(s.strftime('%H:%M'), e.strftime('%H:%M')) for s, e in busy]}")
    return True, busy


async def get_free_seances(bath_id: str, day_type: str, with_proc: bool, date_iso: str) -> list:
    """
    Считает, какие фиксированные заходы свободны, НЕ спрашивая book_times.

    Клиентский метод book_times у этого филиала отдаёт сетку, которая не
    совпадает ни с графиком бани, ни с настройками услуги. Поэтому занятость
    считаем сами: график минус журнал.

    Returns:
        list: времена начала свободных заходов, например ["10:00", "18:00"]
    """
    staff_id = int(BATHS[bath_id]['staff_id'])
    hours = get_seance_length(bath_id, day_type, with_proc) // 3600

    # График и журнал друг от друга не зависят — спрашиваем оба разом, а не по
    # очереди: шаг «Ищу свободное время» становится вдвое короче.
    working, (ok, busy) = await asyncio.gather(
        get_working_intervals(staff_id, date_iso),
        get_busy_intervals(staff_id, date_iso),
    )
    if not working:
        logger.info(f"Баня {bath_id} на {date_iso} не работает или график не получен")
        return []
    if not ok:
        return []

    free = []
    for start_str in SEANCE_STARTS[bath_id][day_type]:
        start = datetime.strptime(f"{date_iso} {start_str}", "%Y-%m-%d %H:%M")
        end = start + timedelta(hours=hours)
        if not any(w_start <= start and end <= w_end for w_start, w_end in working):
            continue
        # Пересечение интервалов: заход занят, если он с чем-то накладывается.
        # Касание встык (10:00-13:00 и 13:00-16:00) пересечением не считается.
        if any(start < b_end and b_start < end for b_start, b_end in busy):
            continue
        free.append(start_str)

    logger.info(f"Свободные заходы {bath_id}/{day_type} на {date_iso}: {free}")
    return free


async def create_booking(yclients_id: int, staff_id: int, services: list, datetime_str: str,
                         comment: str, seance_length: int) -> bool:
    """
    Создаёт запись в журнале YCLIENTS административным методом.

    Returns:
        bool: получилось ли записать
    """
    url = f"https://api.yclients.com/api/v1/records/{YCLIENTS_COMPANY_ID}"
    payload = {
        "staff_id": staff_id,
        "services": services,
        "client": {"id": yclients_id},
        "datetime": datetime_str,
        "seance_length": seance_length,
        "save_if_busy": False,
        "send_sms": False,
        "comment": comment,
    }
    try:
        response = await http_client().post(url, json=payload, headers=yclients_headers(True))
        if response.status_code in (200, 201):
            logger.info(f"Запись создана на {datetime_str}")
            return True
        logger.error(f"Запись не создана: {response.status_code} {response.text[:300]}")
        return False
    except Exception as e:
        logger.error(f"Ошибка создания записи: {e}")
        return False


# ─────────────────────────── экраны бронирования ───────────────────────────

def booking_header(data: dict) -> str:
    """Шапка «баня / дни / процедуры / гостей» — она повторяется на всех экранах."""
    bath = BATHS[data['bath_id']]
    day_text = "Будни" if data['day_type'] == "weekday" else "Выходные"
    proc_text = "с процедурами" if data['with_procedures'] else "без процедур"
    return (f"Баня: {bath['name']}\nДни: {day_text}\nПроцедуры: {proc_text}\n"
            f"Гостей: {data['guest_count']}")


def bath_screen() -> tuple:
    """Экран выбора бани."""
    buttons = [
        [button(f"🌿 {bath['name']} — {bath['desc']}", f"bath_{bath_id}")]
        for bath_id, bath in BATHS.items()
    ]
    buttons.append([button("❌ Отмена", "book_cancel")])
    return "Выберите баню:", buttons


def day_type_screen(bath_id: str) -> tuple:
    """Экран выбора будни/выходные."""
    bath = BATHS[bath_id]
    buttons = [
        [button("Будни (Пн-Пт)", "day_weekday")],
        [button("Выходные (Сб-Вс)", "day_weekend")],
        [button("⬅️ Назад к баням", "back_bath")],
        [button("❌ Отмена", "book_cancel")],
    ]
    return f"Баня: {bath['name']}\n\nКогда планируете прийти?", buttons


def procedures_question_screen(bath_id: str, day_type: str) -> tuple:
    """Экран «нужны ли процедуры»."""
    bath = BATHS[bath_id]
    day_text = "Будни" if day_type == "weekday" else "Выходные"
    buttons = [
        [button("С парением/процедурами", "proc_yes")],
        [button("Без процедур", "proc_no")],
        [button("⬅️ Назад", "back_day_type")],
        [button("❌ Отмена", "book_cancel")],
    ]
    return f"Баня: {bath['name']}\nДни: {day_text}\n\nНужны процедуры/парение?", buttons


def guest_count_screen(data: dict) -> str:
    """Текст с просьбой назвать количество гостей."""
    bath = BATHS[data['bath_id']]
    day_text = "Будни" if data['day_type'] == "weekday" else "Выходные"
    proc_text = "с процедурами" if data['with_procedures'] else "без процедур"
    return (f"Баня: {bath['name']}\nДни: {day_text}\nПроцедуры: {proc_text}\n\n"
            f"Сколько человек придёт? Напишите число.\n"
            f"Стандартно до {bath['max_guests']} гостей включительно.\n"
            f"Если больше — мы свяжем вас с менеджером для индивидуальной брони.")


def procedures_screen(procedures: list, chosen: list, needed: int, header: str) -> tuple:
    """
    Готовит текст и кнопки выбора процедур.

    Процедур нужно ровно столько же, сколько гостей: по одной на человека.
    Повторы разрешены — трое могут взять одно и то же.
    """
    index = {p["id"]: p for p in procedures}
    picked_lines = [
        f"  • {index[service_id]['title']}" + (f" × {count}" if count > 1 else "")
        for service_id, count in Counter(chosen).items() if service_id in index
    ]

    text = (
        f"{header}\n\n"
        f"Выберите процедуры — по одной на каждого гостя.\n"
        f"Одну и ту же можно взять несколько раз.\n\n"
        f"Выбрано: {len(chosen)} из {needed}"
    )
    if picked_lines:
        text += "\n" + "\n".join(picked_lines)

    buttons = []
    if len(chosen) < needed:
        # Только название: цены и длительность гость уже видел на фото меню
        for procedure in procedures:
            buttons.append([button(procedure['title'], f"pick_{procedure['id']}")])
    if chosen:
        buttons.append([button("↩️ Убрать последнюю", "pick_undo")])
    if len(chosen) == needed:
        buttons.append([button("✅ Дальше, к выбору даты", "pick_done")])
    buttons.append([button("⬅️ Назад", "back_procedures")])
    buttons.append([button("❌ Отмена", "book_cancel")])

    return text, buttons


def build_date_buttons(day_type: str, days_ahead: int = DAYS_AHEAD) -> list:
    """
    Строит кнопки выбора даты, показывая ТОЛЬКО дни, подходящие под выбранный тип.

    Отсчёт идёт с завтрашнего дня: баню нужно успеть протопить, записи «на сегодня»
    через бота не принимаются — за ними отправляем к менеджеру.
    """
    buttons = []
    row = []
    today = _today_local()
    days_ru = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

    for i in range(1, days_ahead + 1):
        day = today + timedelta(days=i)
        if day_type == "weekend" and not is_weekend(day):
            continue
        if day_type == "weekday" and is_weekend(day):
            continue
        day_name = days_ru[day.weekday()]
        row.append(button(day.strftime(f"%d.%m {day_name}"), f"date_{day.strftime('%Y-%m-%d')}"))
        if len(row) == DATE_COLUMNS:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    logger.info(f"Показано дат для day_type={day_type}: {sum(len(r) for r in buttons)}")
    return buttons


def date_screen(data: dict) -> tuple:
    """Экран выбора даты вместе с навигацией."""
    buttons = build_date_buttons(data['day_type'])
    buttons.append([button("⬅️ Назад", "back_procedures")])
    buttons.append([button("❌ Отмена", "book_cancel")])
    return f"{booking_header(data)}\nВыберите дату:", buttons


def build_time_buttons(bath_id: str, day_type: str, with_proc: bool, free_starts: list) -> list:
    """Собирает кнопки под свободные заходы, вида «10:00 – 13:00»."""
    return [
        [button(f"{start} – {seance_end(start, bath_id, day_type, with_proc)}", f"time_{start}")]
        for start in free_starts
    ]


# ─────────────────────────────── фотографии ────────────────────────────────

def _photo_files(*dirs: Path) -> list:
    """Собирает фотографии из папок: сначала первая папка целиком, потом вторая,
    внутри каждой — по имени файла. Отсутствующие папки просто пропускаются."""
    files = []
    for directory in dirs:
        if directory.is_dir():
            files += sorted(
                path for path in directory.glob("*")
                if path.suffix.lower() in (".jpg", ".jpeg", ".png")
            )
    return files


async def photo_tokens(dirs: list, cache_key: str) -> list:
    """
    Токены картинок для вложения, загружая их при первой надобности.

    Токены кэшируются и переживают перезапуск вместе с состоянием: заливать
    одни и те же фото каждому гостю заново — это лишние секунды ожидания.
    """
    cached = PHOTO_TOKENS.get(cache_key)
    if cached:
        return cached

    files = _photo_files(*dirs)
    if not files:
        logger.info(f"Фото не найдены в {', '.join(str(d) for d in dirs)}, пропускаем")
        return []

    tokens = []
    for path in files:
        token = await upload_image(path)
        if token:
            tokens.append(token)

    if tokens:
        PHOTO_TOKENS[cache_key] = tokens
        save_state()
        logger.info(f"Альбом «{cache_key}» загружен в MAX: {len(tokens)} шт.")
    return tokens


async def send_photo_album(chat_id, dirs: list, cache_key: str, caption: str) -> None:
    """
    Отправляет фотографии одним сообщением.

    Медиагрупп, как в телеграме, в MAX нет, зато в сообщение можно вложить
    несколько картинок сразу — выглядит это так же.

    Не отправилось — только пишем в лог: разговор из-за фото прерывать незачем.
    """
    tokens = await photo_tokens(dirs, cache_key)
    if not tokens:
        return
    result = await send_message(chat_id, caption, images=tokens)
    if not result:
        # Токены могли протухнуть — сбрасываем, в следующий раз зальём заново
        PHOTO_TOKENS.pop(cache_key, None)
        save_state()
        logger.error(f"Альбом «{caption}» не отправился, кэш токенов сброшен")


# ───────────────────────── автоматические рассылки ─────────────────────────

def booking_card(booking: dict) -> str:
    """Карточка брони для писем гостю: что, когда и на сколько человек."""
    return (
        f"🌿 Баня: {booking['bath']}\n"
        f"📅 Дата: {format_date_ru(booking['date'])}\n"
        f"⏰ Сеанс: {booking['start']} – {booking['end']}\n"
        f"👥 Гостей: {booking['guests']}\n"
        f"🧖 Банное меню: {booking['procedures']}"
    )


def remember_booking(chat_id, details: dict) -> None:
    """Запоминает бронь, чтобы накануне визита прислать напоминание, а наутро
    после него — попросить об отзыве."""
    BOOKINGS.append({
        "chat_id": chat_id,
        "name": details.get("name", ""),
        "bath": details["bath"],
        "date": details["date"],
        "start": details["start"],
        "end": details["end"],
        "guests": details["guests"],
        "procedures": details.get("procedures", "нет"),
        "reminded": False,
        "feedback_sent": False,
    })
    save_state()
    logger.info(f"Бронь на {details['date']} поставлена в очередь напоминаний (чат {chat_id})")


def forget_old_bookings() -> None:
    """Выбрасывает брони, которые давно прошли, — список не должен расти вечно."""
    if not BOOKINGS:
        return
    edge = (_today_local() - timedelta(days=BOOKINGS_KEEP_DAYS)).strftime("%Y-%m-%d")
    kept = [b for b in BOOKINGS if b.get("date", "") >= edge]
    if len(kept) != len(BOOKINGS):
        logger.info(f"Из очереди напоминаний убрано старых броней: {len(BOOKINGS) - len(kept)}")
        BOOKINGS[:] = kept


async def send_day_before_reminders() -> None:
    """Напоминание накануне визита — всем, у кого баня забронирована на завтра."""
    target = (_today_local() + timedelta(days=1)).strftime("%Y-%m-%d")
    buttons = manager_rows("📞 Связаться с менеджером")

    for booking in BOOKINGS:
        if booking.get("reminded") or booking.get("date") != target:
            continue
        name = booking.get("name") or "друзья"
        text = (
            f"Доброго времени суток, {name}!\n\n"
            f"На завтра у вас забронирована баня:\n"
            f"{booking_card(booking)}\n\n"
            f"Не забудьте взять с собой купальники и тапочки.\n"
            f"До встречи в доме тепла и отдыха «Барница»!"
        )
        if await send_message(booking["chat_id"], text, buttons=buttons or None):
            booking["reminded"] = True
            logger.info(f"Напоминание о завтрашней бане отправлено в чат {booking['chat_id']}")


async def send_feedback_requests() -> None:
    """Просьба об отзыве — наутро после визита."""
    target = (_today_local() - timedelta(days=1)).strftime("%Y-%m-%d")
    buttons = manager_rows("💬 Поделиться впечатлениями") + [[book_button("🌿 Забронировать снова")]]

    for booking in BOOKINGS:
        if booking.get("feedback_sent") or booking.get("date") != target:
            continue
        text = (
            "Доброе утро! Надеемся, что вчера Вам удалось отдохнуть и восстановиться "
            "в нашем банном дворе. Для нас очень важна обратная связь, поэтому просим "
            "вас поделиться впечатлениями у нас в гостях. Благодарим Вас и ждём в гости снова!"
        )
        if await send_message(booking["chat_id"], text, buttons=buttons):
            booking["feedback_sent"] = True
            logger.info(f"Просьба об отзыве отправлена в чат {booking['chat_id']}")


async def scheduler_loop() -> None:
    """
    Расписание рассылок вместо JobQueue, которой в MAX нет.

    Просыпается раз в минуту и смотрит, не пора ли. От повторов защищают не
    хитрые вычисления времени, а флаги внутри самой брони: письмо, которое уже
    ушло, второй раз не отправится. Заодно это чинит перезапуск посреди дня —
    пропущенная рассылка догонится на ближайшей минуте.
    """
    while True:
        try:
            now = datetime.now(LOCAL_TZ).time()
            before = json.dumps(BOOKINGS, ensure_ascii=False)

            if now >= REMINDER_TIME:
                await send_day_before_reminders()
            if now >= FEEDBACK_TIME:
                await send_feedback_requests()
            forget_old_bookings()

            if json.dumps(BOOKINGS, ensure_ascii=False) != before:
                save_state()
        except Exception as e:
            logger.error(f"Ошибка в расписании рассылок: {e}", exc_info=e)

        await asyncio.sleep(60)


# ──────────────────────────── шаги регистрации ─────────────────────────────

async def cmd_start(chat_id, user_id: int, user_name: str) -> None:
    """Команда /start — начало регистрации."""
    data = user_state(user_id)
    clear_booking_data(data)
    data["chat_id"] = chat_id
    data["step"] = STEP_NAME
    save_state()
    logger.info(f"Пользователь {user_id} начал регистрацию")

    await send_message(
        chat_id,
        f"Привет, {user_name}! Добро пожаловать в наш банный комплекс 🧖‍♂️\n\n"
        f"Для бронирования нужно зарегистрироваться.\n\n"
        f"Продолжая, вы даёте согласие на обработку персональных данных "
        f"(имя, телефон) в целях бронирования услуг.\n\n"
        f"Как вас зовут? Напишите имя и фамилию.",
    )


async def step_name(chat_id, user_id: int, text: str) -> None:
    """Получает имя гостя и запрашивает телефон."""
    data = user_state(user_id)
    name = text.strip()
    if len(name) < 2:
        logger.warning(f"Пользователь {user_id} ввёл слишком короткое имя: {name}")
        await send_message(chat_id, "Имя слишком короткое. Введите имя и фамилию:")
        return

    data["name"] = name
    data["step"] = STEP_PHONE
    save_state()
    logger.info(f"Пользователь {user_id} ввёл имя: {name}")
    await send_message(
        chat_id,
        f"Отлично, {name}!\nТеперь отправьте номер телефона в формате +79991234567",
        buttons=[[button("❌ Отменить регистрацию", "book_cancel")]],
    )


async def step_phone(chat_id, user_id: int, text: str) -> None:
    """Получает и проверяет номер телефона."""
    data = user_state(user_id)
    phone = text.strip()
    if not PHONE_REGEX.match(phone):
        logger.warning(f"Пользователь {user_id} ввёл неверный формат телефона: {phone}")
        await send_message(
            chat_id, "Неверный формат 😕\nНомер должен быть +79991234567\nПопробуйте ещё раз:"
        )
        return

    data["phone"] = phone
    data["step"] = STEP_CONFIRM
    save_state()
    logger.info(f"Пользователь {user_id} ввёл телефон: {phone}")
    await send_message(
        chat_id,
        f"Проверьте данные:\n\n👤 Имя: {data['name']}\n📱 Телефон: {phone}\n\nВсё правильно?",
        buttons=[
            [button("✅ Всё верно", "confirm_yes")],
            [button("❌ Изменить", "confirm_no")],
        ],
    )


async def do_register(chat_id, user_id: int, callback_id: str) -> None:
    """Регистрирует гостя в YCLIENTS после подтверждения данных."""
    data = user_state(user_id)
    await answer_callback(callback_id, text="Регистрирую вас... ⏳")

    success, yclients_id = await register_in_yclients(
        name=data.get("name", ""), phone=data.get("phone", ""), max_user_id=user_id
    )
    data.pop("step", None)

    if success:
        data["yclients_id"] = yclients_id
        save_state()
        logger.info(f"Пользователь {user_id} успешно зарегистрирован, yclients_id={yclients_id}")
        await send_message(chat_id, "Готово! Вы зарегистрированы ✅", buttons=[[book_button()]])
    else:
        save_state()
        logger.error(f"Ошибка регистрации пользователя {user_id}")
        await send_message(
            chat_id,
            "Не получилось вас зарегистрировать 😔\n\n"
            "Попробуйте ещё раз или напишите менеджеру:\n"
            f"👤 {MANAGER_NAME}\n📱 {MANAGER_PHONE}",
            buttons=[[start_button("🔄 Попробовать снова")]] + manager_rows(),
        )


# ──────────────────────────── шаги бронирования ────────────────────────────

async def cmd_book(chat_id, user_id: int) -> None:
    """Команда /book — начало бронирования."""
    data = user_state(user_id)
    data["chat_id"] = chat_id

    if "yclients_id" not in data:
        logger.warning(f"Пользователь {user_id} пытается бронировать без регистрации")
        await send_message(
            chat_id,
            "Сначала нужно зарегистрироваться — это одна минута:",
            buttons=[[start_button()]],
        )
        return

    logger.info(f"Пользователь {user_id} начал бронирование")
    clear_booking_data(data)
    save_state()
    text, buttons = bath_screen()
    await send_message(chat_id, text, buttons=buttons)


async def step_guest_count(chat_id, user_id: int, text: str) -> None:
    """Обрабатывает введённое количество гостей."""
    data = user_state(user_id)
    bath = BATHS[data['bath_id']]

    try:
        count = int(text.strip())
    except ValueError:
        logger.warning(f"Пользователь {user_id} ввёл не число в количестве гостей")
        await send_message(chat_id, "Нужно ввести число. Сколько человек придёт?")
        return

    if count > bath['max_guests']:
        logger.info(f"Пользователь {user_id} запросил {count} гостей "
                    f"(больше лимита {bath['max_guests']})")
        await send_message(
            chat_id,
            f"Для компании из {count} человек нужно индивидуальное согласование.\n\n"
            f"Баня «{bath['name']}» рассчитана до {bath['max_guests']} гостей.\n\n"
            f"Нажмите кнопку ниже, и менеджер поможет организовать ваш отдых:",
            buttons=[
                [button("📞 Связаться с менеджером", "contact_manager")],
                [button("⬅️ Изменить количество", "back_guest_count")],
                [button("❌ Отмена", "book_cancel")],
            ],
        )
        return

    if count < 1:
        logger.warning(f"Пользователь {user_id} ввёл некорректное количество гостей: {count}")
        await send_message(chat_id, "Минимум 1 человек. Сколько придёт?")
        return

    data['guest_count'] = count
    data['procedures'] = []
    data.pop("step", None)
    save_state()
    logger.info(f"Пользователь {user_id} указал количество гостей: {count}")

    if data['with_procedures']:
        procedures = await get_procedures()
        if procedures:
            await send_photo_album(chat_id, [MENU_PHOTOS_DIR], "menu", "🧖 Банное меню")
            text, buttons = procedures_screen(procedures, [], count, booking_header(data))
            await send_message(chat_id, text, buttons=buttons)
            return
        # Прайс не отдался — не держим человека на пустом экране, пусть
        # бронирует баню, а процедуры согласует менеджер.
        logger.error("Не удалось получить процедуры, пропускаем шаг выбора")

    text, buttons = date_screen(data)
    await send_message(chat_id, text, buttons=buttons)


async def pick_date(chat_id, user_id: int, callback_id: str, date_iso: str) -> None:
    """Обрабатывает выбор даты и показывает свободные заходы."""
    data = user_state(user_id)
    date_obj = datetime.strptime(date_iso, "%Y-%m-%d")

    # Кнопки строятся с завтрашнего дня, но сообщение могло провисеть до полуночи —
    # тогда вчерашняя кнопка «на завтра» указывает на сегодня. Ловим это здесь.
    if date_obj.date() <= _today_local():
        logger.info(f"Пользователь {user_id} выбрал сегодняшнюю или прошедшую дату {date_iso}")
        await answer_callback(
            callback_id, notification="На сегодня записаться уже нельзя — выберите другой день"
        )
        return

    # Проверяем соответствие выбранных будни/выходные и реальной даты
    selected_weekend = is_weekend(date_obj)
    if ((data['day_type'] == "weekday" and selected_weekend)
            or (data['day_type'] == "weekend" and not selected_weekend)):
        logger.warning(f"Пользователь {user_id} выбрал дату не соответствующую типу дня")
        await answer_callback(
            callback_id, notification="Эта дата не соответствует выбранному типу дней"
        )
        return

    data['book_date'] = date_iso
    save_state()
    bath = BATHS[data['bath_id']]
    logger.info(f"Пользователь {user_id} выбрал дату: {date_iso}")

    await answer_callback(
        callback_id, text=f"{booking_header(data)}\nДата: {date_iso}\n\nИщу свободное время... ⏳"
    )

    free_starts = await get_free_seances(
        data['bath_id'], data['day_type'], data['with_procedures'], date_iso
    )
    buttons = build_time_buttons(
        data['bath_id'], data['day_type'], data['with_procedures'], free_starts
    )

    if not buttons:
        logger.info(f"Нет свободных сеансов для даты {date_iso}")
        await send_message(
            chat_id,
            f"Баня: {bath['name']}\nДата: {date_iso}\n\n"
            f"На эту дату свободных сеансов нет 😔\n\n"
            f"Выберите другой день или свяжитесь с менеджером — "
            f"он подскажет, что можно придумать.",
            buttons=[[button("⬅️ Выбрать другую дату", "back_date")]]
                    + manager_rows()
                    + [[button("❌ Отмена", "book_cancel")]],
        )
        return

    buttons += manager_rows("📞 Не подходит время — написать менеджеру")
    buttons.append([button("⬅️ Другая дата", "back_date")])
    buttons.append([button("❌ Отмена", "book_cancel")])
    await send_message(
        chat_id,
        f"{booking_header(data)}\nДата: {date_iso}\n\nВыберите сеанс:",
        buttons=buttons,
    )


async def pick_time(chat_id, user_id: int, callback_id: str, time_str: str) -> None:
    """Обрабатывает выбор времени и создаёт запись в YCLIENTS."""
    data = user_state(user_id)
    date_iso = data['book_date']
    bath = BATHS[data['bath_id']]
    day_text = "Будни" if data['day_type'] == "weekday" else "Выходные"
    proc_text = "с процедурами" if data['with_procedures'] else "без процедур"
    guests = data['guest_count']

    service_id = get_service_id(data['bath_id'], data['day_type'], data['with_procedures'])
    seance_length = get_seance_length(data['bath_id'], data['day_type'], data['with_procedures'])
    logger.info(f"Пользователь {user_id} выбрал время: {time_str}, создаём запись")

    await answer_callback(
        callback_id, text=f"Бронирую {bath['name']} на {date_iso} в {time_str}... ⏳"
    )

    # Аренда бани плюс процедуры — одной записью, чтобы YCLIENTS сам посчитал
    # стоимость и всё легло в отчёты. Повторы схлопываем в amount.
    chosen = data.get('procedures') or []
    services = [{"id": service_id, "amount": 1}]
    services += [{"id": pid, "amount": count} for pid, count in Counter(chosen).items()]

    index = await get_services_index()
    bath_title = (index.get(service_id) or {}).get("title") or f"Аренда бани «{bath['name']}»"
    lines, total = price_breakdown(
        bath_title, (index.get(service_id) or {}).get("price") or 0, chosen, index
    )
    procedures_text = ", ".join(
        f"{(index.get(pid) or {}).get('title', pid)}" + (f" ×{count}" if count > 1 else "")
        for pid, count in Counter(chosen).items()
    ) or "нет"

    comment = (f"Бронь {bath['name']} через бота MAX\nДни: {day_text}\nПроцедуры: {proc_text}\n"
               f"Гостей: {guests}\nБанное меню: {procedures_text}")

    success = await create_booking(
        yclients_id=int(data['yclients_id']),
        staff_id=int(bath['staff_id']),
        services=services,
        datetime_str=f"{date_iso} {time_str}:00",
        comment=comment,
        seance_length=seance_length,
    )
    end_str = seance_end(time_str, data['bath_id'], data['day_type'], data['with_procedures'])

    if success:
        logger.info(f"Бронирование успешно создано для пользователя {user_id}")
        # Ровно то, что понадобится напоминанию накануне визита, — ни телефона,
        # ни суммы: рассылок администрации у этого бота нет, хранить лишнее незачем.
        details = {
            "name": data.get('name', '—'),
            "bath": bath['name'],
            "date": date_iso,
            "start": time_str,
            "end": end_str,
            "guests": guests,
            "procedures": procedures_text,
        }
        await send_message(
            chat_id,
            f"Готово! Баня забронирована ✅\n\n"
            f"🌿 Баня: {bath['name']}\n"
            f"📅 Дата: {date_iso} ({day_text})\n"
            f"⏰ Сеанс: {time_str} – {end_str}\n"
            f"👥 Гостей: {guests}\n\n"
            + "\n".join(lines) + "\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 Итого: {money(total)}\n\n"
            f"По поводу предоплаты вам в скором времени напишет менеджер.\n\n"
            f"Нужно больше времени? Напишите или позвоните:\n"
            f"👤 {MANAGER_NAME}\n"
            f"📱 {MANAGER_PHONE}\n\n"
            f"Ждём вас!",
            buttons=manager_rows() or None,
        )
        remember_booking(chat_id, details)
    else:
        logger.error(f"Не удалось создать бронирование для пользователя {user_id}")
        await send_message(
            chat_id,
            "Не удалось забронировать 😔\nСеанс уже заняли, либо случилась ошибка.",
            buttons=[[book_button("🔄 Попробовать ещё раз")]] + manager_rows(),
        )

    clear_booking_data(data)
    save_state()


async def pick_procedure(chat_id, user_id: int, callback_id: str, payload: str) -> None:
    """Набор процедур: по одной на гостя, повторы разрешены."""
    data = user_state(user_id)
    chosen = data.setdefault('procedures', [])
    needed = data['guest_count']

    if payload == "pick_done":
        if len(chosen) != needed:
            await answer_callback(callback_id, notification=f"Нужно выбрать ровно {needed}")
            return
        logger.info(f"Пользователь {user_id} выбрал процедуры: {chosen}")
        save_state()
        text, buttons = date_screen(data)
        await answer_callback(callback_id, text=text, buttons=buttons)
        return

    if payload == "pick_undo":
        if chosen:
            chosen.pop()
    else:
        if len(chosen) >= needed:
            await answer_callback(
                callback_id, notification=f"Уже выбрано {needed}, больше не нужно"
            )
            return
        chosen.append(int(payload.split("_")[1]))

    save_state()
    procedures = await get_procedures()
    text, buttons = procedures_screen(procedures, chosen, needed, booking_header(data))
    await answer_callback(callback_id, text=text, buttons=buttons)


# ──────────────────────────── прочие команды ───────────────────────────────

async def cmd_price(chat_id) -> None:
    """Команда /price — прайс-лист: сначала аренда бань, следом банное меню."""
    logger.info(f"Запрошен прайс-лист в чате {chat_id}")
    await send_photo_album(chat_id, [PRICE_PHOTOS_DIR, MENU_PHOTOS_DIR], "price", "💰 Прайс-лист")
    await send_message(
        chat_id,
        "Остались вопросы или готовы бронировать?",
        buttons=[[book_button()]] + manager_rows(),
    )


async def cmd_help(chat_id) -> None:
    """Команда /help — подсказка с кнопками вместо команд."""
    await send_message(
        chat_id,
        "Чем помочь?\n\n"
        "Кнопки ниже делают то же, что команды /start, /book и /cancel — "
        "набирать их руками не обязательно.",
        buttons=[[book_button()], [start_button("📝 Регистрация заново")]] + manager_rows(),
    )


async def cmd_cancel(chat_id, user_id: int) -> None:
    """Команда /cancel — сбрасывает текущий шаг, регистрацию не трогает."""
    data = user_state(user_id)
    clear_booking_data(data)
    save_state()
    logger.info(f"Пользователь {user_id} отменил действие")
    await send_message(
        chat_id,
        "Действие отменено. Что дальше?",
        buttons=[[book_button()], [start_button("📝 Регистрация заново")]],
    )


async def cmd_contact_manager(chat_id, user_id: int, callback_id: str) -> None:
    """Показывает контакты менеджера при слишком большой компании."""
    data = user_state(user_id)
    bath = BATHS[data['bath_id']]
    day_text = "Будни" if data['day_type'] == "weekday" else "Выходные"
    proc_text = "с процедурами" if data['with_procedures'] else "без процедур"

    logger.info(f"Пользователь {user_id} запросил связь с менеджером")
    await answer_callback(
        callback_id,
        text=f"📞 Контакты менеджера:\n\n"
             f"👤 {MANAGER_NAME}\n"
             f"📱 {MANAGER_PHONE}\n\n"
             f"Ваша заявка:\n"
             f"Баня: {bath['name']}\n"
             f"Дни: {day_text}\n"
             f"Процедуры: {proc_text}\n"
             f"Гостей: больше {bath['max_guests']}\n\n"
             f"Позвоните или напишите менеджеру.\n"
             f"Он уже видит ваши данные: {data.get('name', '')}, {data.get('phone', '')}",
        buttons=manager_rows() + [[book_button("🌿 Забронировать заново")]],
    )
    # Чистим только бронь: регистрация должна остаться, иначе гостю придётся
    # заводить себя заново.
    clear_booking_data(data)
    save_state()


# ──────────────────────────────── диспетчер ────────────────────────────────

async def handle_text(chat_id, user_id: int, user_name: str, text: str) -> None:
    """Разбирает обычное сообщение: сперва команды, потом текущий шаг диалога."""
    data = user_state(user_id)
    data["chat_id"] = chat_id
    command = text.strip().lower().split()[0] if text.strip() else ""

    # Команды работают всегда, даже посреди диалога, — как и в телеграм-версии
    if command in ("/start", "start"):
        await cmd_start(chat_id, user_id, user_name)
        return
    if command == "/book":
        await cmd_book(chat_id, user_id)
        return
    if command == "/price":
        await cmd_price(chat_id)
        return
    if command == "/help":
        await cmd_help(chat_id)
        return
    if command == "/cancel":
        await cmd_cancel(chat_id, user_id)
        return

    step = data.get("step")
    if step == STEP_NAME:
        await step_name(chat_id, user_id, text)
    elif step == STEP_PHONE:
        await step_phone(chat_id, user_id, text)
    elif step == STEP_GUEST_COUNT:
        await step_guest_count(chat_id, user_id, text)
    else:
        await send_message(
            chat_id,
            "Не понял 🤔 Выберите действие кнопкой ниже.",
            buttons=[[book_button()], [start_button("📝 Регистрация заново")]] + manager_rows(),
        )


async def handle_callback(chat_id, user_id: int, user_name: str,
                          callback_id: str, payload: str) -> None:
    """Разбирает нажатие кнопки."""
    data = user_state(user_id)
    data["chat_id"] = chat_id

    if payload == "go_start":
        await answer_callback(callback_id)
        await cmd_start(chat_id, user_id, user_name)
        return
    if payload == "go_book":
        await answer_callback(callback_id)
        await cmd_book(chat_id, user_id)
        return

    if payload == "book_cancel":
        logger.info(f"Пользователь {user_id} отменил действие")
        clear_booking_data(data)
        save_state()
        await answer_callback(callback_id, text="Отменено.", buttons=[[book_button()]])
        return

    if payload == "confirm_yes":
        await do_register(chat_id, user_id, callback_id)
        return
    if payload == "confirm_no":
        data["step"] = STEP_NAME
        save_state()
        await answer_callback(callback_id, text="Хорошо, начнём заново. Как вас зовут?")
        return

    # Дальше идут шаги бронирования. Все они опираются на ранее выбранное, и
    # если состояние потерялось (перезапуск, старое сообщение), продолжать
    # нельзя — отправляем начинать заново, а не падаем на отсутствующем ключе.
    if payload.startswith("bath_"):
        data['bath_id'] = payload.split("_")[1]
        save_state()
        logger.info(f"Пользователь {user_id} выбрал баню: {BATHS[data['bath_id']]['name']}")
        text, buttons = day_type_screen(data['bath_id'])
        await answer_callback(callback_id, text=text, buttons=buttons)
        return

    if 'bath_id' not in data:
        await answer_callback(
            callback_id,
            text="Эта кнопка уже неактуальна 🤔 Давайте начнём бронирование заново.",
            buttons=[[book_button()]],
        )
        return

    if payload == "back_bath":
        text, buttons = bath_screen()
        await answer_callback(callback_id, text=text, buttons=buttons)
        return

    if payload in ("day_weekday", "day_weekend"):
        data['day_type'] = "weekday" if payload == "day_weekday" else "weekend"
        save_state()
        logger.info(f"Пользователь {user_id} выбрал дни: {data['day_type']}")
        text, buttons = procedures_question_screen(data['bath_id'], data['day_type'])
        await answer_callback(callback_id, text=text, buttons=buttons)
        return

    if payload == "back_day_type":
        text, buttons = day_type_screen(data['bath_id'])
        await answer_callback(callback_id, text=text, buttons=buttons)
        return

    if payload in ("proc_yes", "proc_no"):
        data['with_procedures'] = payload == "proc_yes"
        data["step"] = STEP_GUEST_COUNT
        save_state()
        logger.info(f"Пользователь {user_id} выбрал процедуры: {data['with_procedures']}")
        await answer_callback(callback_id, text=guest_count_screen(data))
        return

    if payload == "back_procedures":
        data['procedures'] = []
        save_state()
        text, buttons = procedures_question_screen(data['bath_id'], data['day_type'])
        await answer_callback(callback_id, text=text, buttons=buttons)
        return

    if payload == "back_guest_count":
        data["step"] = STEP_GUEST_COUNT
        save_state()
        await answer_callback(callback_id, text=guest_count_screen(data))
        return

    if payload == "contact_manager":
        await cmd_contact_manager(chat_id, user_id, callback_id)
        return

    if payload.startswith("pick_"):
        await pick_procedure(chat_id, user_id, callback_id, payload)
        return

    if payload == "back_date":
        text, buttons = date_screen(data)
        await answer_callback(callback_id, text=text, buttons=buttons)
        return

    if payload.startswith("date_"):
        await pick_date(chat_id, user_id, callback_id, payload.split("_", 1)[1])
        return

    if payload.startswith("time_"):
        await pick_time(chat_id, user_id, callback_id, payload.split("_", 1)[1])
        return

    logger.warning(f"Неизвестное нажатие от {user_id}: {payload}")
    await answer_callback(callback_id, notification="Эта кнопка уже неактуальна 🤔")


async def handle_update(update: dict) -> None:
    """
    Разбирает одно обновление из MAX.

    Ошибки ловим здесь и не даём им уронить цикл опроса: одно кривое сообщение
    не должно останавливать бота для всех остальных.
    """
    update_type = update.get("update_type")
    try:
        if update_type == "message_created":
            message = update.get("message") or {}
            sender = message.get("sender") or {}
            recipient = message.get("recipient") or {}
            body = message.get("body") or {}
            text = body.get("text") or ""
            if not text:
                return  # картинка или стикер без текста — отвечать нечего
            await handle_text(
                recipient.get("chat_id"),
                int(sender.get("user_id")),
                sender.get("name") or "гость",
                text,
            )

        elif update_type == "message_callback":
            callback = update.get("callback") or {}
            user = callback.get("user") or {}
            message = update.get("message") or {}
            recipient = message.get("recipient") or {}
            chat_id = recipient.get("chat_id")
            user_id = int(user.get("user_id"))
            if chat_id is None:
                # Сообщение к нажатию не приложили — берём чат из состояния гостя
                chat_id = user_state(user_id).get("chat_id")
            await handle_callback(
                chat_id,
                user_id,
                user.get("name") or "гость",
                callback.get("callback_id"),
                callback.get("payload") or "",
            )

        elif update_type == "bot_started":
            # Гость только что открыл диалог с ботом — это ровно тот момент,
            # когда уместно поздороваться и предложить регистрацию.
            chat_id = update.get("chat_id")
            user = update.get("user") or {}
            await cmd_start(chat_id, int(user.get("user_id")), user.get("name") or "гость")

        else:
            logger.info(f"Обновление без обработчика: {update_type}")

    except Exception as e:
        logger.error(f"Ошибка обработки обновления {update_type}: {e}", exc_info=e)


async def polling_loop() -> None:
    """
    Долгий опрос обновлений.

    Каждое обновление уходит в отдельную задачу: иначе, пока один гость ждёт
    ответа YCLIENTS, все остальные стоят в очереди и смотрят на висящие кнопки.

    Marker — метка последнего полученного события; без неё MAX прислал бы одно
    и то же по второму разу.
    """
    marker = None
    while True:
        params = {"timeout": UPDATES_TIMEOUT, "limit": 100}
        if marker is not None:
            params["marker"] = marker

        data = await max_request(
            "GET", "/updates", params=params, timeout=UPDATES_REQUEST_TIMEOUT
        )
        if not data:
            # Сеть моргнула или API ответил ошибкой — подождём и попробуем снова,
            # но не бросимся долбить его в цикле без передышки.
            await asyncio.sleep(5)
            continue

        marker = data.get("marker", marker)
        for update in data.get("updates") or []:
            asyncio.create_task(handle_update(update))


async def run() -> None:
    """Поднимает бота: команды, расписание рассылок и опрос обновлений."""
    load_state()
    await set_bot_commands()

    scheduler = asyncio.create_task(scheduler_loop())
    logger.info("Бот MAX запущен")
    try:
        await polling_loop()
    finally:
        scheduler.cancel()
        save_state()
        await close_http_client()


def main() -> None:
    """запуск бота"""
    check_config()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")


if __name__ == "__main__":
    main()
