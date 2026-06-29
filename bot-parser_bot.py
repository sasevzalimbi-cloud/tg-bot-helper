import asyncio
import logging
import sqlite3
import urllib.parse
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, html, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, 
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    CallbackQuery
)

# Токен вашего бота (получите у @BotFather)
BOT_TOKEN = "ВАШ_ТЕЛЕГРАМ_ТОКЕН_ЗДЕСЬ"

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_NAME = "parser_subscriptions.db"

# Заголовки для запросов, чтобы сайты не блокировали бота
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# --- Инициализация и работа с БД (Реализация связи Many-to-Many) ---
def init_db():
    """Создает таблицы базы данных со связями Многие-ко-Многим"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # 1. Таблица пользователей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT
        )
    """)
    
    # 2. Таблица уникальных целей для парсинга (уникальная пара тип + поисковый запрос)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,          -- 'habr_career' или 'steam_deals'
            query TEXT NOT NULL,         -- поисковый запрос (например, 'python' или 'witcher')
            UNIQUE(type, query)
        )
    """)
    
    # 3. Таблица связей Многие-ко-Многим (пользователи <-> подписки)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_targets (
            user_id INTEGER,
            target_id INTEGER,
            PRIMARY KEY (user_id, target_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
        )
    """)
    
    # 4. Таблица отправленных постов/объявлений (чтобы не спамить дубликатами)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sent_items (
            target_id INTEGER,
            item_key TEXT,               -- Уникальный хэш или URL вакансии/игры
            PRIMARY KEY (target_id, item_key),
            FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
        )
    """)
    
    conn.commit()
    conn.close()

# --- Асинхронные обертки над SQLite операциями ---
async def register_user(user_id: int, username: str):
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO users (id, username) VALUES (?, ?)", (user_id, username))
        conn.commit()
        conn.close()
    await asyncio.to_thread(_db)

async def subscribe_user(user_id: int, target_type: str, query: str):
    """Связывает пользователя с целью парсинга (создает цель, если её нет)"""
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Вставляем или получаем существующую цель
        cursor.execute(
            "INSERT OR IGNORE INTO targets (type, query) VALUES (?, ?)", 
            (target_type, query.lower().strip())
        )
        cursor.execute(
            "SELECT id FROM targets WHERE type = ? AND query = ?", 
            (target_type, query.lower().strip())
        )
        target_id = cursor.fetchone()[0]
        
        # Связываем пользователя с целью
        cursor.execute(
            "INSERT OR IGNORE INTO user_targets (user_id, target_id) VALUES (?, ?)",
            (user_id, target_id)
        )
        conn.commit()
        conn.close()
    await asyncio.to_thread(_db)

async def get_user_subscriptions(user_id: int):
    """Возвращает список активных подписок пользователя"""
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ut.target_id, t.type, t.query 
            FROM user_targets ut
            JOIN targets t ON ut.target_id = t.id
            WHERE ut.user_id = ?
        """, (user_id,))
        rows = cursor.fetchall()
        conn.close()
        return rows
    return await asyncio.to_thread(_db)

async def unsubscribe_user(user_id: int, target_id: int):
    """Удаляет связь пользователя с целью парсинга"""
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM user_targets WHERE user_id = ? AND target_id = ?",
            (user_id, target_id)
        )
        
        # Очистка сиротских целей (на которые больше никто не подписан)
        cursor.execute("""
            DELETE FROM targets 
            WHERE id NOT IN (SELECT DISTINCT target_id FROM user_targets)
        """)
        conn.commit()
        conn.close()
    await asyncio.to_thread(_db)

async def check_and_add_sent_item(target_id: int, item_key: str) -> bool:
    """Проверяет, отправлялся ли ранее элемент. Если нет — записывает его"""
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM sent_items WHERE target_id = ? AND item_key = ?", 
            (target_id, item_key)
        )
        exists = cursor.fetchone() is not None
        if not exists:
            cursor.execute(
                "INSERT INTO sent_items (target_id, item_key) VALUES (?, ?)", 
                (target_id, item_key)
            )
            conn.commit()
        conn.close()
        return exists
    return await asyncio.to_thread(_db)

async def get_subscribers_for_target(target_id: int):
    """Получает всех ID пользователей, подписанных на конкретную цель"""
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM user_targets WHERE target_id = ?", (target_id,))
        users = [row[0] for row in cursor.fetchall()]
        conn.close()
        return users
    return await asyncio.to_thread(_db)

async def get_all_active_targets():
    """Получает вообще все уникальные цели из БД для парсинга в фоновом режиме"""
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT id, type, query FROM targets")
        rows = cursor.fetchall()
        conn.close()
        return rows
    return await asyncio.to_thread(_db)


# --- Модули Парсинга (Scrapers) ---

async def parse_habr_career(query: str) -> list:
    """Асинхронный парсинг вакансий с Хабр Карьеры"""
    encoded_query = urllib.parse.quote(query)
    url = f"https://career.habr.com/vacancies?q={encoded_query}&type=all"
    
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    return []
                html_data = await response.text()
        except Exception as e:
            logging.error(f"Ошибка запроса к Habr Career ({query}): {e}")
            return []

    soup = BeautifulSoup(html_data, "html.parser")
    cards = soup.select(".vacancy-card")
    results = []

    for card in cards:
        try:
            title_el = card.select_one(".vacancy-card__title-link")
            title = title_el.text.strip()
            link = "https://career.habr.com" + title_el["href"]
            
            company_el = card.select_one(".vacancy-card__company-title a")
            company = company_el.text.strip() if company_el else "Компания не указана"
            
            salary_el = card.select_one(".vacancy-card__salary")
            salary = salary_el.text.strip() if salary_el else "З/П не указана"

            # Уникальным ключом выступает ссылка на вакансию
            results.append({
                "key": link,
                "title": title,
                "meta": f"🏢 Компания: {company}\n💰 Оклад: {salary}",
                "link": link
            })
        except Exception as ex:
            logging.error(f"Ошибка парсинга карточки Хабра: {ex}")
            continue
            
    return results

async def parse_steam_deals(query: str) -> list:
    """Асинхронный парсинг скидок с магазина Steam"""
    encoded_query = urllib.parse.quote(query)
    url = f"https://store.steampowered.com/search/?specials=1&term={encoded_query}"
    
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    return []
                html_data = await response.text()
        except Exception as e:
            logging.error(f"Ошибка запроса к Steam ({query}): {e}")
            return []

    soup = BeautifulSoup(html_data, "html.parser")
    # Получаем первые 5 результатов поиска
    search_results = soup.select("#search_resultsRows a")[:5]
    results = []

    for item in search_results:
        try:
            link = item["href"].split("?")[0]  # Очищаем URL от реф-хвостов
            title = item.select_one(".title").text.strip()
            
            discount_pct_el = item.select_one(".discount_pct")
            if not discount_pct_el:
                continue # Если нет скидки, пропускаем
                
            discount_pct = discount_pct_el.text.strip()
            
            # Парсим цены
            prices_div = item.select_one(".discount_prices")
            original_price = prices_div.select_one(".discount_original_price").text.strip()
            discounted_price = prices_div.select_one(".discount_final_price").text.strip()

            results.append({
                "key": link,
                "title": title,
                "meta": f"📉 Скидка: {discount_pct}\n❌ Старая цена: {original_price}\n🔥 Новая цена: {discounted_price}",
                "link": link
            })
        except Exception as ex:
            logging.error(f"Ошибка парсинга элемента Steam: {ex}")
            continue

    return results


# --- FSM: Состояния подписки ---
class SubscribeStates(StatesGroup):
    waiting_for_type = State()   # Выбор площадки
    waiting_for_query = State()  # Ввод ключевого слова/игры

# --- Клавиатуры ---
def main_keyboard():
    kb = [
        [KeyboardButton(text="🔔 Добавить подписку"), KeyboardButton(text="📋 Мои подписки")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def category_keyboard():
    kb = [
        [InlineKeyboardButton(text="💼 Хабр Карьера (Вакансии)", callback_query_data="type_habr_career")],
        [InlineKeyboardButton(text="🎮 Steam (Скидки на игры)", callback_query_data="type_steam_deals")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# --- Хэндлеры бота ---

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await register_user(message.from_user.id, message.from_user.username)
    await message.answer(
        f"Привет, {html.escape(message.from_user.first_name)}! 👋\n\n"
        "Я бот-парсер мониторинга.\n"
        "Я могу регулярно сканировать <b>Хабр Карьеру</b> на наличие новых вакансий по вашим тегам "
        "или искать горячие скидки на игры в <b>Steam</b>.\n\n"
        "Используйте меню для создания подписок!",
        reply_markup=main_keyboard()
    )

@dp.message(F.text == "🔔 Добавить подписку")
async def add_subscription_start(message: Message, state: FSMContext):
    await state.set_state(SubscribeStates.waiting_for_type)
    await message.answer("Выберите площадку для мониторинга:", reply_markup=category_keyboard())

@dp.callback_query(SubscribeStates.waiting_for_type, F.data.startswith("type_"))
async def add_subscription_type(callback: CallbackQuery, state: FSMContext):
    target_type = callback.data.split("_")[1] + "_" + callback.data.split("_")[2]
    await state.update_data(target_type=target_type)
    await state.set_state(SubscribeStates.waiting_for_query)
    
    prompt = (
        "Введите ключевое слово для поиска вакансий (например: <code>python</code>):"
        if target_type == "habr_career" else
        "Введите название игры для отслеживания скидки (например: <code>cyberpunk</code>):"
    )
    
    await callback.message.edit_text(prompt)
    await callback.answer()

@dp.message(SubscribeStates.waiting_for_query)
async def add_subscription_query(message: Message, state: FSMContext):
    query = message.text.strip()
    if len(query) < 2 or len(query) > 50:
        await message.answer("⚠️ Длина запроса должна быть от 2 до 50 символов. Попробуйте снова:")
        return

    data = await state.get_data()
    target_type = data["target_type"]
    user_id = message.from_user.id
    
    # Регистрируем пользователя и создаем подписку
    await register_user(user_id, message.from_user.username)
    await subscribe_user(user_id, target_type, query)
    await state.clear()
    
    display_type = "Хабр Карьера" if target_type == "habr_career" else "Steam Скидки"
    await message.answer(
        f"✅ <b>Подписка оформлена!</b>\n"
        f"Площадка: <code>{display_type}</code>\n"
        f"Запрос: <code>{query}</code>\n\n"
        f"Я пришлю уведомление, как только найду свежие обновления!",
        reply_markup=main_keyboard()
    )

@dp.message(F.text == "📋 Мои подписки")
async def list_subscriptions(message: Message):
    subs = await get_user_subscriptions(message.from_user.id)
    if not subs:
        await message.answer("ℹ️ У вас пока нет активных подписок.")
        return

    text = "<b>📋 Ваши подписки:</b>\n"
    keyboard_buttons = []
    
    for target_id, t_type, query in subs:
        display_type = "💼 Хабр" if t_type == "habr_career" else "🎮 Steam"
        text += f"\n• {display_type}: <code>{query}</code>"
        
        # Кнопка для удаления подписки
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"❌ Удалить {display_type} ({query})", 
                callback_query_data=f"unsub_{target_id}"
            )
        ])
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    await message.answer(text, reply_markup=markup)

@dp.callback_query(F.data.startswith("unsub_"))
async def remove_subscription(callback: CallbackQuery):
    target_id = int(callback.data.split("_")[1])
    await unsubscribe_user(callback.from_user.id, target_id)
    await callback.answer("Подписка удалена!")
    await callback.message.edit_text("✅ Подписка успешно удалена.")


# --- Фоновая периодическая задача парсинга (Asyncio Loop) ---

async def parse_target_task(target_id: int, target_type: str, query: str, semaphore: asyncio.Semaphore):
    """Задача парсинга конкретной цели с ограничением конкурентности"""
    async with semaphore:
        logging.info(f"Запуск парсинга [{target_type}] по запросу '{query}'...")
        
        # Выбираем соответствующий парсер
        if target_type == "habr_career":
            items = await parse_habr_career(query)
        elif target_type == "steam_deals":
            items = await parse_steam_deals(query)
        else:
            items = []

        if not items:
            return

        # Получаем список подписчиков этой цели
        subscribers = await get_subscribers_for_target(target_id)
        if not subscribers:
            return

        for item in items:
            # Проверяем, отправляли ли мы этот элемент по этой подписке ранее
            already_sent = await check_and_add_sent_item(target_id, item["key"])
            if not already_sent:
                # Если элемент новый — отправляем его всем подписчикам
                for user_id in subscribers:
                    try:
                        title_escaped = html.escape(item["title"])
                        meta_escaped = html.escape(item["meta"])
                        
                        message_text = (
                            f"🔔 <b>Найдены новые обновления!</b>\n\n"
                            f"📣 <a href='{item['link']}'><b>{title_escaped}</b></a>\n"
                            f"{meta_escaped}\n\n"
                            f"🔍 Подписка: <code>{query}</code>"
                        )
                        await bot.send_message(
                            chat_id=user_id, 
                            text=message_text, 
                            disable_web_page_preview=False
                        )
                        # Небольшая пауза во избежание флуда в Telegram API
                        await asyncio.sleep(0.05)
                    except Exception as err:
                        logging.error(f"Не удалось отправить сообщение пользователю {user_id}: {err}")

async def parsing_scheduler_loop():
    """Фоновый шедулер, выполняющий проверку каждые N минут"""
    # Семафор ограничивает количество одновременных запросов к сайтам (макс. 3 запроса параллельно)
    # Это предотвращает блокировку IP-адреса хостинга
    semaphore = asyncio.Semaphore(3)
    
    while True:
        try:
            targets = await get_all_active_targets()
            if targets:
                logging.info(f"Начало цикла парсинга. Всего уникальных целей: {len(targets)}")
                
                # Создаем список асинхронных задач парсинга
                tasks = []
                for target_id, target_type, query in targets:
                    tasks.append(parse_target_task(target_id, target_type, query, semaphore))
                
                # Запускаем задачи параллельно
                await asyncio.gather(*tasks)
                
            logging.info("Цикл парсинга завершен. Сон 10 минут...")
            await asyncio.sleep(600)  # Сон 10 минут перед следующим циклом
            
        except Exception as ex:
            logging.error(f"Ошибка в планировщике парсинга: {ex}")
            await asyncio.sleep(60)


# --- Старт приложения ---
async def main():
    logging.basicConfig(level=logging.INFO)
    init_db() # Подготовка Базы данных
    
    # Запускаем фоновый цикл парсинга в Event Loop без блокировки основного бота
    asyncio.create_task(parsing_scheduler_loop())
    
    logging.info("Бот-парсер успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())