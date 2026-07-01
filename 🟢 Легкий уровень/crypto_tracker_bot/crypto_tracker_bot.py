import asyncio
import logging
import sqlite3
import aiohttp
import matplotlib.pyplot as plt
from io import BytesIO
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
    CallbackQuery,
    BufferedInputFile
)

# Токен вашего бота (получите у @BotFather)
BOT_TOKEN = "ВАШ_ТЕЛЕГРАМ_ТОКЕН_ЗДЕСЬ"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

DB_NAME = "crypto_tracker.db"
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# --- Инициализация БД (Хранение алертов) ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            coin_id TEXT NOT NULL,
            target_price REAL NOT NULL,
            alert_type TEXT NOT NULL, -- 'above' (выше) или 'below' (ниже)
            is_active INTEGER DEFAULT 1
        )
    """)
    conn.commit()
    conn.close()

async def add_alert_db(user_id: int, coin_id: str, target_price: float, alert_type: str):
    loop = asyncio.get_running_loop()
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO alerts (user_id, coin_id, target_price, alert_type) VALUES (?, ?, ?, ?)",
            (user_id, coin_id, target_price, alert_type)
        )
        conn.commit()
        conn.close()
    await loop.run_in_executor(None, _db)

async def get_active_alerts_db():
    loop = asyncio.get_running_loop()
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, coin_id, target_price, alert_type FROM alerts WHERE is_active = 1")
        rows = cursor.fetchall()
        conn.close()
        return rows
    return await loop.run_in_executor(None, _db)

async def deactivate_alert_db(alert_id: int):
    loop = asyncio.get_running_loop()
    def _db():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("UPDATE alerts SET is_active = 0 WHERE id = ?", (alert_id,))
        conn.commit()
        conn.close()
    await loop.run_in_executor(None, _db)

# --- Асинхронная работа с внешними API ---
async def fetch_price_data(coin_id: str) -> dict:
    """Получает актуальные цены с CoinGecko (Simple Price)"""
    url = f"{COINGECKO_BASE_URL}/simple/price?ids={coin_id}&vs_currencies=usd&include_24hr_change=true"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
                return {}
        except Exception as e:
            logging.error(f"Ошибка запроса к CoinGecko Simple Price: {e}")
            return {}

async def fetch_historical_data(coin_id: str, days: int = 7) -> list:
    """Получает исторические цены за указанный интервал для построения графика"""
    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart?vs_currency=usd&days={days}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    # Возвращаем список кортежей [timestamp, price]
                    return data.get("prices", [])
                return []
        except Exception as e:
            logging.error(f"Ошибка получения исторических данных: {e}")
            return []

# --- Машина состояний (FSM) для Алертов ---
class AlertStates(StatesGroup):
    waiting_for_coin = State()
    waiting_for_direction = State()
    waiting_for_price = State()

# --- Кнопки управления ---
def main_menu():
    kb = [
        [KeyboardButton(text="🪙 Курсы валют"), KeyboardButton(text="🔔 Настроить Алерт")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def coin_selection_kb():
    kb = [
        [InlineKeyboardButton(text="Bitcoin (BTC)", callback_query_data="coin_bitcoin")],
        [InlineKeyboardButton(text="Ethereum (ETH)", callback_query_data="coin_ethereum")],
        [InlineKeyboardButton(text="Toncoin (TON)", callback_query_data="coin_the-open-network")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- Построение графика изменений (Matplotlib) ---
def generate_chart(historical_prices: list, coin_title: str) -> BytesIO:
    """Генерирует график во временном буфере без сохранения на диск"""
    # Разделяем timestamps и значения цен
    times = [item[0] for item in historical_prices]
    prices = [item[1] for item in historical_prices]
    
    # Преобразуем timestamps в читаемый формат
    import datetime
    dates = [datetime.datetime.fromtimestamp(t / 1000.0) for t in times]
    
    plt.figure(figsize=(8, 4))
    plt.plot(dates, prices, color='#0088cc', linewidth=2, label=f'{coin_title} (USD)')
    
    # Красивое форматирование графика
    plt.title(f"Динамика курса {coin_title.capitalize()} за 7 дней", fontsize=14, fontweight='bold', pad=15)
    plt.xlabel("Дата", fontsize=10)
    plt.ylabel("Цена в USD", fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.xticks(rotation=30)
    plt.tight_layout()
    
    # Сохраняем график в байт-объект в памяти
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close() # Очистка ресурсов matplotlib
    return buf

# --- Фоновая задача проверки алертов пользователей ---
async def check_alerts_loop():
    """Фоновый цикл для периодической проверки цен на рынке и отправки алертов"""
    while True:
        try:
            alerts = await get_active_alerts_db()
            if alerts:
                # Собираем уникальные ID коинов, которые надо проверить
                unique_coins = list(set([a[2] for a in alerts]))
                
                # Запрашиваем цены одной пачкой для оптимизации лимитов API
                ids_param = ",".join(unique_coins)
                prices_data = await fetch_price_data(ids_param)
                
                if prices_data:
                    for alert_id, user_id, coin_id, target_price, alert_type in alerts:
                        coin_info = prices_data.get(coin_id)
                        if not coin_info:
                            continue
                        
                        current_price = coin_info.get("usd")
                        trigger_alert = False
                        
                        if alert_type == "above" and current_price >= target_price:
                            trigger_alert = True
                        elif alert_type == "below" and current_price <= target_price:
                            trigger_alert = True
                            
                        if trigger_alert:
                            direction_str = "вырос выше" if alert_type == "above" else "упал ниже"
                            # Отправляем оповещение пользователю
                            await bot.send_message(
                                chat_id=user_id,
                                text=f"🚨 <b>Сработал триггер цены!</b>\n\n"
                                     f"🪙 Монета: <b>{coin_id.upper()}</b>\n"
                                     f"📈 Курс {direction_str} установленной вами отметки в <b>${target_price:,.2f}</b>!\n"
                                     f"💰 Текущая цена: <b>${current_price:,.2f}</b>"
                                )
                            # Деактивируем алерт, чтобы не слать спам
                            await deactivate_alert_db(alert_id)
                            
            # Проверяем каждые 60 секунд (для продакшна можно выставить реже из-за лимитов бесплатного API)
            await asyncio.sleep(60)
        except Exception as e:
            logging.error(f"Ошибка в фоновом цикле алертов: {e}")
            await asyncio.sleep(10)

# --- Хэндлеры бота ---

@dp.message(Command("start"))
async def start_cmd(message: Message):
    await message.answer(
        f"Привет, {html.escape(message.from_user.first_name)}! 👋\n"
        "Я трекер цен криптовалют. Я могу показывать актуальные курсы с графиками и уведомлять тебя о движении цен.",
        reply_markup=main_menu()
    )

@dp.message(F.text == "🪙 Курсы валют")
async def show_rates_menu(message: Message):
    await message.answer("Выберите интересующую вас монету для аналитики:", reply_markup=coin_selection_kb())

@dp.callback_query(F.data.startswith("coin_"))
async def handle_coin_info(callback: CallbackQuery):
    coin_id = callback.data.split("_")[1]
    
    # Оповещаем пользователя, что идет загрузка данных
    await callback.answer("Загружаю данные и график...")
    
    # Запрос данных по API
    price_info = await fetch_price_data(coin_id)
    historical_prices = await fetch_historical_data(coin_id, days=7)
    
    if not price_info:
        await callback.message.answer("❌ Не удалось получить текущие котировки. Попробуйте позже.")
        return
    
    current_price = price_info[coin_id]["usd"]
    change_24h = price_info[coin_id].get("usd_24h_change", 0)
    emoji_change = "📈" if change_24h >= 0 else "📉"
    
    text = (
        f"🪙 <b>Информация о {coin_id.upper()}:</b>\n\n"
        f"💵 Текущая цена: <b>${current_price:,.2f}</b>\n"
        f"{emoji_change} Изменение за 24ч: <b>{change_24h:+.2f}%</b>"
    )
    
    # Генерация и отправка графика
    if historical_prices:
        chart_buffer = generate_chart(historical_prices, coin_id)
        input_file = BufferedInputFile(chart_buffer.read(), filename=f"{coin_id}_chart.png")
        await callback.message.answer_photo(
            photo=input_file,
            caption=text
        )
    else:
        await callback.message.answer(text)

# --- FSM: Процесс настройки алерта ---

@dp.message(F.text == "🔔 Настроить Алерт")
async def process_alert_setup(message: Message, state: FSMContext):
    await state.set_state(AlertStates.waiting_for_coin)
    await message.answer(
        "Выберите монету для отслеживания:",
        reply_markup=coin_selection_kb()
    )

@dp.callback_query(AlertStates.waiting_for_coin, F.data.startswith("coin_"))
async def process_alert_coin(callback: CallbackQuery, state: FSMContext):
    coin_id = callback.data.split("_")[1]
    await state.update_data(coin_id=coin_id)
    
    # Переход к выбору направления
    await state.set_state(AlertStates.waiting_for_direction)
    
    kb = [
        [InlineKeyboardButton(text="🚀 Выше уровня", callback_query_data="dir_above")],
        [InlineKeyboardButton(text="🩸 Ниже уровня", callback_query_data="dir_below")]
    ]
    direction_markup = InlineKeyboardMarkup(inline_keyboard=kb)
    
    await callback.message.edit_text(
        f"Вы выбрали {coin_id.upper()}.\nКогда вас уведомить?",
        reply_markup=direction_markup
    )
    await callback.answer()

@dp.callback_query(AlertStates.waiting_for_direction, F.data.startswith("dir_"))
async def process_alert_direction(callback: CallbackQuery, state: FSMContext):
    direction = callback.data.split("_")[1]
    await state.update_data(direction=direction)
    
    await state.set_state(AlertStates.waiting_for_price)
    
    direction_text = "поднимется выше" if direction == "above" else "упадет ниже"
    await callback.message.edit_text(
        f"Введите целевую цену в USD (дробная часть через точку).\n"
        f"Бот оповестит вас, когда монета {direction_text} этого значения.\n"
        f"Пример: <code>61250.50</code>"
    )
    await callback.answer()

@dp.message(AlertStates.waiting_for_price)
async def process_alert_price(message: Message, state: FSMContext):
    try:
        target_price = float(message.text.replace(",", "."))
        if target_price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Неверный формат цены! Введите положительное число:")
        return
        
    user_data = await state.get_data()
    coin_id = user_data["coin_id"]
    direction = user_data["direction"]
    
    # Сохраняем в БД
    await add_alert_db(message.from_user.id, coin_id, target_price, direction)
    await state.clear()
    
    direction_text = "вырастет выше" if direction == "above" else "упадет ниже"
    await message.answer(
        f"✅ <b>Алерт успешно установлен!</b>\n\n"
        f"🪙 Монета: {coin_id.upper()}\n"
        f"🎯 Условие: Если цена {direction_text} <b>${target_price:,.2f}</b>",
        reply_markup=main_menu()
    )

# --- Запуск ---
async def main():
    logging.basicConfig(level=logging.INFO)
    init_db() # Инициализация таблиц БД
    
    # Запускаем фоновую задачу проверки триггеров алертов в Event Loop
    asyncio.create_task(check_alerts_loop())
    
    logging.info("Крипто-трекер успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())