import asyncio
import logging
import sqlite3
from datetime import datetime
from aiogram import Bot, Dispatcher, html, F
from aiogram.filters import Command, StateFilter
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
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Токен вашего бота (получите у @BotFather)
BOT_TOKEN = "ВАШ_ТЕЛЕГРАМ_ТОКЕН_ЗДЕСЬ"

# Инициализация бота, диспетчера и планировщика
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler()

DB_NAME = "todo_tasks.db"

# --- Работа с Базой Данных (Асинхронные обертки над SQLite) ---
def init_db():
    """Создает таблицы в БД при запуске, если они не существуют"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            reminder_time TEXT NOT NULL,
            is_completed INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

async def add_task(user_id: int, title: str, reminder_time: datetime) -> int:
    """Асинхронное добавление задачи в БД"""
    loop = asyncio.get_running_loop()
    def _db_op():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tasks (user_id, title, reminder_time) VALUES (?, ?, ?)",
            (user_id, title, reminder_time.strftime("%Y-%m-%d %H:%M"))
        )
        task_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return task_id
    return await loop.run_in_executor(None, _db_op)

async def get_active_tasks(user_id: int):
    """Получение списка невыполненных задач пользователя"""
    loop = asyncio.get_running_loop()
    def _db_op():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, title, reminder_time FROM tasks WHERE user_id = ? AND is_completed = 0 ORDER BY reminder_time ASC",
            (user_id,)
        )
        rows = cursor.fetchall()
        conn.close()
        return rows
    return await loop.run_in_executor(None, _db_op)

async def complete_task_db(task_id: int):
    """Пометка задачи как выполненной"""
    loop = asyncio.get_running_loop()
    def _db_op():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET is_completed = 1 WHERE id = ?", (task_id,))
        conn.commit()
        conn.close()
    await loop.run_in_executor(None, _db_op)

async def load_all_active_reminders():
    """Загрузка напоминаний в планировщик при старте бота"""
    loop = asyncio.get_running_loop()
    def _db_op():
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, title, reminder_time FROM tasks WHERE is_completed = 0")
        rows = cursor.fetchall()
        conn.close()
        return rows
    
    tasks = await loop.run_in_executor(None, _db_op)
    now = datetime.now()
    for task_id, user_id, title, reminder_time_str in tasks:
        rem_time = datetime.strptime(reminder_time_str, "%Y-%m-%d %H:%M")
        if rem_time > now:
            schedule_reminder(task_id, user_id, title, rem_time)

# --- Машина состояний (FSM) для добавления задачи ---
class TaskStates(StatesGroup):
    waiting_for_title = State()       # Ожидание ввода названия задачи
    waiting_for_datetime = State()    # Ожидание ввода даты и времени

# --- Клавиатуры ---
def main_menu_keyboard():
    keyboard = [
        [KeyboardButton(text="➕ Добавить задачу"), KeyboardButton(text="📋 Мои задачи")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def task_action_keyboard(task_id: int):
    # Генерация инлайн-кнопок для управления конкретной задачей
    keyboard = [
        [InlineKeyboardButton(text="✅ Выполнено", callback_query_data=f"done_{task_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# --- Логика Уведомлений (APScheduler) ---
async def send_reminder(task_id: int, user_id: int, title: str):
    """Функция, вызываемая планировщиком при наступлении времени напоминания"""
    try:
        # Проверяем, актуальна ли еще задача
        loop = asyncio.get_running_loop()
        def _check():
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.execute("SELECT is_completed FROM tasks WHERE id = ?", (task_id,))
            res = cursor.fetchone()
            conn.close()
            return res[0] if res else 1
        
        is_completed = await loop.run_in_executor(None, _check)
        if is_completed == 0:
            await bot.send_message(
                chat_id=user_id,
                text=f"⏰ <b>Напоминание о задаче!</b>\n\n📌 {html.escape(title)}",
                reply_markup=task_action_keyboard(task_id)
            )
    except Exception as e:
        logging.error(f"Ошибка при отправке напоминания {task_id}: {e}")

def schedule_reminder(task_id: int, user_id: int, title: str, run_date: datetime):
    """Добавление задачи в APScheduler"""
    scheduler.add_job(
        send_reminder,
        trigger="date",
        run_date=run_date,
        args=[task_id, user_id, title],
        id=f"task_{task_id}",
        replace_existing=True
    )

# --- Хэндлеры бота ---

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Приветственный хэндлер"""
    await message.answer(
        f"Привет, {html.escape(message.from_user.first_name)}! 👋\n"
        "Я твой умный менеджер задач с напоминаниями.\n"
        "Используй кнопки ниже для управления своими делами.",
        reply_markup=main_menu_keyboard()
    )

@dp.message(F.text == "➕ Добавить задачу")
async def process_add_task(message: Message, state: FSMContext):
    """Старт процесса создания задачи"""
    await state.set_state(TaskStates.waiting_for_title)
    await message.answer(
        "📝 Введите <b>название задачи</b> (например: 'Позвонить стоматологу' или 'Купить продукты'):"
    )

@dp.message(TaskStates.waiting_for_title)
async def process_title(message: Message, state: FSMContext):
    """Получение названия и переход к вводу времени"""
    if len(message.text) > 100:
        await message.answer("⚠️ Название слишком длинное (макс. 100 символов). Попробуйте еще раз:")
        return
    await state.update_data(title=message.text)
    await state.set_state(TaskStates.waiting_for_datetime)
    await message.answer(
        "📅 Когда вам напомнить?\n"
        "Введите дату и время в формате: <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>\n"
        "Пример: <code>29.06.2026 18:30</code>"
    )

@dp.message(TaskStates.waiting_for_datetime)
async def process_datetime(message: Message, state: FSMContext):
    """Парсинг времени, сохранение задачи в БД и планировщик"""
    try:
        reminder_time = datetime.strptime(message.text.strip(), "%d.%m.%Y %H:%M")
        if reminder_time <= datetime.now():
            await message.answer("⚠️ Время не может быть в прошлом! Введите будущую дату и время:")
            return
    except ValueError:
        await message.answer("⚠️ Неверный формат! Пожалуйста, используйте шаблон: <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>")
        return

    # Извлекаем название из состояния
    user_data = await state.get_data()
    title = user_data["title"]
    user_id = message.from_user.id

    # Запись в БД
    task_id = await add_task(user_id, title, reminder_time)
    
    # Добавление в планировщик напоминаний
    schedule_reminder(task_id, user_id, title, reminder_time)

    await state.clear()
    await message.answer(
        f"✅ <b>Задача успешно добавлена!</b>\n\n"
        f"📌 Задача: {html.escape(title)}\n"
        f"⏰ Напоминание: {reminder_time.strftime('%d.%m.%Y в %H:%M')}",
        reply_markup=main_menu_keyboard()
    )

@dp.message(F.text == "📋 Мои задачи")
async def show_tasks(message: Message):
    """Показ всех активных задач пользователя"""
    tasks = await get_active_tasks(message.from_user.id)
    if not tasks:
        await message.answer("🎉 У вас нет активных задач! Самое время добавить новую.")
        return

    text = "<b>📋 Список ваших активных задач:</b>\n\n"
    for idx, (task_id, title, rem_time_str) in enumerate(tasks, 1):
        rem_time = datetime.strptime(rem_time_str, "%Y-%m-%d %H:%M")
        formatted_time = rem_time.strftime("%d.%m.%Y %H:%M")
        text += f"{idx}. 📌 <b>{html.escape(title)}</b>\n⏰ Напоминание: {formatted_time}\n/done_{task_id} — Выполнить\n\n"
    
    await message.answer(text)

@dp.message(F.text.startswith("/done_"))
async def cmd_complete_task(message: Message):
    """Выполнение задачи по быстрой команде /done_ID"""
    try:
        task_id = int(message.text.split("_")[1])
        await complete_task_db(task_id)
        
        # Удаляем задачу из планировщика, если она там есть
        job_id = f"task_{task_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            
        await message.answer("🎉 Отлично! Задача отмечена как выполненная.")
    except Exception:
        await message.answer("⚠️ Не удалось найти или выполнить задачу. Попробуйте еще раз.")

@dp.query_handler(F.data.startswith("done_"))
async def callback_complete_task(callback: CallbackQuery):
    """Выполнение задачи из инлайн-уведомления"""
    try:
        task_id = int(callback.data.split("_")[1])
        await complete_task_db(task_id)
        
        # Удаляем из планировщика
        job_id = f"task_{task_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)

        await callback.answer("Задача выполнена!")
        await callback.message.edit_text(
            f"{callback.message.text}\n\n✅ <b>Выполнено!</b>"
        )
    except Exception as e:
        logging.error(f"Ошибка в callback: {e}")
        await callback.answer("Ошибка при обработке запроса", show_alert=True)

# --- Запуск бота ---
async def main():
    logging.basicConfig(level=logging.INFO)
    init_db() # Инициализация БД
    
    scheduler.start() # Запуск планировщика алертов
    await load_all_active_reminders() # Подгрузка старых напоминаний
    
    logging.info("Менеджер задач успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())