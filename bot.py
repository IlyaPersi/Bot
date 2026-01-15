import asyncio
import logging
import sqlite3
import os
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    ReplyKeyboardMarkup, 
    KeyboardButton,
    Message
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
import hashlib

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация бота
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
if not TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN не установлен!")
    raise ValueError("Установите TELEGRAM_BOT_TOKEN в переменных окружения")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Партнерские ссылки (только 3 платформы)
PARTNER_LINKS = {
    'skillbox': 'https://l.skbx.pro/DQLFW6',
    'skillfactory': 'https://go.redav.online/26e5202921d69dd1',
    'geekbrains': 'https://go.redav.online/17d53d9e858961e1',
}

# Названия платформ для красивого отображения
PLATFORM_NAMES = {
    'skillbox': 'Skillbox 🎓',
    'skillfactory': 'SkillFactory 🚀',
    'geekbrains': 'GeekBrains 👨‍💻'
}

# Комиссии по платформам
COMMISSIONS = {
    'skillbox': '20-40%',
    'skillfactory': '20-35%',
    'geekbrains': '15-30%'
}

# База данных
DB_NAME = "courses_bot.db"

def get_db_connection():
    """Создание соединения с БД"""
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Инициализация базы данных"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Таблица пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            clicks_count INTEGER DEFAULT 0,
            ref_code TEXT UNIQUE,
            referrer_id INTEGER,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица кликов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            platform TEXT NOT NULL,
            course_id INTEGER,
            clicked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
        )
    ''')
    
    # Таблица курсов (кеш)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS courses_cache (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            platform TEXT NOT NULL,
            category TEXT NOT NULL,
            data TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Индексы для оптимизации
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram ON users(telegram_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_clicks_user ON clicks(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_clicks_platform ON clicks(platform)')
    
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def add_user(telegram_id: int, username: str, first_name: str, last_name: str, referrer_id: int = None):
    """Добавление пользователя в БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Генерация реферального кода
        ref_code = hashlib.md5(f"{telegram_id}{datetime.now().timestamp()}".encode()).hexdigest()[:8]
        
        cursor.execute('''
            INSERT OR IGNORE INTO users 
            (telegram_id, username, first_name, last_name, ref_code, referrer_id, last_active)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (telegram_id, username, first_name, last_name, ref_code, referrer_id))
        
        if cursor.rowcount > 0:
            logger.info(f"Новый пользователь: {telegram_id} ({username})")
        
        conn.commit()
        return ref_code
    except Exception as e:
        logger.error(f"Ошибка добавления пользователя: {e}")
        return None
    finally:
        conn.close()

def update_user_activity(telegram_id: int):
    """Обновление времени последней активности"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE users 
            SET last_active = CURRENT_TIMESTAMP 
            WHERE telegram_id = ?
        ''', (telegram_id,))
        conn.commit()
    except Exception as e:
        logger.error(f"Ошибка обновления активности: {e}")
    finally:
        conn.close()

def add_click(telegram_id: int, platform: str, course_id: int = None):
    """Добавление клика в БД"""
    if platform not in PARTNER_LINKS:
        logger.warning(f"Неизвестная платформа: {platform}")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Получаем ID пользователя
        cursor.execute('SELECT id FROM users WHERE telegram_id = ?', (telegram_id,))
        user = cursor.fetchone()
        
        if not user:
            logger.warning(f"Пользователь {telegram_id} не найден в БД")
            return
        
        user_id = user['id']
        
        # Добавляем клик
        cursor.execute('''
            INSERT INTO clicks (user_id, platform, course_id)
            VALUES (?, ?, ?)
        ''', (user_id, platform, course_id))
        
        # Обновляем счетчик кликов
        cursor.execute('''
            UPDATE users 
            SET clicks_count = clicks_count + 1,
                last_active = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (user_id,))
        
        conn.commit()
        logger.info(f"Клик добавлен: user={telegram_id}, platform={platform}, course={course_id}")
    except Exception as e:
        logger.error(f"Ошибка добавления клика: {e}")
    finally:
        conn.close()

def get_user_stats(telegram_id: int) -> dict:
    """Получение статистики пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Основная информация
        cursor.execute('''
            SELECT u.*, 
                   COUNT(DISTINCT c.platform) as platforms_count,
                   COUNT(c.id) as total_clicks
            FROM users u
            LEFT JOIN clicks c ON u.id = c.user_id
            WHERE u.telegram_id = ?
            GROUP BY u.id
        ''', (telegram_id,))
        
        user_data = cursor.fetchone()
        
        if not user_data:
            return None
        
        # Клики по платформам
        cursor.execute('''
            SELECT platform, COUNT(*) as clicks
            FROM clicks c
            JOIN users u ON c.user_id = u.id
            WHERE u.telegram_id = ?
            GROUP BY platform
            ORDER BY clicks DESC
        ''', (telegram_id,))
        
        platforms_clicks = cursor.fetchall()
        
        # Последние клики
        cursor.execute('''
            SELECT c.platform, c.clicked_at
            FROM clicks c
            JOIN users u ON c.user_id = u.id
            WHERE u.telegram_id = ?
            ORDER BY c.clicked_at DESC
            LIMIT 5
        ''', (telegram_id,))
        
        recent_clicks = cursor.fetchall()
        
        return {
            'user': dict(user_data),
            'platforms_clicks': [dict(row) for row in platforms_clicks],
            'recent_clicks': [dict(row) for row in recent_clicks]
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return None
    finally:
        conn.close()

# Данные о курсах (оптимизированные)
COURSES_DATA = {
    'programming': [
        {
            'id': 1,
            'title': 'Python-разработчик с нуля',
            'platform': 'skillfactory',
            'description': 'Освойте Python, Django, PostgreSQL и Docker. Станьте junior-разработчиком за 12 месяцев.',
            'duration': '12 месяцев',
            'price': 'от 5,900 ₽/мес',
            'skills': ['Python', 'Django', 'PostgreSQL', 'Docker', 'REST API'],
            'rating': '4.8/5',
            'comment': 'Самый популярный курс по Python. Отличная поддержка и реальные проекты.'
        },
        {
            'id': 2,
            'title': 'Fullstack-разработчик на JavaScript',
            'platform': 'skillbox',
            'description': 'Научитесь создавать веб-приложения с нуля. React, Node.js, MongoDB и облачные технологии.',
            'duration': '18 месяцев',
            'price': 'от 6,500 ₽/мес',
            'skills': ['JavaScript', 'React', 'Node.js', 'MongoDB', 'Docker'],
            'rating': '4.7/5',
            'comment': 'Идеально для карьеры fullstack-разработчика. Современный стек технологий.'
        },
        {
            'id': 3,
            'title': 'Java-разработчик PRO',
            'platform': 'geekbrains',
            'description': 'Профессия Java-разработчик с гарантией трудоустройства. Spring, Hibernate, микросервисы.',
            'duration': '14 месяцев',
            'price': 'от 7,000 ₽/мес',
            'skills': ['Java', 'Spring Boot', 'Hibernate', 'Kafka', 'Docker'],
            'rating': '4.6/5',
            'comment': 'Лучший выбор для enterprise-разработки. Сильное комьюнити.'
        },
        {
            'id': 4,
            'title': 'Разработчик на C# и .NET',
            'platform': 'skillbox',
            'description': 'Освойте разработку на C# для Windows, веба и игр. Unity, ASP.NET Core, Entity Framework.',
            'duration': '10 месяцев',
            'price': 'от 5,500 ₽/мес',
            'skills': ['C#', '.NET Core', 'ASP.NET', 'SQL Server', 'Unity'],
            'rating': '4.5/5',
            'comment': 'Отличный курс для разработки под экосистему Microsoft.'
        }
    ],
    'design': [
        {
            'id': 5,
            'title': 'UX/UI-дизайнер с нуля до PRO',
            'platform': 'skillbox',
            'description': 'Научитесь создавать современные интерфейсы для сайтов и приложений. Figma, Adobe XD, Tilda.',
            'duration': '12 месяцев',
            'price': 'от 5,000 ₽/мес',
            'skills': ['Figma', 'UI/UX', 'Прототипирование', 'User Research', 'Design Systems'],
            'rating': '4.9/5',
            'comment': 'Лучший курс по дизайну интерфейсов. Много реальных кейсов.'
        },
        {
            'id': 6,
            'title': 'Графический дизайн и брендинг',
            'platform': 'skillfactory',
            'description': 'Освойте Adobe Photoshop, Illustrator и создавайте профессиональный дизайн для брендов.',
            'duration': '8 месяцев',
            'price': 'от 4,500 ₽/мес',
            'skills': ['Photoshop', 'Illustrator', 'Брендинг', 'Верстка', 'Typography'],
            'rating': '4.7/5',
            'comment': 'Практический курс с фокусом на коммерческий дизайн.'
        }
    ],
    'marketing': [
        {
            'id': 7,
            'title': 'Digital-маркетолог от А до Я',
            'platform': 'geekbrains',
            'description': 'Полный курс по интернет-маркетингу: SMM, SEO, контекстная реклама, аналитика и стратегия.',
            'duration': '10 месяцев',
            'price': 'от 5,800 ₽/мес',
            'skills': ['SMM', 'SEO', 'Google Ads', 'Analytics', 'Content Marketing'],
            'rating': '4.8/5',
            'comment': 'Комплексный подход к digital-маркетингу. Актуальные инструменты 2024.'
        },
        {
            'id': 8,
            'title': 'SMM-специалист PRO',
            'platform': 'skillbox',
            'description': 'Научитесь продвигать бренды в соцсетях. Instagram, VK, YouTube, Telegram, TikTok.',
            'duration': '7 месяцев',
            'price': 'от 4,800 ₽/мес',
            'skills': ['Instagram', 'TikTok', 'YouTube', 'Таргетинг', 'Content Plan'],
            'rating': '4.6/5',
            'comment': 'Практический курс с упором на монетизацию.'
        }
    ],
    'analytics': [
        {
            'id': 9,
            'title': 'Data Science и Machine Learning',
            'platform': 'skillfactory',
            'description': 'Станьте data scientist. Python для анализа данных, машинное обучение, нейросети и SQL.',
            'duration': '16 месяцев',
            'price': 'от 7,200 ₽/мес',
            'skills': ['Python', 'Pandas', 'ML', 'SQL', 'Tableau', 'Deep Learning'],
            'rating': '4.9/5',
            'comment': 'Самый глубокий курс по Data Science на русском языке.'
        },
        {
            'id': 10,
            'title': 'Аналитик данных с нуля',
            'platform': 'geekbrains',
            'description': 'Освойте SQL, Excel, Python и BI-системы. Научитесь принимать решения на основе данных.',
            'duration': '9 месяцев',
            'price': 'от 5,500 ₽/мес',
            'skills': ['SQL', 'Excel', 'Python', 'Tableau', 'Statistics', 'Power BI'],
            'rating': '4.7/5',
            'comment': 'Отличный старт в аналитике. Много практических заданий.'
        }
    ]
}

@dp.message(Command("start"))
async def start_command(message: Message, command: CommandObject = None):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or "Пользователь"
    last_name = message.from_user.last_name or ""
    
    # Обработка реферальной ссылки
    referrer_id = None
    if command and command.args:
        if command.args.startswith('ref'):
            try:
                referrer_id = int(command.args[3:])
                logger.info(f"Реферальный переход: {user_id} от {referrer_id}")
            except ValueError:
                logger.warning(f"Некорректный реферальный код: {command.args}")
    
    # Добавляем/обновляем пользователя
    ref_code = add_user(user_id, username, first_name, last_name, referrer_id)
    update_user_activity(user_id)
    
    # Приветственное сообщение
    welcome_text = f"""
🎓 <b>Привет, {first_name}!</b>

Я — бот-куратор курсов по IT и digital.
Помогу выбрать лучшие курсы с проверенными отзывами.

<b>Доступные платформы:</b>
• Skillbox — {COMMISSIONS['skillbox']} комиссия
• SkillFactory — {COMMISSIONS['skillfactory']} комиссия  
• GeekBrains — {COMMISSIONS['geekbrains']} комиссия

<blockquote>💡 <i>Для вас цена не меняется! 
Комиссия идет на развитие бота.</i></blockquote>

👇 <b>Выберите категорию:</b>
    """
    
    # Основная клавиатура
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="💻 Программирование"),
                KeyboardButton(text="🎨 Дизайн")
            ],
            [
                KeyboardButton(text="📈 Маркетинг"),
                KeyboardButton(text="📊 Аналитика")
            ],
            [
                KeyboardButton(text="🔍 Подобрать курс"),
                KeyboardButton(text="📊 Моя статистика")
            ],
            [
                KeyboardButton(text="ℹ️ О боте"),
                KeyboardButton(text="🤝 Партнерка")
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )
    
    await message.answer(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

@dp.message(Command("help"))
async def help_command(message: Message):
    """Команда помощи"""
    help_text = """
<b>📚 Доступные команды:</b>

/start — Главное меню
/help — Эта справка
/courses — Все категории курсов
/stats — Ваша статистика (только для вас)

<b>🏷️ Категории курсов:</b>
💻 <b>Программирование</b> — Python, JavaScript, Java, C#
🎨 <b>Дизайн</b> — UX/UI, Графический дизайн
📈 <b>Маркетинг</b> — Digital, SMM, SEO
📊 <b>Аналитика</b> — Data Science, Анализ данных

<b>🔄 Навигация:</b>
• Используйте кнопки меню
• Нажмите на курс для подробной информации
• Переходите по ссылкам для оформления

<b>💼 Партнерская программа:</b>
Приглашайте друзей и получайте 10% от нашей комиссии!

<i>Есть вопросы? Напишите нам!</i>
    """
    
    await message.answer(help_text, parse_mode=ParseMode.HTML)

@dp.message(F.text == "💻 Программирование")
async def programming_category(message: Message):
    """Курсы по программированию"""
    await show_category(message, 'programming', "💻 <b>Курсы по программированию</b>")

@dp.message(F.text == "🎨 Дизайн")
async def design_category(message: Message):
    """Курсы по дизайну"""
    await show_category(message, 'design', "🎨 <b>Курсы по дизайну</b>")

@dp.message(F.text == "📈 Маркетинг")
async def marketing_category(message: Message):
    """Курсы по маркетингу"""
    await show_category(message, 'marketing', "📈 <b>Курсы по маркетингу</b>")

@dp.message(F.text == "📊 Аналитика")
async def analytics_category(message: Message):
    """Курсы по аналитике"""
    await show_category(message, 'analytics', "📊 <b>Курсы по аналитике</b>")

async def show_category(message: Message, category: str, title: str):
    """Показать курсы в категории"""
    update_user_activity(message.from_user.id)
    
    courses = COURSES_DATA.get(category, [])
    
    if not courses:
        await message.answer("😔 Курсы в этой категории временно недоступны.")
        return
    
    # Формируем список курсов
    text = f"{title}\n\n"
    keyboard_buttons = []
    
    for course in courses:
        platform_name = PLATFORM_NAMES.get(course['platform'], course['platform'])
        button_text = f"{course['title']} ({platform_name})"
        
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=button_text,
                callback_data=f"course_{course['id']}"
            )
        ])
    
    # Добавляем кнопку назад
    keyboard_buttons.append([
        InlineKeyboardButton(text="🔙 Назад в меню", callback_data="menu_back")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    text += "<i>Выберите курс для подробной информации:</i>"
    
    await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("course_"))
async def show_course_detail(callback: types.CallbackQuery):
    """Показать детальную информацию о курсе"""
    try:
        course_id = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка загрузки курса", show_alert=True)
        return
    
    # Ищем курс
    course = None
    for category in COURSES_DATA.values():
        for c in category:
            if c['id'] == course_id:
                course = c
                break
        if course:
            break
    
    if not course:
        await callback.answer("❌ Курс не найден", show_alert=True)
        return
    
    update_user_activity(callback.from_user.id)
    
    # Регистрируем клик
    add_click(callback.from_user.id, course['platform'], course_id)
    
    # Получаем данные
    platform = course['platform']
    platform_name = PLATFORM_NAMES.get(platform, platform)
    commission = COMMISSIONS.get(platform, "15-30%")
    partner_link = PARTNER_LINKS.get(platform, "#")
    
    # Формируем сообщение
    text = f"""
🎓 <b>{course['title']}</b>
🏢 <i>Платформа: {platform_name}</i>
⭐ <b>Рейтинг: {course['rating']}</b>

📝 <b>Описание:</b>
{course['description']}

⏱ <b>Длительность:</b> {course['duration']}
💰 <b>Стоимость:</b> {course['price']}

🛠 <b>Освоите навыки:</b>
{chr(10).join([f'• {skill}' for skill in course['skills']])}

💬 <b>Наш отзыв:</b>
<blockquote>{course['comment']}</blockquote>

💼 <b>Партнерская комиссия:</b> {commission}
    """
    
    # Клавиатура действий
    keyboard_buttons = [
        [
            InlineKeyboardButton(
                text="🌐 Перейти на сайт курса",
                url=partner_link
            )
        ],
        [
            InlineKeyboardButton(
                text="📋 Похожие курсы",
                callback_data=f"similar_{platform}"
            )
        ],
        [
            InlineKeyboardButton(text="🔙 Назад к категориям", callback_data="category_back"),
            InlineKeyboardButton(text="🏠 В меню", callback_data="menu_back")
        ]
    ]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    except:
        await callback.message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    
    await callback.answer()

@dp.message(F.text == "🔍 Подобрать курс")
async def course_finder(message: Message):
    """Подбор курса по параметрам"""
    update_user_activity(message.from_user.id)
    
    text = """
🎯 <b>Подбор идеального курса</b>

Ответьте на 3 вопроса, и я подберу курсы именно для вас:

<b>1. Какое направление вас интересует?</b>
    """
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💻 Программирование", callback_data="finder_prog"),
            InlineKeyboardButton(text="🎨 Дизайн", callback_data="finder_design")
        ],
        [
            InlineKeyboardButton(text="📈 Маркетинг", callback_data="finder_marketing"),
            InlineKeyboardButton(text="📊 Аналитика", callback_data="finder_analytics")
        ],
        [
            InlineKeyboardButton(text="❓ Не знаю, помогите", callback_data="finder_help"),
            InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")
        ]
    ])
    
    await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

@dp.message(F.text == "📊 Моя статистика")
async def my_stats(message: Message):
    """Статистика пользователя"""
    user_id = message.from_user.id
    update_user_activity(user_id)
    
    stats = get_user_stats(user_id)
    
    if not stats or not stats['user']:
        text = "📊 <b>Ваша статистика</b>\n\nВы еще не совершали активных действий."
    else:
        user_data = stats['user']
        platforms = stats['platforms_clicks']
        
        text = f"""
📊 <b>Ваша статистика</b>

👤 <b>Пользователь:</b> {user_data['first_name']}
📅 <b>Зарегистрирован:</b> {user_data['registered_at'][:10]}
🔗 <b>Ваш реф-код:</b> <code>{user_data['ref_code']}</code>

📈 <b>Активность:</b>
• Всего переходов: <b>{user_data['clicks_count']}</b>
• Активных платформ: <b>{len(platforms)}</b>

<b>Переходы по платформам:</b>
"""
        
        for platform in platforms:
            platform_name = PLATFORM_NAMES.get(platform['platform'], platform['platform'])
            text += f"• {platform_name}: {platform['clicks']} переходов\n"
        
        if user_data['referrer_id']:
            text += f"\n🤝 <b>Вас пригласил:</b> пользователь #{user_data['referrer_id']}"
        
        text += f"""

💼 <b>Ваш заработок:</b>
Приглашено друзей: <b>0</b>
Доступно к выводу: <b>0 ₽</b>

<i>Приглашайте друзей по реферальной ссылке!</i>
"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Моя реф-ссылка", callback_data="my_ref_link"),
            InlineKeyboardButton(text="💳 Вывод средств", callback_data="withdraw")
        ],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_stats"),
            InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")
        ]
    ])
    
    await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

@dp.message(F.text == "ℹ️ О боте")
async def about_bot(message: Message):
    """Информация о боте"""
    update_user_activity(message.from_user.id)
    
    text = f"""
🤖 <b>О боте-кураторе</b>

<b>Наша миссия:</b>
Помогать находить качественные IT-курсы и начинать карьеру в digital.

<b>Как мы работаем:</b>
1. Тщательно отбираем курсы
2. Даем честные отзывы
3. Используем партнерские ссылки
4. Развиваем бота на комиссию

<b>Партнерские платформы:</b>
• Skillbox — курсы с практикой
• SkillFactory — обучение с менторами  
• GeekBrains — гарантия трудоустройства

<b>Партнерские комиссии:</b>
{chr(10).join([f'• {PLATFORM_NAMES[k]}: {v}' for k, v in COMMISSIONS.items()])}

<blockquote>💡 <i>Для вас цена не меняется!
Мы получаем комиссию только при успешной покупке.</i></blockquote>

<b>Контакты:</b>
По вопросам сотрудничества: @username

<i>Бот работает на энтузиазме и партнерских комиссиях ❤️</i>
    """
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📢 Наш канал", url="https://t.me/your_channel"),
            InlineKeyboardButton(text="💬 Чат поддержки", url="https://t.me/your_support")
        ],
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")
        ]
    ])
    
    await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

@dp.message(F.text == "🤝 Партнерка")
async def partner_program(message: Message):
    """Партнерская программа"""
    update_user_activity(message.from_user.id)
    
    stats = get_user_stats(message.from_user.id)
    ref_code = stats['user']['ref_code'] if stats and stats['user'] else "Ошибка"
    
    text = f"""
🤝 <b>Партнерская программа</b>

Приглашайте друзей и получайте <b>10% от нашей комиссии</b> с их покупок!

<b>Ваша реферальная ссылка:</b>
<code>https://t.me/{(await bot.get_me()).username}?start=ref{message.from_user.id}</code>

<b>Или код для ручного ввода:</b>
<code>{ref_code}</code>

<b>Как это работает:</b>
1. Друг переходит по вашей ссылке
2. Регистрируется через бота
3. Совершает покупку любого курса
4. Вы получаете 10% от нашей комиссии

<b>Пример расчета:</b>
Курс стоимостью 50,000 ₽
Наша комиссия: 30% = 15,000 ₽
Ваш заработок: 10% = 1,500 ₽

<b>Условия выплат:</b>
• Минимальная сумма вывода: 500 ₽
• Вывод на карту РФ или криптовалюту
• Статистика обновляется ежедневно
• Выплаты раз в месяц

<i>Начните приглашать друзей уже сегодня!</i>
    """
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Скопировать ссылку", callback_data="copy_ref_link"),
            InlineKeyboardButton(text="📊 Мои рефералы", callback_data="my_refs")
        ],
        [
            InlineKeyboardButton(text="💰 Баланс", callback_data="balance"),
            InlineKeyboardButton(text="💳 Вывод", callback_data="withdraw")
        ],
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")
        ]
    ])
    
    await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

@dp.callback_query(F.data == "menu_back")
async def back_to_menu(callback: types.CallbackQuery):
    """Возврат в главное меню"""
    await start_command(callback.message)
    await callback.answer()

@dp.callback_query(F.data == "category_back")
async def back_to_categories(callback: types.CallbackQuery):
    """Возврат к категориям"""
    text = "👇 <b>Выберите категорию курсов:</b>"
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="💻 Программирование"),
                KeyboardButton(text="🎨 Дизайн")
            ],
            [
                KeyboardButton(text="📈 Маркетинг"),
                KeyboardButton(text="📊 Аналитика")
            ]
        ],
        resize_keyboard=True
    )
    
    await callback.message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    await callback.answer()

@dp.callback_query(F.data.startswith("similar_"))
async def show_similar_courses(callback: types.CallbackQuery):
    """Показать похожие курсы"""
    platform = callback.data.split("_")[1]
    
    if platform not in PARTNER_LINKS:
        await callback.answer("❌ Платформа не найдена", show_alert=True)
        return
    
    # Ищем курсы этой платформы
    similar_courses = []
    for category in COURSES_DATA.values():
        for course in category:
            if course['platform'] == platform:
                similar_courses.append(course)
    
    if not similar_courses:
        await callback.answer("😔 Похожие курсы не найдены", show_alert=True)
        return
    
    platform_name = PLATFORM_NAMES.get(platform, platform)
    text = f"<b>Другие курсы на {platform_name}:</b>\n\n"
    
    keyboard_buttons = []
    for course in similar_courses[:5]:  # Ограничиваем 5 курсами
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=course['title'],
                callback_data=f"course_{course['id']}"
            )
        ])
    
    keyboard_buttons.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    except:
        await callback.message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    
    await callback.answer()

@dp.callback_query(F.data == "my_ref_link")
async def show_ref_link(callback: types.CallbackQuery):
    """Показать реферальную ссылку"""
    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref{callback.from_user.id}"
    
    text = f"""
<b>Ваша реферальная ссылка:</b>

<code>{ref_link}</code>

👇 Нажмите, чтобы скопировать:
    """
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📋 Скопировать ссылку",
                url=f"https://t.me/share/url?url={ref_link}&text=Привет! Нашел классного бота с курсами по IT!"
            )
        ],
        [
            InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")
        ]
    ])
    
    try:
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    except:
        await callback.message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    
    await callback.answer("Ссылка готова!")

@dp.callback_query(F.data == "refresh_stats")
async def refresh_stats(callback: types.CallbackQuery):
    """Обновить статистику"""
    await my_stats(callback.message)
    await callback.answer("✅ Статистика обновлена")

# Команда /stats для пользователя (альтернатива кнопке)
@dp.message(Command("stats"))
async def stats_command(message: Message):
    """Команда статистики"""
    await my_stats(message)

# Команда /admin для администратора
@dp.message(Command("admin"))
async def admin_panel(message: Message):
    """Админ-панель"""
    ADMIN_IDS = [int(os.getenv("ADMIN_ID", "0"))]  # Ваш ID из .env
    
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Доступ запрещен")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Общая статистика
    cursor.execute('SELECT COUNT(*) as total FROM users')
    total_users = cursor.fetchone()['total']
    
    cursor.execute('SELECT COUNT(*) as total FROM clicks')
    total_clicks = cursor.fetchone()['total']
    
    cursor.execute('''
        SELECT COUNT(DISTINCT telegram_id) as active 
        FROM users 
        WHERE last_active > datetime('now', '-7 days')
    ''')
    active_users = cursor.fetchone()['active']
    
    cursor.execute('''
        SELECT platform, COUNT(*) as clicks
        FROM clicks
        GROUP BY platform
        ORDER BY clicks DESC
    ''')
    platform_stats = cursor.fetchall()
    
    cursor.execute('''
        SELECT DATE(clicked_at) as date, COUNT(*) as clicks
        FROM clicks
        WHERE clicked_at > datetime('now', '-7 days')
        GROUP BY DATE(clicked_at)
        ORDER BY date DESC
    ''')
    daily_stats = cursor.fetchall()
    
    conn.close()
    
    text = f"""
<b>📊 АДМИН-ПАНЕЛЬ</b>

👥 <b>Пользователи:</b> {total_users}
📈 <b>Активные (7 дней):</b> {active_users}
🖱️ <b>Всего кликов:</b> {total_clicks}

<b>Клики по платформам:</b>
"""
    
    for stat in platform_stats:
        platform_name = PLATFORM_NAMES.get(stat['platform'], stat['platform'])
        text += f"• {platform_name}: {stat['clicks']}\n"
    
    text += f"\n<b>Клики за 7 дней:</b>\n"
    for stat in daily_stats:
        text += f"• {stat['date']}: {stat['clicks']}\n"
    
    text += f"\n<i>Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}</i>"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_refresh"),
            InlineKeyboardButton(text="📥 Экспорт", callback_data="admin_export")
        ],
        [
            InlineKeyboardButton(text="✉️ Рассылка", callback_data="admin_broadcast"),
            InlineKeyboardButton(text="🚪 Выход", callback_data="menu_back")
        ]
    ])
    
    await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

# Обработка неизвестных сообщений
@dp.message()
async def handle_unknown(message: Message):
    """Обработка неизвестных сообщений"""
    update_user_activity(message.from_user.id)
    
    response = """
🤔 Я не понимаю это сообщение.

Используйте кнопки меню или команды:
/start — Главное меню
/help — Помощь по боту
/stats — Ваша статистика
    """
    
    await message.answer(response)

async def main():
    """Главная функция"""
    # Инициализация БД
    init_db()
    
    logger.info("=" * 50)
    logger.info("БОТ ЗАПУЩЕН")
    logger.info(f"Платформы: {', '.join(PARTNER_LINKS.keys())}")
    logger.info(f"Количество курсов: {sum(len(c) for c in COURSES_DATA.values())}")
    logger.info("=" * 50)
    
    # Запуск бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())