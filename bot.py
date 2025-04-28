import os
import logging
import asyncio
import aiohttp
import json
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from dotenv import load_dotenv

# Импортируем наш класс для работы с базой данных
from database import RealEstateDatabase

# Загружаем переменные окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Инициализация базы данных
db = RealEstateDatabase('real_estate.db')

# Токен Telegram бота из переменных окружения
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

# URL для API ИИ (например, OpenAI или другой API)
AI_API_URL = os.getenv('AI_API_URL')
AI_API_KEY = os.getenv('AI_API_KEY')

# Состояния пользователя
MAIN_MENU, SEARCH, PROPERTY_DETAILS, FILTER, ADD_PROPERTY = range(5)

# Коллбэки для клавиатуры
CALLBACK_SEARCH = 'search'
CALLBACK_FILTER = 'filter'
CALLBACK_DISTRICTS = 'districts'
CALLBACK_TYPES = 'types'
CALLBACK_FEATURES = 'features'
CALLBACK_PROPERTY = 'property'
CALLBACK_BACK = 'back'
CALLBACK_NEXT_PAGE = 'next_page'
CALLBACK_PREV_PAGE = 'prev_page'

# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик команды старт, показывает приветственное сообщение и главное меню"""
    user = update.effective_user
    
    # Инициализация пользовательских данных в контексте
    if 'filters' not in context.user_data:
        context.user_data['filters'] = {}
    
    message = (
        f"👋 Привет, {user.first_name}!\n\n"
        "Я бот для поиска недвижимости с искусственным интеллектом. "
        "Я помогу вам найти идеальный вариант жилья.\n\n"
        "🔍 Вы можете написать мне свой запрос обычным языком, например:\n"
        "- \"Хочу купить 2-комнатную квартиру в центре\"\n"
        "- \"Ищу дом с бассейном и парковкой\"\n"
        "- \"Квартира до 100000 с балконом\"\n\n"
        "Или используйте меню для поиска:"
    )
    
    # Создаем клавиатуру
    keyboard = [
        [InlineKeyboardButton("🔍 Поиск по критериям", callback_data=CALLBACK_FILTER)],
        [InlineKeyboardButton("🏙️ Выбрать район", callback_data=CALLBACK_DISTRICTS)],
        [InlineKeyboardButton("🏠 Типы недвижимости", callback_data=CALLBACK_TYPES)],
        [InlineKeyboardButton("✨ Особенности", callback_data=CALLBACK_FEATURES)]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(message, reply_markup=reply_markup)
    
    # Получаем и отправляем статистику
    stats = db.get_statistics()
    stats_message = (
        "📊 *Статистика по недвижимости:*\n\n"
        f"• Всего объектов: {stats['total_properties']}\n"
        f"• Средняя цена: ${stats['average_price']:,.2f}\n"
        f"• Средняя площадь: {stats['average_area']} м²\n\n"
        "*Популярные особенности:*\n"
    )
    
    for feature, count in stats.get('popular_features', {}).items():
        stats_message += f"• {feature}: {count} объектов\n"
    
    await update.message.reply_text(stats_message, parse_mode='Markdown')
    
    return MAIN_MENU

# Функция для обработки обычных текстовых сообщений (ИИ-поиск)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает текстовые запросы пользователя с помощью ИИ"""
    query_text = update.message.text
    
    # Сообщаем пользователю, что запрос обрабатывается
    processing_message = await update.message.reply_text("🔍 Обрабатываю ваш запрос...")
    
    try:
        # Используем ИИ для анализа запроса
        filters = await ai_analyze_query(query_text)
        
        # Сохраняем фильтры в контексте пользователя
        context.user_data['filters'] = filters
        context.user_data['query_text'] = query_text
        
        # Ищем недвижимость с помощью полученных фильтров
        properties = db.search_properties(filters, limit=5)
        
        # Удаляем сообщение об обработке
        await processing_message.delete()
        
        if properties:
            # Отправляем результаты поиска
            await send_search_results(update, context, properties, f"🔍 Результаты по запросу: *{query_text}*")
        else:
            # Если ничего не найдено
            keyboard = [
                [InlineKeyboardButton("🔄 Изменить критерии поиска", callback_data=CALLBACK_FILTER)],
                [InlineKeyboardButton("🏠 Вернуться в главное меню", callback_data=CALLBACK_BACK)]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"😕 По запросу «{query_text}» ничего не найдено.\n\n"
                "Попробуйте изменить критерии поиска или уточнить запрос.",
                reply_markup=reply_markup
            )
        
        return SEARCH
        
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {e}")
        
        # Удаляем сообщение об обработке
        await processing_message.delete()
        
        await update.message.reply_text(
            "😢 Произошла ошибка при обработке вашего запроса.\n"
            "Пожалуйста, попробуйте еще раз или используйте меню для поиска."
        )
        
        return MAIN_MENU

# Функция для обработки кнопок
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатия на кнопки"""
    query = update.callback_query
    await query.answer()
    
    callback_data = query.data