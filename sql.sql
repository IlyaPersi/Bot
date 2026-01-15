-- Пользователи
users: id, telegram_id, username, first_name, last_name, registered_at, clicks_count, ref_code, referrer_id, last_active

-- Клики
clicks: id, user_id, platform, course_id, clicked_at

-- Кэш курсов
courses_cache: id, title, platform, category, data, updated_at