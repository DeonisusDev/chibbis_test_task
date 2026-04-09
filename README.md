# JSONPlaceholder ETL

Загружает данные (users, posts, comments) из [jsonplaceholder.typicode.com](https://jsonplaceholder.typicode.com) и сохраняет их в локальную базу SQLite.

Повторный запуск безопасен — дублирования не будет (используется `INSERT … ON CONFLICT DO UPDATE`).

---

## Требования

- Python 3.10+
- pip

---

## Быстрый старт

```bash
# 1. Клонировать репозиторий
git clone <repo-url>
cd etl_project

# 2. Создать виртуальное окружение
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 3. Установить зависимости
pip install -r requirements.txt

# 4. Запустить
python etl.py
```

После успешного запуска в директории проекта появится файл `data.db`.

---

## Структура базы данных

```
users
├── id          INTEGER  PK
├── name        TEXT
├── username    TEXT
├── email       TEXT
├── phone       TEXT
├── website     TEXT
├── company     TEXT     (JSON-строка)
└── address     TEXT     (JSON-строка)

posts
├── id          INTEGER  PK
├── user_id     INTEGER  FK → users.id
├── title       TEXT
└── body        TEXT

comments
├── id          INTEGER  PK
├── post_id     INTEGER  FK → posts.id
├── name        TEXT
├── email       TEXT
└── body        TEXT
```

---

## Пример вывода

```
2024-01-15 12:00:00 [INFO] Starting ETL pipeline.
2024-01-15 12:00:00 [INFO] Database schema ready.
2024-01-15 12:00:00 [INFO] Fetching https://jsonplaceholder.typicode.com/users ...
2024-01-15 12:00:01 [INFO]   -> 10 records received.
2024-01-15 12:00:01 [INFO]   -> 10 rows upserted into 'users'.
2024-01-15 12:00:01 [INFO] Fetching https://jsonplaceholder.typicode.com/posts ...
2024-01-15 12:00:01 [INFO]   -> 100 records received.
2024-01-15 12:00:01 [INFO]   -> 100 rows upserted into 'posts'.
2024-01-15 12:00:01 [INFO] Fetching https://jsonplaceholder.typicode.com/comments ...
2024-01-15 12:00:02 [INFO]   -> 500 records received.
2024-01-15 12:00:01 [INFO]   -> 500 rows upserted into 'comments'.
2024-01-15 12:00:02 [INFO] ETL pipeline finished. Database: /path/to/data.db
```

---

## Технические детали

| Аспект | Решение |
|---|---|
| Идемпотентность | `INSERT … ON CONFLICT(id) DO UPDATE` (upsert) |
| Порядок загрузки | `users → posts → comments` (соблюдение FK) |
| Внешние ключи | Включены через `PRAGMA foreign_keys = ON` |
| Зависимости | Только `requests`; `sqlite3` входит в стандартную библиотеку |
