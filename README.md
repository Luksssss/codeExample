# Схема модуля

Файл            | Описание
--------------- | --------
`conf.py`       | Общие настройки, обработка аргументов командной строки, соединение с БД, логи
`run.py`        | Точка входа
`helpers.py`    | Функции и классы общего назначения
`validators.py` | Проверки для БД, таблиц и входных данных
`common.py`     | Запуск скриптов из `calc/` согласно конфигу
`calc/`         | Модуль с расчётами (скрыт)

Группа скриптов по загрузке и расчётов данных в основной проект.