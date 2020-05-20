#!/usr/bin/env python3
"""
Точка входа модуля.
"""

import sys

from conf import BadConf, read_conf, show_conf, db_connect, make_logger
from common import process_road, get_amount
from common_json import acad_process_road, get_amount
from helpers import Timer, ask_confirmation, total_exit

# Исключения
from common import ProcessRoadError
from calc.helpers import ProcessError
from validators import InvalidInputError
from helpers import fmt_ex


# Считать конфигурацию
try:
    conf = read_conf()
except BadConf as e:
    print(str(e), file=sys.stderr)
    sys.exit(1)

# Логи
logger = make_logger(conf['logfile'], conf['quiet'])

# Засечь время
timer = Timer()
timer.start()

# Проверка настроек
if not conf['quiet']:
    show_conf(conf)
    proceed = ask_confirmation(
        '\nПроверьте настройки и нажмите \"y\" для продолжения:'
    )
    if not proceed:
        sys.exit(0)

# Соединение с БД
# если запускает сервис acad, то расположение сервера нужно тащить с базы db_acad
if conf['acad']:
    try:
        cursor = db_connect(conf['server_acad'], conf['db_name_acad'],
                            conf['db_user'], conf['db_pass'])
    except BadConf as e:
        total_exit(logger, timer, 'Не удалось подключиться к БД acad: '+fmt_ex(e))

    get_param_proj = """
        select ip as server from db_project where name = %(project)s limit 1;
    """
    cursor.execute(get_param_proj, {'project': conf['project']})
    res = cursor.fetchone()
    if res:
        conf['server'] = res['server']
    else:
        total_exit(logger, timer, 'Не заведены данные по проекту: '+conf['project'])            
    
try:
    cursor = db_connect(conf['server'], conf['db_name'],
                        conf['db_user'], conf['db_pass'])
except BadConf as e:
    total_exit(logger, timer, 'Не удалось подключиться к БД: '+fmt_ex(e))

# Оценка объёма работы (длина всех дорог)
try:
    amount = get_amount(cursor, conf['road_codes'])
except ProcessRoadError as e:
    logger.error(fmt_ex(e))
    total_exit(logger, timer, 'Не удалось получить длину дорог: '+fmt_ex(e))
logger.info('AMOUNT: %d' % amount)

errors_count = 0
for road_code in conf['road_codes']:
    # Запуск обработчиков
    # NOTE: 2 варианта-через сервис acad (API) и старый вариант через схему editor
    try:
        if conf['acad']:
            acad_process_road(logger, cursor, conf, road_code)
        else:
            process_road(logger, cursor, conf, road_code)
    except (ProcessRoadError, ProcessError, InvalidInputError) as e:
        logger.error(fmt_ex(e))
        errors_count += 1

if errors_count:
    errors = 'Завершено с ошибками (%d)' % errors_count
else:
    errors = None
total_exit(logger, timer, errors)
