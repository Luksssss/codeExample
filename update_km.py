#!/usr/bin/env python3
"""
Запуск перепривязки километража объектов дороги через API (для acad)
"""

import sys
import optparse

from conf import BadConf, read_conf, show_conf, db_connect, make_logger
from common import get_amount
from helpers import Timer, ask_confirmation, total_exit

# Исключения
from common import ProcessRoadError
from calc.helpers import ProcessError
from validators import InvalidInputError
from helpers import fmt_ex

# Исключающие таблицы
tbl_not_update = []

def read_flag_conf():
    """Считать параметры командной строки"""

    parser = optparse.OptionParser(
        description='Обновление привязки объектов'
    )

    # Общие параметры
    parser.add_option(
        '--logfile',
        action='store', type=str, dest='logfile',
        help='Лог-файл'
    )
    parser.add_option(
        '--project',
        action='store', type=str, dest='project',
        help='Название проекта (без приставки "dorgis_")'
    )
    parser.add_option(
        '--server',
        action='store', type=str, dest='server',
        help='Расположение сервера базы'
    )
    parser.add_option(
        '--road-codes',
        action='store', type=str, dest='road_codes',
        help='Код дороги'
    )
    parser.add_option(
        '--layers',
        action='store', type=str, dest='layers',
        help='Слои для обновления привязки'
    )

    options, args = parser.parse_args()

    return options, args

def make_conf(options):
    # инициализация
    conf = {}
    conf.setdefault('db_name', '')
    conf.setdefault('db_server', '')
    conf.setdefault('project', '')
    conf.setdefault('logfile', '')
    conf.setdefault('road_codes', [])
    conf.setdefault('layers', '')
    conf.setdefault('quiet', True)

    if options.project:
        conf['db_name'] = 'dorgis_' + options.project
    if options.server:
        conf['db_server'] = options.server
    if options.logfile:
        conf['logfile'] = options.logfile
    if options.layers:
        conf['layers'] = options.layers
    try:
        conf['road_codes'] = [int(rc) for rc in conf['road_codes']]
    except ValueError:
        raise BadConf('Коды дорог должны быть целыми числами')

    return conf

# Вернём список таблиц из struct_db по которым нужно проходиться
def get_table_list(cursor, layers):
    
    # получаем только таблицы struct_db (без словарей и непонятных таблицами dtp_)
    sql_str = """select db_name as table_name, name as title
                from dorgis.struct_db
                where schema_name = 'dorgis' and type = 5 
                and (always_show_all is null or not always_show_all) and db_name not LIKE 'dtp_%'"""
    
    # исключаем таблицы которые не нужно обновлять, т.к. в них есть ручные данные
    if tbl_not_update:
        mas_not_update = ["'" +el + "'" for el in tbl_not_update]
        table_not_layers = ','.join(mas_not_update)
        str_not_layers = " and db_name not in (%s)" % table_not_layers
        sql_str = sql_str + str_not_layers
    
    # если в параметре пришёл конкретный список таблиц, то обрабатываем его
    if layers:
        # преобразуем таблицы к нужному формату запросу
        s = layers.split(",")
        mas_layers = ["'" +el + "'" for el in s]
        table_layers = ','.join(mas_layers)
        str_layers = " and db_name in (%s)" % table_layers
        sql_str = sql_str + str_layers
    cursor.execute(sql_str)
    mas_layers = cursor.fetchall()

    return mas_layers


def update_km(logger, cursor, table_list, road_code):
    """
    Запустить все указанные в conf обработчики по дороге и
    вернуть ошибку, если что-то пошло не так
    """

    logger.info('Дорога: %d' % road_code)

    road_len = get_amount(cursor, [road_code])
    logger.info('Длина дороги: %d м' % road_len)
    logger.info('Начало обновлений км привязок.')


    for table in table_list:
        tbl = table['table_name']
        # получаем количество объектов и проверяем существование поля id, road_code
        sql_sel = """
            select EXISTS (SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema='dorgis' and table_name= '%(table)s' and column_name='id')
            and 
            EXISTS (SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema='dorgis' and table_name= '%(table)s' and column_name='road_code') as exist
            """% {'table': tbl, 'road_code': road_code}
        cursor.execute(sql_sel)
        exist = cursor.fetchone()['exist']

        if exist:
            sql_sel = """
                    select sum(1) 
                    from dorgis.%(table)s 
                    where road_code=%(road_code)s
                """% {'table': tbl, 'road_code': road_code}
            cursor.execute(sql_sel)
            count = cursor.fetchone()['sum']

            if count:
                # есть таблицы в которых км не обновится просто id=id
                if tbl in ('tbl_contactpoints', 'tbl_autostations', 'tbl_carwashstations', 'tbl_phones', 
                            'tbl_puliccaterings', 'tbl_publictoilets', 'tbl_petrolstations', 'tbl_hotels',
                            'tbl_maintenancestations'):
                    sql_upd = """update dorgis.%(table)s SET k_s040_1 = null, id=id
                        where road_code = %(road_code)s
                        """% {'table': tbl, 'road_code': road_code}
                elif tbl in ('tbl_stationaryweightcontrolposts', 'tbl_borders_attrs'):
                    sql_upd = """update dorgis.%(table)s SET position = null, id=id
                        where road_code = %(road_code)s
                        """% {'table': tbl, 'road_code': road_code}  
                #  4.1 Съезды (можно менять только положение лево и право, остальные руками)
                elif tbl in ('tbl_crossroads'):
                    sql_upd = """update dorgis.%(table)s SET k_s025_1 = null, id=id
                        where road_code = %(road_code)s and k_s025_1 in (1,2)
                        """% {'table': tbl, 'road_code': road_code}                     
                else:
                    sql_upd = """update dorgis.%(table)s SET id=id
                        where road_code = %(road_code)s
                        """% {'table': tbl, 'road_code': road_code}
                cursor.execute(sql_upd)

                logger.info('Обновлен километраж %d объектов таблицы %s(%s)' % (count, table['title'], tbl))

    logger.info('Километраж для объектов дорог %d обновлен.' % road_code)


# Парсинг аргументов командной строки
options, args = read_flag_conf()

conf = make_conf(options)

if options.road_codes:
    conf['road_codes'] = options.road_codes.split(',')

try:
    conf['road_codes'] = [int(rc) for rc in conf['road_codes']]
except ValueError:
    raise BadConf('Коды дорог должны быть целыми числами')

# Логи
logger = make_logger(conf['logfile'], conf['quiet'])

# Засечь время
timer = Timer()
timer.start()
     
try:
    cursor = db_connect(conf['db_server'], conf['db_name'],
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
table_list = get_table_list(cursor, conf['layers'])

if table_list:
    for road_code in conf['road_codes']:
        # Запуск обработчиков
        try:
            update_km(logger, cursor, table_list, road_code)
        except (ProcessRoadError, ProcessError, InvalidInputError) as e:
            logger.error(fmt_ex(e))
            errors_count += 1
            errors = 'Завершено с ошибками (%d)' % errors_count

else:    
    logger.info('Выбранные таблицы не найдены в схеме dorgis.')
    errors_count += 1
    errors = 'Завершено с ошибками (%d)' % errors_count

if errors_count == 0:
    errors = None    

total_exit(logger, timer, errors)