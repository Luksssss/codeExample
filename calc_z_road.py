#!/usr/bin/env python3
"""
Запуск расчёта координаты Z для оси через API (для acad)
+ изменение километража если дорога начинается не с 0
+ добавление/изменение записи в словаре dict_roads
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


def read_flag_conf():
    """Считать параметры командной строки"""

    parser = optparse.OptionParser(
        description='Расчёт координаты Z дороги'
    )

    # Общие параметры
    parser.add_option(
        '--logfile',
        action='store', type=str, dest='logfile',
        help='Лог-файл'
    )
    # parser.add_option(
    #     '--quiet',
    #     action='store_true', dest='quiet',
    #     default=False,
    #     help='Не выводить логи на консоль, не спрашивать вопросы'
    # )
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
        '--km-beg',
        action='store', type=str, dest='km_beg',
        help='Км начала дороги'
    )

    options, args = parser.parse_args()

    return options, args

def make_conf(options):
    # инициализация
    conf = {}
    conf.setdefault('db_name', '')
    conf.setdefault('db_server', '')
    conf.setdefault('db_user', 'postgres')
    conf.setdefault('db_pass', 'postgres')
    conf.setdefault('project', '')
    conf.setdefault('logfile', '')
    conf.setdefault('road_codes', '')
    # conf.setdefault('road_codes', [])
    conf.setdefault('km_beg', '')
    conf.setdefault('quiet', True)

    if options.project:
        conf['db_name'] = 'dorgis_' + options.project
    if options.server:
        conf['db_server'] = options.server
    if options.logfile:
        conf['logfile'] = options.logfile
    if options.km_beg:
        try:
            conf['km_beg'] = float(options.km_beg)
        except ValueError:
            raise BadConf('Км начала дороги должен быть вещественным числом.')
    if options.road_codes:
        try:
            conf['road_codes'] = int(options.road_codes)
        except ValueError:
            raise BadConf('Код дороги должен быть целым числом')

    return conf


def process_z(logger, cursor, conf, road_code):
    """
    Запустить все указанные в conf обработчики по дороге и
    вернуть ошибку, если что-то пошло не так
    """

    logger.info('Дорога: %d' % road_code)

    road_len = get_amount(cursor, [road_code])
    logger.info('Длина дороги: %d м' % road_len)
    logger.info('Расчёт Z-значения оси начался.')
    logger.info('Расчёт 3D оси.')
    
    # Считаем 3D геометрию пишем в geomz
    sql_upd_z = '''
        update tbl_roads set geomz = get_linez_from_line(geom, road_code) where road_code = %(road_code)s
    '''% {'road_code': road_code}
    cursor.execute(sql_upd_z)
    
    # передаём в geom M-координату
    logger.info('Преобразование М-координаты.')
    sql_upd_m = '''
        update tbl_roads set geom = ST_Force3DM(geomz) where road_code = %(road_code)s
    '''% {'road_code': road_code}
    cursor.execute(sql_upd_m)

    # обновляем длину дороги
    logger.info('Обновление длины дороги.')
    sql = '''
        select update_road_measure(%(road_code)s)
    '''% {'road_code': road_code}
    cursor.execute(sql)

    # пересчёт длины оси если дорога начинается не с 0
    if conf['km_beg'] > 0:
        logger.info('Изменение начала и конца дороги.')
        sql_upd_fmp = '''
            update tbl_roads set fmp = (fmp + %(fmp)s), tmp = (fmp+ %(fmp)s + length_km) where road_code = %(road_code)s
        '''% {'road_code': road_code, 'fmp': conf['km_beg']}
        cursor.execute(sql_upd_fmp)
        
        logger.info('Изменение М-координаты дороги.')
        sql_upd_m2 = '''
            update tbl_roads SET geom = ST_AddMeasure_Meters(geom, fmp, tmp) where road_code = %(road_code)s
        '''% {'road_code': road_code}
        cursor.execute(sql_upd_m2)

        logger.info('Перепривязка панорам.')
        sql_upd_pano = '''
            UPDATE tbl_panoram_road ta
            SET km_beg = ST_InterpolatePoint_Meters(
                (
                    SELECT geom
                    FROM tbl_roads tb
                    WHERE road_code = ta.road_code
                    ORDER BY ST_Distance(tb.geom,ta.geom)
                    LIMIT 1
                ),
                ta.geom
            )
            WHERE road_code = %(road_code)s;
        '''% {'road_code': road_code}
        cursor.execute(sql_upd_pano)

    # также добавляем/изменяем запись в словаре dict_roads
    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM dorgis.dict_roads
            WHERE road_code = %(road_code)s
        )
    ''' % {'road_code': road_code}
    cursor.execute(sql)
    res = cursor.fetchone()['exists']
    
    if res:
        sql_dict = '''
            UPDATE dorgis.dict_roads SET lenght = (
                SELECT sum(length_km) as length_km
                FROM tbl_roads
                WHERE road_code = %(road_code)s
                group by road_code
            )
            WHERE road_code=%(road_code)s
        ''' % {'road_code': road_code}
    
        logger.info('Изменена запись в dict_roads.')

    else:
        sql_dict = '''
            INSERT INTO dorgis.dict_roads(road_code, name, lenght)
                SELECT road_code, name, sum(length_km) as lenght
                FROM tbl_roads
                where road_code = %(road_code)s
                group by road_code, name
        ''' % {'road_code': road_code}

        logger.info('Добавлена запись в dict_roads.')

    cursor.execute(sql_dict)

    logger.info('Расчёт Z-значения оси завершён.')

# Парсинг аргументов командной строки
options, args = read_flag_conf()

conf = make_conf(options)

road_code = int(conf['road_codes'])   

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
    amount = get_amount(cursor, [road_code])
except ProcessRoadError as e:
    logger.error(fmt_ex(e))
    total_exit(logger, timer, 'Не удалось получить длину дорог: '+fmt_ex(e))
logger.info('AMOUNT: %d' % amount)

errors_count = 0
 
# Проверка существования поверхностей по потокам данной дороги
sql = '''
    SELECT EXISTS (
        SELECT 1
        FROM tbl_las_tin
        WHERE fname IN (
            SELECT fname FROM tbl_fname_road_code WHERE road_code = %d
        )
        LIMIT 1
    )
''' % road_code
cursor.execute(sql)
res = cursor.fetchone()['exists']
if not res:
    errors_count += 1
    errors = 'Сначала загрузите точки и поверхности по дороге %s' % road_code
else:
    # Запуск обработчиков
    try:
        process_z(logger, cursor, conf, road_code)
    except (ProcessRoadError, ProcessError, InvalidInputError) as e:
        logger.error(fmt_ex(e))
        errors_count += 1
        errors = 'Завершено с ошибками (%d)' % errors_count

if errors_count == 0:
    errors = None    

total_exit(logger, timer, errors)