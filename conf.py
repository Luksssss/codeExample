"""
Функции, отвечающие за настройку работы скрипта.
"""

import json
import logging
from logging.config import dictConfig
import optparse
import os
import psycopg2
import psycopg2.extras


DIR = os.path.dirname(os.path.abspath(__file__))
CONF_FILE = 'conf.json'

TASK_NAMES = {
    'import_objects': 'Импорт объектов',
    'roadways': 'Построение площадного слоя ПЧ (tbl_roadways)',
    'width': 'Расчёт ширины ПЧ и обочин',
    'curves_in_plane': 'Расчёт кривых (tbl_curvesinplane)',
    'transverse_slopes': 'Расчёт поперечных уклонов (tbl_transverseslopes)',
    'latprofile': 'Расчёт продольного профиля (tbl_latprofile)',
    'rut': 'Расчёт колейности (tbl_rutdepth)',
    'iri': 'Расчёт ровности покрытия (tbl_coatingquality)',
    'acad': 'Загрузка данных через сервис ACAD (без использования схемы editor)',
    'def': 'Расчёт БКАД дефектов для диагностики'
}


class BadConf(Exception):
    pass


def db_connect(server, db_name, db_user, db_pass):
    try:
        connection = psycopg2.connect(
            database=db_name, host=server,
            user=db_user, password=db_pass
        )
        connection.autocommit = True
    except psycopg2.OperationalError as e:
        raise BadConf(str(e))
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor


def get_db_connect(server, db_name, db_user, db_pass):
    try:
        connection = psycopg2.connect(
            database=db_name, host=server,
            user=db_user, password=db_pass
        )
        connection.autocommit = True
    except psycopg2.OperationalError as e:
        raise BadConf(str(e))
    return connection


def get_cursor(connection):
    return connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def read_flag_conf():
    """Считать параметры командной строки"""

    parser = optparse.OptionParser(
        description='Импорт объектов из AutoCAD'
    )

    # Общие параметры
    parser.add_option(
        '--logfile',
        action='store', type=str, dest='logfile',
        help='Лог-файл'
    )
    parser.add_option(
        '--quiet',
        action='store_true', dest='quiet',
        default=False,
        help='Не выводить логи на консоль, не спрашивать вопросы'
    )
    parser.add_option(
        '--project',
        action='store', type=str, dest='project',
        help='Название проекта (без приставки "dorgis_")'
    )
    parser.add_option(
        '--road-codes',
        action='store', type=str, dest='road_codes',
        help='Коды дорог через запятую (без пробелов)'
    )
    parser.add_option(
        '--srid',
        action='store', type=str, dest='srid',
        help='SRID проекта'
    )
    parser.add_option(
        '--new_calc',
        action='store', type=str, dest='new_calc',
        help='Признак нового пересчета колейности'
    )
    # Параметры запуска модулей
    parser.add_option(
        '-a',
        action='store_true', dest='all',
        default=False,
        help='Импорт объектов и все расчёты'
    )
    parser.add_option(
        '-i',
        action='store_true', dest='import_objects',
        default=False,
        help=TASK_NAMES['import_objects']
    )
    parser.add_option(
        '-r',
        action='store_true', dest='roadways',
        default=False,
        help=TASK_NAMES['roadways']
    )
    parser.add_option(
        '-w',
        action='store_true', dest='width',
        default=False,
        help=TASK_NAMES['width']
    )
    parser.add_option(
        '-c',
        action='store_true', dest='curves_in_plane',
        default=False,
        help=TASK_NAMES['curves_in_plane']
    )
    parser.add_option(
        '-t',
        action='store_true', dest='transverse_slopes',
        default=False,
        help=TASK_NAMES['transverse_slopes']
    )
    parser.add_option(
        '-p',
        action='store_true', dest='latprofile',
        default=False,
        help=TASK_NAMES['latprofile']
    )
    parser.add_option(
        '-u',
        action='store_true', dest='rut',
        default=False,
        help=TASK_NAMES['rut']
    )
    parser.add_option(
        '-o',
        action='store_true', dest='iri',
        default=False,
        help=TASK_NAMES['iri']
    )
    parser.add_option(
        '-f',
        action='store_true', dest='def',
        default=False,
        help=TASK_NAMES['def']
    )    
    parser.add_option(
        '-d',
        action='store_true', dest='acad',
        default=False,
        help=TASK_NAMES['acad']
    )  

    options, args = parser.parse_args()

    return options, args


def make_logger(logfile=None, quiet=False):
    """Задать конфигурацию логгера"""

    logging_config = {
        'version': 1,
        'formatters': {
            'default': {
                'format': '%(asctime)s %(levelname)-8s %(message)s',
                'datefmt': '%Y/%m/%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default'
            },
            'file': {
                'filename': 'from_autocad.log',
                'class': 'logging.FileHandler',
                'formatter': 'default'
            }
        },
        'root': {
            'handlers': ['console', 'file'],
            'level': logging.DEBUG
        },
    }

    if logfile:
        logging_config['handlers']['file']['filename'] = logfile
    if quiet:
        logging_config['root']['handlers'] = ['file']

    # Очистить лог
    open(logging_config['handlers']['file']['filename'], 'w').close()

    dictConfig(logging_config)
    logger = logging.getLogger()

    return logger


def read_conf():
    """Считать всю конфигурацию в один словарь"""

    conf = {}

    # Считать конфигурацию из файла
    try:
        conf_file = open(os.path.join(DIR, CONF_FILE)).read()
    except FileNotFoundError:
        raise BadConf('Не найден файл конфигурации '+CONF_FILE)
    try:
        conf.update(json.loads(conf_file))
    except json.decoder.JSONDecodeError:
        raise BadConf('Ошибка парсинга файла конфигурации '+CONF_FILE)

    # Парсинг аргументов командной строки
    options, args = read_flag_conf()

    # Составить словарь из options
    attrs = ('logfile', 'quiet', 'all', 'import_objects', 'roadways',
             'width', 'curves_in_plane', 'transverse_slopes', 'latprofile',
             'rut', 'iri', 'acad', 'def')
    for attr in attrs:
        conf[attr] = getattr(options, attr)

    if options.project:
        conf['db_name'] = 'dorgis_' + options.project
     
    # нужен исключительно для сервиса rut
    if options.new_calc:
        conf['new_calc'] = options.new_calc

    # нужен исключительно для сервиса acad
    if options.acad:
        conf['srid'] = int(options.srid)
        conf['project'] = options.project

    if options.road_codes:
        conf['road_codes'] = options.road_codes.split(',')

    try:
        conf['road_codes'] = [int(rc) for rc in conf['road_codes']]
    except ValueError:
        raise BadConf('Коды дорог должны быть целыми числами')

    # Проверить ключи запуска модулей расчётов - должен быть хоть один
    if not any([conf['all'],
                conf['import_objects'],
                conf['roadways'],
                conf['width'],
                conf['curves_in_plane'],
                conf['transverse_slopes'],
                conf['latprofile'],
                conf['rut'],
                conf['iri'],
                conf['def']]):
        raise BadConf('Не указаны ключи параметров обработки')

    return conf


def show_conf(conf):
    """Вывести самые важные переменные конфигурации для проверки"""

    template = ('\nСервер: %s\n'
                'БД: %s\n'
                'SRID: вычисляется динамически')
    print(template % (conf['server'], conf['db_name']))
    print('ЗАДАЧИ:')
    # Задача выполняется, если указана явно или через параметр --all
    for name in ('import_objects', 'roadways', 'width', 'def',
                 'curves_in_plane', 'transverse_slopes', 'latprofile'):
        if conf['all'] or conf[name]:
            print(' * '+TASK_NAMES[name])
    # Расчёт колейности и ровности происходит только при явном указании
    for name in ('rut', 'iri'):
        if conf[name]:
            print(' * '+TASK_NAMES[name])
