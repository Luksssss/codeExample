"""
Чтение входного файла, импорт обычных объектов.
"""

from calc.curves_in_plane import calc_curves_in_plane
from calc.iri import calc_iri
from calc.latprofile import calc_latprofile
from calc.roadways import calc_roadways
#from calc.rut import calc_rut
from calc.rut2 import calc_rut
from calc.transverse_slopes import calc_transverse_slopes
from calc.width import calc_width
from calc.defects import calc_def
from calc.objects import calc_z, import_table
from validators import check_road, check_object, check_table, \
    check_input_data, check_tins


class ProcessRoadError(Exception):
    """Исключение для обработчика дорог"""
    pass


def get_amount(cursor, road_codes):
    """Получить общую длину дорог из списка road_codes"""

    sql = '''
        SELECT sum(length_km)*1000 AS len
        FROM tbl_roads
        WHERE road_code IN (%s)
    ''' % ','.join([str(rc) for rc in road_codes])
    cursor.execute(sql)
    row = cursor.fetchone()

    if not row or not row['len']:
        raise ProcessRoadError('Не найдены дороги из списка '
                               'или у них не заполнено поле length_km')

    return int(row['len'])


def get_utm(cursor):
    """Определить UTM-зону проекта"""

    sql = '''
        SELECT srid
        FROM (
            SELECT utmzone(geom) AS srid, count(1) AS c
            FROM tbl_roads
            GROUP BY utmzone(geom)
        ) AS tmp
        ORDER BY c DESC
        LIMIT 1
    '''
    cursor.execute(sql)
    utm = cursor.fetchone()['srid']

    return utm


def get_tables(cursor):
    """Получить имена всех таблиц для импорта"""

    sql = '''
        SELECT DISTINCT table_name
        FROM editor.tbl_acad_objects
    '''
    cursor.execute(sql)
    tables = cursor.fetchall()
    return [t['table_name'] for t in tables]


def process_road(logger, cursor, conf, road_code):
    """
    Запустить все указанные в conf обработчики по дороге и
    вернуть ошибку, если что-то пошло не так
    """

    logger.info('Дорога: %d' % road_code)

    # Определение UTM-зоны
    utm = get_utm(cursor)
    if not utm:
        raise ProcessRoadError('Не удалось определить зону UTM')

    if not check_road(cursor, road_code):
        raise ProcessRoadError('Отсутствует ось дороги')

    road_len = get_amount(cursor, [road_code])
    logger.info('Длина дороги: %d м' % road_len)

    # Проверка поверхностей
    if not check_tins(cursor, road_code):
        raise ProcessRoadError('Отсутствуют поверхности')

    # Расчёт Z-значений (не будет считаться, если посчитано ранее)
    if conf['all'] or conf['import_objects']:
        calc_z(logger, cursor, road_code)

    # Таблицы, которые надо загрузить в первую очередь и в нужном порядке
    special_tables = (
        'tbl_roadways_line',
        'tbl_crossroads_endline',
        'tbl_crossroads',
        'tbl_roadsides_forcedline',
        'tbl_roadsides_stopline',
        'tbl_roadsides_slopeline',
        'tbl_constructionsapproach',
        'tbl_constructionssidewalk',
        'tbl_tbl_constructions',
        'tbl_busstationsstopping',
        'tbl_busstationspavilions',
        'tbl_busstationslanding',
        'tbl_transitionalroadway'
    )

    # Импорт обочин, кромок и съездов
    if conf['all'] or conf['import_objects']:
        for table in special_tables:
            # Импортировать только если есть входные данные на замену
            if check_input_data(cursor, road_code, table):
                import_table(logger, cursor, road_code, table)

    # Импорт остальных объектов
    if conf['all'] or conf['import_objects']:
        for table in get_tables(cursor):
            # Таблицы, которые были загружены выше
            if table in special_tables:
                continue

            # Пройтись по объектам слоя и загрузить каждый.
            # Импортировать только если есть входные данные на замену.
            if check_input_data(cursor, road_code, table):
                import_table(logger, cursor, road_code, table)

    # Расчёт и запись площадного слоя "Проезжая часть"
    if conf['all'] or conf['roadways']:
        calc_roadways(logger, cursor, road_code, utm)

    # Расчёт ширины ПЧ и обочин
    if conf['all'] or conf['width']:
        calc_width(logger, cursor, utm, road_code)

    # Расчёт поперечных уклонов
    if conf['all'] or conf['transverse_slopes']:
        calc_transverse_slopes(logger, cursor, utm, road_code)

    # Расчёт продольного профиля
    if conf['all'] or conf['latprofile']:
        calc_latprofile(logger, cursor, road_code)

    # Расчёт кривых в плане
    if conf['all'] or conf['curves_in_plane']:
        calc_curves_in_plane(logger, cursor, utm, road_code)

    # Расчёт БКАД диагностики (8 таблиц)
    if conf['def']:
        calc_def(cursor, logger, utm, road_code)

    # Расчёт колейности
    if conf['rut']:
        calc_rut(conf, logger, utm, road_code)

    # Расчёт ровности покрытия
    if conf['iri']:
        calc_iri(cursor, logger, utm, road_code)
