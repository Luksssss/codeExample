"""
Валидаторы для работы импорта
"""


class InvalidInputError(Exception):
    """Исключение для проверки входных данных"""
    pass


def check_road(cursor, road_code):
    """Проверить, существует ли ось дороги"""

    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM tbl_roads
            WHERE road_code = %d
        )
    ''' % road_code
    cursor.execute(sql)
    res = cursor.fetchone()['exists']

    return res


def check_tins(cursor, road_code):
    """Проверить поверхности для данной дороги"""

    # Проверка существования таблицы tbl_las_tin
    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'tbl_las_tin'
        )
    '''
    cursor.execute(sql)
    res = cursor.fetchone()['exists']
    if not res:
        return False

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
        return False

    return True


def check_table(cursor, table):
    """Проверить, существует ли таблица в схеме dorgis"""

    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'dorgis' AND table_name = '%s'
        )
    ''' % table
    cursor.execute(sql)
    res = cursor.fetchone()['exists']

    return res

def acad_check_input_data(cursor, road_code, table):
    """Проверить, есть ли данные для импорта в таблицу"""

    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM editor.tbl_acad_objects
            WHERE road_code = '%s' AND table_name = '%s'
        )
    ''' % (road_code, table)
    cursor.execute(sql)
    res = cursor.fetchone()['exists']

    return res


def check_input_data(cursor, road_code, table):
    """Проверить, есть ли данные для импорта в таблицу"""

    sql = '''
        SELECT EXISTS (
            SELECT 1
            FROM editor.tbl_acad_objects
            WHERE road_code = '%s' AND table_name = '%s'
        )
    ''' % (road_code, table)
    cursor.execute(sql)
    res = cursor.fetchone()['exists']

    return res


def check_object(obj, table):
    """Валидация объекта"""

    if not obj.get('acid'):
        raise InvalidInputError('Объект не имеет идентификатора AutoCAD')

    if not obj.get('geom'):
        raise InvalidInputError('Объект не имеет геометрии')
