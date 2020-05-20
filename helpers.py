import sys
from datetime import datetime


class Timer(object):
    """Засекатель времени выполнения"""

    def start(self):
        self.time_start = datetime.now()

    def end(self):
        time_end = datetime.now()
        duration = time_end.timestamp() - self.time_start.timestamp()
        return duration


def fmt_ex(e):
    """Форматирование ошибок для логов"""
    return str(e).replace('\n', '; ')


def ask_confirmation(question):
    """Спросить подтверждение"""
    print(question, end=' ')
    answer = input()
    print()
    return answer == 'y'


def total_exit(logger, timer, err=None):
    """Полностью завершить выполнения скрипта и вывести результат"""

    duration = timer.end()

    if err:
        status = 'FAIL'
        code = 1
    else:
        status = 'SUCCESS'
        code = 0

    logger.info('DURATION: %d' % duration)
    logger.info('STATUS: '+status)
    if err:
        logger.info('MESSAGE: '+err)
    else:
        logger.info('MESSAGE: Завершено успешно')
    sys.exit(code)
