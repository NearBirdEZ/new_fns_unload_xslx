import os

basedir = os.path.abspath(os.path.dirname(__file__))
os.chdir(basedir)


class Config(object):
    """ИНФОРМАЦИЯ ПО ПРОМО ДБ"""
    NAME_DATABASE_PROM = os.environ.get('NAME_DATABASE_PROM') or ''
    USER_DB_PROM = os.environ.get('USER_DB_PROM') or ''
    PASSWORD_DB_PROM = os.environ.get('PASSWORD_DB_PROM') or ''
    HOST_DB_PROM = os.environ.get('HOST_DB_PROM') or ''
    PORT_DB_PROM = os.environ.get('PORT_DB_PROM') or ''

    USER_EL_PROM = os.environ.get('USER_EL_PROM') or ''
    PASSWORD_EL_PROM = os.environ.get('PASSWORD_EL_PROM') or ''
    HOST_EL_PROM = os.environ.get('HOST_EL_PROM') or ''
    PORT_EL_PROM = os.environ.get('PORT_EL_PROM') or ''

    local_version = 0.1
