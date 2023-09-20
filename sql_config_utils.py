import configparser
import traceback

from sqlalchemy import create_engine, NullPool
from sshtunnel import SSHTunnelForwarder

from sqlalchemy import text

import logging


def get_config(env_value=None, get_only_start_time=False):
    config = configparser.ConfigParser()
    config.read('config.ini')

    ssh_host = config['database']['ssh_host_main']  # переключить на ssh_host_main для работы на прод сервере
    ssh_port = int(config['database']['ssh_port'])
    ssh_username = config['database']['ssh_username']
    ssh_password = config['database']['ssh_password']
    database_username = config['database']['database_username']
    database_password = config['database']['database_password']
    if env_value == None:
        database_name = config['database']['database_name']
    else:
        database_name = config['database']['preprod_database_name']
    localhost = config['database']['localhost']
    localhost_port = int(config['database']['localhost_port'])
    table_name = config['database']['table_name']

    ya_token = config['yandex']['ya_token']
    ya_api = config['yandex']['ya_api']
    ya_link = config['yandex']['ya_link']

    start_time = int(config['start_time']['daily_start_hour'])

    if get_only_start_time == False:
        return ssh_host, ssh_port, ssh_username, ssh_password, database_username, database_password, database_name, \
               localhost, localhost_port, table_name, ya_token, ya_api, ya_link
    else:
        return start_time


def get_sql_engine(env_value=None):
    ssh_host, ssh_port, ssh_username, ssh_password, database_username, database_password, \
        database_name, localhost, localhost_port, table_name, ya_token, ya_api, ya_link = get_config(env_value=env_value)

    sql_server = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=(localhost, localhost_port)
    )

    sql_server.start()
    local_port = str(sql_server.local_bind_port)
    sql_engine = create_engine('mariadb+pymysql://{}:{}@{}:{}/{}'.format(database_username, database_password,
                                                                         localhost, local_port, database_name),
                               poolclass=NullPool)
    
    logging.info('connected to host {} base {}'.format(ssh_host, database_name))
    
    return sql_server, sql_engine


def check_sql_connection(sql_server, sql_engine):
    try:
        con_obj = sql_engine.connect()
        con_obj.close()
        return sql_server, sql_engine, None
    except:
        try:
            sql_server, sql_engine = get_sql_engine()
            logging.info('подключение к базе восстановлено')
            return sql_server, sql_engine, None
        except Exception as exc:
            logging.error('не удается подключиться к базе')
            return None, None, True


def load_df_into_sql_table(df, table_name, engine, bigsize=False):
    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine()
            logging.info('подключение к базе восстановлено')
        except Exception:
            logging.error('не удается подключиться к базе {}'.format(traceback.format_exc()))
    chunk_s = 2500 if bigsize else 5000

    df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=chunk_s, method='multi', index=False)


def drop_temp_table(engine, table):
    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine()
            logging.info('подключение к базе восстановлено')
        except Exception as exc:
            logging.error('не удается подключиться к базе: {}'.format(traceback.format_exc()))
            return exc
    try:
        delete_table_query = \
            f"""DROP table {table}"""
        con_obj = engine.connect()
        con_obj.execute(text(delete_table_query))
        con_obj.commit()
        con_obj.close()
        return None
    except Exception as exc:
        logging.error('не удалось удалить таблицу: {} ({})'.format(table, traceback.format_exc()))
        return exc


def close_sql_connection(server, engine):
    server.stop()
    engine.dispose()
