import logging as logging_module

from main_utils import run_bot_send_msg, move_logfile, create_load_save_dir
from sql_config_utils import close_sql_connection


def error_loading_files_f(sql_server, sql_engine, logging, env_value=None):
    logging.error('Ошибка при загрузке файлов')
    close_sql_connection(sql_server, sql_engine)
    run_bot_send_msg('❌ Завершен процесс: импорт объявлений', logging, monitoring_bot=True)
    local_save_dir_data = create_load_save_dir('data')
    move_logfile(local_save_dir_data, 'error', logging, env_value)
    logff = logging_module.getLogger()
    for i in range(len(logff.handlers)):
        logff.removeHandler(logff.handlers[i])
    logging_module.shutdown()


def error_creating_report_f(sql_server, sql_engine, logging, env_value=None):
    logging.error('Ошибка при создании отчета для tg')
    close_sql_connection(sql_server, sql_engine)
    run_bot_send_msg('❌ Завершен процесс: импорт объявлений', logging, monitoring_bot=True)
    local_save_dir_data = create_load_save_dir('data')
    move_logfile(local_save_dir_data, 'error', logging, env_value)
    logff = logging_module.getLogger()
    for i in range(len(logff.handlers)):
        logff.removeHandler(logff.handlers[i])
    logging_module.shutdown()


def error_getting_ad_id_f(sql_server, sql_engine, logging, env_value=None):
    logging.error('Ошибка при получении ad_id')
    close_sql_connection(sql_server, sql_engine)
    run_bot_send_msg('❌ Завершен процесс: импорт объявлений', logging, monitoring_bot=True)
    local_save_dir_data = create_load_save_dir('data')
    move_logfile(local_save_dir_data, 'error', logging, env_value)
    logff = logging_module.getLogger()
    for i in range(len(logff.handlers)):
        logff.removeHandler(logff.handlers[i])
    logging_module.shutdown()


def error_processing_files_f(sql_server, sql_engine, logging, env_value=None):
    logging.error('Ошибка при обработке файлов')
    close_sql_connection(sql_server, sql_engine)
    run_bot_send_msg('❌ Завершен процесс: импорт объявлений', logging, monitoring_bot=True)
    local_save_dir_data = create_load_save_dir('data')
    move_logfile(local_save_dir_data, 'error', logging, env_value)
    logff = logging_module.getLogger()
    for i in range(len(logff.handlers)):
        logff.removeHandler(logff.handlers[i])
    logging_module.shutdown()


def error_updating_realty_f(sql_server, sql_engine, logging, env_value=None):
    logging.error('Ошибка при обновлении объявлений в таблице realty')
    close_sql_connection(sql_server, sql_engine)
    run_bot_send_msg('❌ Завершен процесс: импорт объявлений', logging, monitoring_bot=True)
    local_save_dir_data = create_load_save_dir('data')
    move_logfile(local_save_dir_data, 'error', logging, env_value)
    logff = logging_module.getLogger()
    for i in range(len(logff.handlers)):
        logff.removeHandler(logff.handlers[i])
    logging_module.shutdown()


def error_writing_files_f(sql_server, sql_engine, logging, env_value=None):
    logging.error('Ошибка при записи файлов в базу')
    close_sql_connection(sql_server, sql_engine)
    run_bot_send_msg('❌ Завершен процесс: импорт объявлений', logging, monitoring_bot=True)
    local_save_dir_data = create_load_save_dir('data')
    move_logfile(local_save_dir_data, 'error', logging, env_value)
    logff = logging_module.getLogger()
    for i in range(len(logff.handlers)):
        logff.removeHandler(logff.handlers[i])
    logging_module.shutdown()


def error_db_con_f(sql_server, sql_engine, logging, env_value=None):
    logging.error('Ошибка подключения к базе')
    close_sql_connection(sql_server, sql_engine)
    run_bot_send_msg('❌ Завершен процесс: импорт объявлений', logging, monitoring_bot=True)
    local_save_dir_data = create_load_save_dir('data')
    move_logfile(local_save_dir_data, 'error', logging, env_value)
    logff = logging_module.getLogger()
    for i in range(len(logff.handlers)):
        logff.removeHandler(logff.handlers[i])
    logging_module.shutdown()
