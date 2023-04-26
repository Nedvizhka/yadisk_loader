import time

from main_utils import *
from sql_utils import *


def get_config_1(get_only_start_time=False):
    config = configparser.ConfigParser()
    config.read('config.ini')

    ssh_host = config['database']['ssh_host_test']  # переключить на ssh_host_main для работы на прод сервере
    ssh_port = int(config['database']['ssh_port'])
    ssh_username = config['database']['ssh_username']
    ssh_password = config['database']['ssh_password']
    database_username = config['database']['database_username']
    database_password = config['database']['database_password']
    database_name = config['database']['database_name']  # переключить на database_name для работы на прод сервере
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


if __name__ == '__main__':
    while True:
        if datetime.now().hour != get_config(get_only_start_time=True):
            continue
        else:
            # get config
            ssh_host, ssh_port, ssh_username, ssh_password, database_username, database_password, database_name, \
                localhost, localhost_port, table_name, ya_token, ya_api, ya_link = get_config_1()

            # create dir
            local_save_dir_avito = create_load_save_dir('avito')
            local_save_dir_cian = create_load_save_dir('cian')

            handled_files_cian = get_saved_files_names('cian')

            # create conn
            sql_server, sql_engine = get_sql_engine(ssh_host, ssh_port, ssh_username, ssh_password, localhost,
                                                    localhost_port, database_username, database_password, database_name)

            files_to_process_cian, error_file_loading_cian = download_yadisk_files(ya_api, ya_link,
                                                                                   handled_files_cian,
                                                                                   local_save_dir_cian, 'cian')
            print('error is', error_file_loading_cian)
            print('got files')
            # files_to_process_cian, error_file_loading_cian = ['циан 26-04-23 (без дублей).csv'], False
            files_to_process_avito, error_file_loading_avito = [''], False

            for filename in files_to_process_cian:
                # обработка realty циан
                print('обработка файла {} для cian'.format(filename))
                df_cian_realty, file_date, error_file_processing = process_realty(local_save_dir_cian, filename,
                                                                                  sql_engine, 'cian')
                print('обработка ок')
            close_sql_connection(sql_server, sql_engine)
            print('con closed')
            time.sleep(100)
            continue
