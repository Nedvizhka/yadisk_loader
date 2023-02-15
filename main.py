import configparser
import time

from main_utils import *


if __name__ == '__main__':
    while True:
        config = configparser.ConfigParser()
        config.read('config.ini')
        if datetime.now().hour != int(config['start_time']['daily_start_hour']):
            time.sleep(100)
            continue
        else:
            print('Скрипт запущен: ', get_today_date())

            ssh_host = config['database']['ssh_host']
            ssh_port = int(config['database']['ssh_port'])
            ssh_username = config['database']['ssh_username']
            ssh_password = config['database']['ssh_password']
            database_username = config['database']['database_username']
            database_password = config['database']['database_password']
            database_name = config['database']['database_name']
            localhost = config['database']['localhost']
            localhost_port = int(config['database']['localhost_port'])
            table_name = config['database']['table_name']
            ya_api = config['yandex']['ya_api']
            ya_link = config['yandex']['ya_link']

            local_save_dir = create_load_save_dir()

            sql_server, sql_engine = get_sql_engine(ssh_host, ssh_port, ssh_username, ssh_password, localhost,
                                                    localhost_port, database_username, database_password, database_name)

            files_in_db, error_with_db_con = get_db_saved_files(sql_engine)

            if error_with_db_con:
                close_sql_connection(sql_server, sql_engine)
                time.sleep(100)
                print('Ошибка при чтении таблицы {}. Перезапуск скрипта...'.format(table_name))
                continue

            error_file_loading = download_yadisk_files(ya_api, ya_link, files_in_db, local_save_dir)

            if error_file_loading:
                time.sleep(100)
                print('Ошибка при загрузке файлов из {}. Перезапуск скрипта...'.format(ya_link))
                continue

            try:
                csv_upload = create_csv(local_save_dir)
                csv_upload.to_sql(name=table_name, con=sql_engine, if_exists='append',
                                  chunksize=7000, method='multi')
                delete_files(local_save_dir)
                close_sql_connection(sql_server, sql_engine)
                print('Новые данные успешно добавлены в таблицу {}'.format(table_name))
                time.sleep(3601)
                continue
            except Exception as exc:
                print(exc)
                delete_files(local_save_dir)
                close_sql_connection(sql_server, sql_engine)
                print('Не удаолсь добавить данные в таблицу {}. Перезапуск скрипта...'.format(table_name))
                time.sleep(100)
                continue