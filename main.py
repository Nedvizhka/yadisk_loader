import time

from main_utils import *

if __name__ == '__main__':
    while True:
        # ежедневный запуск скрипта происходит только в определенный час start_time в config-файле
        if datetime.now().hour != get_config(get_only_start_time=True):
            time.sleep(100)
            continue
        # если текущий час совпадает с заданным start_time - запускаем скрипт
        else:
            st_time = datetime.now()
            print('Скрипт запущен: ', get_today_date())

            # получение данных для подключения к базам
            ssh_host, ssh_port, ssh_username, ssh_password, database_username, database_password, database_name, \
            localhost, localhost_port, table_name, ya_token, ya_api, ya_link = get_config()

            # создание директории для хранения файлов (если ее нет) и сохранение пути к ней
            local_save_dir_avito = create_load_save_dir('avito')
            local_save_dir_cian = create_load_save_dir('cian')

            # создание переменных для подключения к базам
            sql_server, sql_engine = get_sql_engine(ssh_host, ssh_port, ssh_username, ssh_password, localhost,
                                                    localhost_port, database_username, database_password, database_name)

            # загрузка списка сохраненных файлов
            handled_files_avito = get_saved_files_names('avito')
            handled_files_cian = get_saved_files_names('cian')

            # чтение и сохранение в local_save_dir файлов из ядиска avito
            files_to_process_avito, error_file_loading_avito = download_yadisk_files(ya_api, ya_link,
                                                                                     handled_files_avito,
                                                                                     local_save_dir_avito)

            # чтение и сохранение в local_save_dir файлов из лиичного ядиска cian
            files_to_process_cian, error_file_loading_cian = download_local_yadisk_files(ya_token,
                                                                                         handled_files_cian,
                                                                                         local_save_dir_cian)

            # проверка состояния
            if error_file_loading_avito or error_file_loading_cian:
                time.sleep(300)
                print('Ошибка при загрузке файлов из {}. Перезапуск скрипта...'.format(ya_link))
                error_loading_files = True
                continue
            else:
                error_loading_files = False
                print('ready to process files', files_to_process_avito, files_to_process_cian)

            # обработка файлов и загрузка данных в таблицу
            for filename in files_to_process_avito:
                # обработка realty avito
                print('обработка файла {} для авито'.format(filename))
                df_avito_realty, file_date, error_file_processing = process_realty(local_save_dir_avito, filename,
                                                                                   sql_engine, 'avito')

                # проверка состояния
                if error_file_processing:
                    time.sleep(300)
                    print('Ошибка при обработке файлов из авито {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    continue
                else:
                    error_processing_files = False
                    print('Выгрузка в таблицу realty обработанного файла из авито:', filename)

                # загрузка realty в таблицу
                try:
                    df_avito_realty[list_realty_cols].to_sql(name='realty', con=sql_engine, if_exists='append',
                                           chunksize=7000, method='multi', index=False)
                    # df_avito_realty.to_csv(f'{filename}_test_avito_realty.csv')
                    error_adding_files = False
                except Exception as exc:
                    print(exc)
                    close_sql_connection(sql_server, sql_engine)
                    print('Не удаолсь добавить данные в таблицу {}. Перезапуск скрипта...'.format('realty'))
                    time.sleep(100)
                    error_adding_files = True
                    continue

                # обработка prices avito из realty avito
                df_avito_prices, error_file_processing = create_prices(df_avito_realty, filename, sql_engine, 'avito')

                # проверка состояния
                if error_file_processing:
                    time.sleep(300)
                    print('Ошибка при обработке файлов для prices из авито {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    continue
                else:
                    error_processing_files = False
                    print('Выгрузка в таблицу prices обработанного файла из авито:', filename)
                # загрузка realty в таблицу, запись названия файла в .txt
                try:
                    df_avito_prices.to_sql(name='prices', con=sql_engine, if_exists='append',
                                           chunksize=7000, method='multi', index=False)
                    # df_avito_prices.to_csv(f'{filename}_test_avito_prices.csv')
                    write_saved_file_names(Path(filename).stem, 'avito')
                    error_writing_files = False
                except Exception as exc:
                    print(exc)
                    close_sql_connection(sql_server, sql_engine)
                    print('Не удаолсь добавить данные в таблицу {}. Перезапуск скрипта...'.format('prices'))
                    time.sleep(100)
                    error_writing_files = True
                    continue

            for filename in files_to_process_cian:
                # обработка realty циан
                print('обработка файла {} для cian'.format(filename))
                df_cian_realty, file_date, error_file_processing = process_realty(local_save_dir_cian, filename,
                                                                                  sql_engine, 'cian')

                # проверка состояния
                if error_file_processing:
                    time.sleep(300)
                    print('Ошибка при обработке файлов из CIAN {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    continue
                else:
                    error_processing_files = False
                    print('Выгрузка в таблицу realty обработанного файла из cian:', filename)

                # загрузка realty в таблицу
                try:
                    df_cian_realty[list_realty_cols].to_sql(name='realty', con=sql_engine, if_exists='append',
                                           chunksize=7000, method='multi', index=False)
                    # df_cian_realty.to_csv(f'{filename}_test_cian_realty.csv')
                    error_writing_files = False
                except Exception as exc:
                    print(exc)
                    print('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('realty'))
                    error_writing_files = True
                    continue

                # обработка prices avito из realty avito
                df_cian_prices, error_file_processing = create_prices(df_cian_realty, filename, sql_engine, 'cian')

                # проверка состояния
                if error_file_processing:
                    print('Ошибка при обработке файлов для prices из cian {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    continue
                else:
                    error_processing_files = False
                    print('Выгрузка в таблицу prices обработанного файла из cian:', filename)
                # загрузка realty в таблицу, запись названия файла в .txt
                try:
                    df_cian_prices.to_sql(name='prices', con=sql_engine, if_exists='append',
                                           chunksize=7000, method='multi', index=False)
                    # df_cian_prices.to_csv(f'{filename}_test_cian_prices.csv')
                    write_saved_file_names(Path(filename).stem, 'cian')
                    error_writing_files = False
                except Exception as exc:
                    print(exc)
                    print('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('prices'))
                    error_writing_files = True
                    continue
            try:
                if error_loading_files:
                    print('Ошибка при загрузке файлов')
                    close_sql_connection(sql_server, sql_engine)
                    continue
                elif error_processing_files:
                    print('Ошибка при обработке файлов')
                    close_sql_connection(sql_server, sql_engine)
                    continue
                elif error_writing_files:
                    print('Ошибка при записи файлов в базу')
                    close_sql_connection(sql_server, sql_engine)
                    continue
                else:
                    close_sql_connection(sql_server, sql_engine)
                    print('Новые данные загружены по файлам {} из авито и {} из циана за {}'
                          .format(files_to_process_avito, files_to_process_cian, datetime.now() - st_time))
                    time.sleep(3601)
                    continue
            except:
                close_sql_connection(sql_server, sql_engine)
                print('нет новых данных для загрузки')
                time.sleep(100)
                continue
