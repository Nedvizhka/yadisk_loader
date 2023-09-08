from main_utils import *

import logging
import time

if __name__ == '__main__':
    # определение аргументов для запуска скрипта на препрод/прод (--env 'preprod' в запуске)
    parser = argparse.ArgumentParser(description='parse arguments to run script on prod or preprod')
    parser.add_argument("--env")
    args = parser.parse_args()
    env_value = args.env
    print("скрипт запущен с аргументом: ", str(env_value))

    while True:
        # ежедневный запуск скрипта происходит только в определенный час start_time в config файле
        if datetime.now().hour != get_config(get_only_start_time=True):
            # проверка баланса за 2 часа до запуска
            if env_value == None and datetime.now().hour == get_config(get_only_start_time=True) - 2:
                try:
                    dadata_balance = check_balance()
                    balance_txt = f'Остаток баланса {dadata_balance} ₽ {"❌" if dadata_balance < 500 else "✅"}'
                    run_bot_send_msg(balance_txt)
                    print(f'Баланс {dadata_balance} отправлен в tg')
                    time.sleep(3601)
                    continue
                except:
                    print('Ошибка, ', traceback.format_exc())
                    continue
            else:
                time.sleep(100)
                continue
        # если текущий час совпадает с заданным start_time - запускаем скрипт
        else:
            logging.basicConfig(filename=f'ya_loader{"_"+env_value if env_value != None else ""}.log',
                                filemode='w', level=logging.INFO,
                                format='%(asctime)s [%(levelname)-8s] %(message)s', encoding='cp1251')
            st_time = datetime.now()
            print('Скрипт запущен: ', get_today_date(), 'выйди из скрина и сделай "tail -f ya_loader.log"')
            logging.info('Скрипт запущен: {}'.format(st_time))

            # получение данных для подключения к базам
            ssh_host, ssh_port, ssh_username, ssh_password, database_username, database_password, database_name, \
                localhost, localhost_port, table_name, ya_token, ya_api, ya_link = get_config(env_value=env_value)

            # создание директории для хранения файлов (если ее нет) и сохранение пути к ней
            local_save_dir_avito = create_load_save_dir('avito')
            local_save_dir_cian = create_load_save_dir('cian')
            local_save_dir_data = create_load_save_dir('data')

            # создание переменных для подключения к базам
            sql_server, sql_engine = get_sql_engine()

            # загрузка списка сохраненных файлов
            handled_files_avito = get_saved_files_names('avito', env_value)
            handled_files_cian = get_saved_files_names('cian', env_value)

            # чтение и сохранение в local_save_dir файлов из ядиска avito
            files_to_process_avito, error_file_loading_avito = download_local_yadisk_files(ya_token,
                                                                                           handled_files_avito,
                                                                                           local_save_dir_avito,
                                                                                           'avito')
            # заглушка загрузки avito
            # files_to_process_avito, error_file_loading_avito = [], False

            # чтение и сохранение в local_save_dir файлов из ядиска cian
            files_to_process_cian, error_file_loading_cian = download_local_yadisk_files(ya_token,
                                                                                         handled_files_cian,
                                                                                         local_save_dir_cian,
                                                                                         'cian')
            # заглушка загрузки cian
            # files_to_process_cian, error_file_loading_cian = [], False

            # проверка успешности загрузки файлов
            if error_file_loading_avito or error_file_loading_cian:
                time.sleep(300)
                logging.error('Ошибка при загрузке файлов из {}. Перезапуск скрипта...'.format(ya_link))
                # сохранение log файла
                error_loading_files = True
                move_logfile(local_save_dir_data, 'error', env_value)
                continue
            else:
                error_loading_files = False
                logging.info('ready to process files {}, {}'.format(files_to_process_avito, files_to_process_cian))

            if len(files_to_process_avito) == 0 and len(files_to_process_cian) == 0:
                logging.info('{} файлов для загрузки авито и {} файлов для циан'.format(len(files_to_process_avito),
                                                                                        len(files_to_process_cian)))
                # сохранение log файла
                move_logfile(local_save_dir_data, 'no_new_file', env_value)
                time.sleep(900)
                continue

            # создание df для отправки txt отчета в tg
            common_rep_df = create_common_rep()
            dadata_rep_df, error_creating_dadata_report = create_dadata_rep(sql_engine)
            if error_creating_dadata_report:
                time.sleep(300)
                logging.error('Ошибка при создании отчета для tg. Перезапуск скрипта...')
                error_loading_files = True
                # сохранение log файла
                move_logfile(local_save_dir_data, 'error', env_value)
                continue

            # создание df для ограничения количества запросов к dadata
            jkh_addr_df, err = count_jkh_addr(sql_engine)

            ### обработка файлов и загрузка данных в таблицу
            for filename in files_to_process_cian:
                # обработка realty циан
                logging.info('обработка файла {} для cian'.format(filename))
                df_cian_realty, file_date, error_file_processing = process_realty(local_save_dir_cian, filename,
                                                                                  sql_engine, 'cian')

                # добавление инфо к отчету
                report_df_append(common_rep_df, 'parse_date', file_date)
                report_df_append(common_rep_df, 'c_total', len(df_cian_realty))
                report_df_append(common_rep_df, 'c_adr_total', len(df_cian_realty.addr.unique()))


                # проверка состояния
                if error_file_processing:
                    logging.error('Ошибка при обработке файлов из CIAN {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    break
                else:
                    error_processing_files = False
                    logging.info('Выгрузка в таблицу realty обработанного файла из cian: {}'.format(filename))

                # выгрузка и обновление данных в таблице realty
                error_create_temp_realty, error_getting_ad_id, error_loading_into_realty, error_updating_realty = \
                    load_and_update_realty_db(sql_engine, df_cian_realty, filename, common_rep_df,
                                              dadata_rep_df, jkh_addr_df, 'cian')

                if any([error_create_temp_realty, error_getting_ad_id,
                        error_loading_into_realty, error_updating_realty]):
                    close_sql_connection(sql_server, sql_engine)
                    logging.error('Ошибка: {}'.format(show_error([error_create_temp_realty, error_getting_ad_id,
                                                                 error_loading_into_realty, error_updating_realty])))
                    logging.error('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('realty'))
                    time.sleep(10)
                    break

                # проверка подключения к базе
                sql_server, sql_engine, error_db_con = check_sql_connection(sql_server, sql_engine)
                if error_db_con:
                    break

                # обработка prices cian из realty cian
                df_cian_prices, error_file_processing = create_prices(df_cian_realty, filename, sql_engine, 'cian')

                # проверка состояния
                if error_file_processing:
                    logging.error('Ошибка при обработке файлов для prices из cian {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    break
                else:
                    error_processing_files = False
                    logging.info('Выгрузка в таблицу prices обработанного файла из cian: {}'.format(filename))
                # загрузка и обновление prices в таблицу на сервере, запись названия файла в .txt
                try:
                    df_cian_prices.to_sql(name='prices', con=sql_engine, if_exists='append',
                                          chunksize=7000, method='multi', index=False)
                    # df_cian_prices.to_csv(f'{filename}_test_cian_prices.csv')
                    write_saved_file_names(Path(filename).stem, 'cian')
                    logging.info('данные cian загружены успешно')
                    error_writing_files = False
                except Exception as exc:
                    logging.error(traceback.format_exc())
                    logging.error('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('prices'))
                    error_writing_files = True
                    break

            # проверка подключения к базе
            sql_server, sql_engine, error_db_con = check_sql_connection(sql_server, sql_engine)
            if error_db_con:
                break

            for filename in files_to_process_avito:
                # обработка realty avito
                logging.info('обработка файла {} для авито'.format(filename))
                df_avito_realty, file_date, error_file_processing = process_realty(local_save_dir_avito, filename,
                                                                                   sql_engine, 'avito')
                # ограничение городов авито
                df_avito_realty = df_avito_realty[df_avito_realty.city_id.isin([4, 18, 12, 7, 17, 2, 23, 3])]

                # добавление инфо к отчету
                report_df_append(common_rep_df, 'av_total', len(df_avito_realty))
                report_df_append(common_rep_df, 'av_adr_total', len(df_avito_realty.addr.unique()))

                # проверка состояния
                if error_file_processing:
                    time.sleep(300)
                    logging.error('Ошибка при обработке файлов из авито {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    break
                else:
                    error_processing_files = False
                    logging.info('Выгрузка в таблицу realty обработанного файла из авито: {}'.format(filename))

                # выгрузка и обновление данных в таблице realty
                error_create_temp_realty, error_getting_ad_id, error_loading_into_realty, error_updating_realty = \
                    load_and_update_realty_db(sql_engine, df_avito_realty, filename, common_rep_df,
                                              dadata_rep_df, jkh_addr_df, 'avito')

                if any([error_create_temp_realty, error_getting_ad_id,
                        error_loading_into_realty, error_updating_realty]):
                    close_sql_connection(sql_server, sql_engine)
                    logging.error('Ошибка: {}'.format(show_error([error_create_temp_realty, error_getting_ad_id,
                                                                  error_loading_into_realty, error_updating_realty])))
                    logging.error('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('realty'))
                    time.sleep(10)
                    break

                # проверка подключения к базе
                sql_server, sql_engine, error_db_con = check_sql_connection(sql_server, sql_engine)
                if error_db_con:
                    break

                # обработка prices avito из realty avito
                df_avito_prices, error_file_processing = create_prices(df_avito_realty, filename, sql_engine, 'avito')

                # проверка состояния
                if error_file_processing:
                    time.sleep(300)
                    logging.error('Ошибка при обработке файлов для prices из авито {}. Перезапуск скрипта...'.format(filename))
                    error_processing_files = True
                    break
                else:
                    error_processing_files = False
                    logging.info('Выгрузка в таблицу prices обработанного файла из авито: {}'.format(filename))
                # загрузка prices в таблицу, запись названия файла в .txt
                try:
                    df_avito_prices.to_sql(name='prices', con=sql_engine, if_exists='append',
                                           chunksize=7000, method='multi', index=False)
                    write_saved_file_names(Path(filename).stem, 'avito')
                    logging.info('данные avito загружены успешно')
                    error_writing_files = False
                except Exception as exc:
                    logging.info(traceback.format_exc())
                    close_sql_connection(sql_server, sql_engine)
                    logging.info('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('prices'))
                    error_writing_files = True
                    break

            drop_temp_table(sql_engine, 'temp_realty_new')
            drop_temp_table(sql_engine, 'temp_jkh_houses')

            try:
                if error_loading_files:
                    logging.error('Ошибка при загрузке файлов')
                    close_sql_connection(sql_server, sql_engine)
                    move_logfile(local_save_dir_data, 'error', env_value)
                    time.sleep(1000)
                    continue
                elif error_getting_ad_id:
                    logging.error('Ошибка при получении ad_id')
                    close_sql_connection(sql_server, sql_engine)
                    move_logfile(local_save_dir_data, 'error', env_value)
                    time.sleep(1000)
                    continue
                elif error_processing_files:
                    logging.error('Ошибка при обработке файлов')
                    close_sql_connection(sql_server, sql_engine)
                    move_logfile(local_save_dir_data, 'error', env_value)
                    time.sleep(1000)
                    continue
                elif error_updating_realty:
                    logging.error('Ошибка при обновлении объявлений в таблице realty')
                    close_sql_connection(sql_server, sql_engine)
                    move_logfile(local_save_dir_data, 'error', env_value)
                    time.sleep(1000)
                    continue
                elif error_writing_files:
                    logging.error('Ошибка при записи файлов в базу')
                    close_sql_connection(sql_server, sql_engine)
                    move_logfile(local_save_dir_data, 'error', env_value)
                    time.sleep(1000)
                    continue
                elif error_db_con:
                    logging.error('Ошибка подключения к базе')
                    close_sql_connection(sql_server, sql_engine)
                    move_logfile(local_save_dir_data, 'error', env_value)
                    time.sleep(1000)
                    continue
                else:
                    close_sql_connection(sql_server, sql_engine)
                    logging.info('Новые данные загружены по файлам {} из авито и {} из циана за {}'
                                 .format(files_to_process_avito, files_to_process_cian, datetime.now() - st_time))
                    print('Новые данные загружены по файлам {} из авито и {} из циана за {}'
                          .format(files_to_process_avito, files_to_process_cian, datetime.now() - st_time))
                    # проверка баланса dadata
                    dadata_balance = check_balance()
                    # создание и отправка отчета
                    if env_value == None:
                        report_txt = report_text(common_rep_df, dadata_rep_df, jkh_addr_df, dadata_balance)
                        if report_txt:
                            run_bot_send_msg(report_txt)
                        else:
                            pass
                    move_logfile(local_save_dir_data, 'success', env_value)
                    time.sleep(3500)
                    continue
            except:
                close_sql_connection(sql_server, sql_engine)
                logging.info('нет новых данных для загрузки')
                move_logfile(local_save_dir_data, 'no_new_file', env_value)
                time.sleep(1100)
                continue