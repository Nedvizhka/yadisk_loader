from main_utils import *
from finisher_utils import *

# import logging.config
import time

def run_ya_loader(env_value):

    while True:
        logging_module.basicConfig(filename=f'ya_loader{"_"+env_value if env_value != None else ""}.log',
                            filemode='w', format='%(asctime)s [%(levelname)-8s] %(message)s')

        # logging.config.dictConfig({
        #     'version': 1,
        #     'disable_existing_loggers': True,
        # })

        ya_logger = logging_module.getLogger()
        ya_logger.setLevel(logging_module.INFO)

        st_time = datetime.now()
        print('Скрипт запущен: ', get_today_date(), 'выйди из скрина и сделай "tail -f ya_loader.log"')
        # отправить сообщение о старте в бот
        run_bot_send_msg('▶️ Старт процесса: импорт объявлений', ya_logger, monitoring_bot=True)
        ya_logger.info('Скрипт запущен: {}'.format(st_time))
        ya_logger.info(f'создан лог: ya_loader{"_"+env_value if env_value != None else ""}.log')

        # получение данных для подключения к базам
        ssh_host, ssh_port, ssh_username, ssh_password, database_username, database_password, database_name, \
            localhost, localhost_port, table_name, ya_token, ya_api, ya_link = get_config(env_value=env_value)

        # создание директории для хранения файлов (если ее нет) и сохранение пути к ней
        local_save_dir_avito = create_load_save_dir('avito')
        local_save_dir_cian = create_load_save_dir('cian')
        local_save_dir_data = create_load_save_dir('data')

        # создание переменных для подключения к базам
        sql_server, sql_engine = get_sql_engine(ya_logger, env_value)

        # загрузка списка сохраненных файлов
        handled_files_avito = get_saved_files_names('avito', env_value)
        handled_files_cian = get_saved_files_names('cian', env_value)

        # чтение и сохранение в local_save_dir файлов из ядиска avito
        files_to_process_avito, error_file_loading_avito = download_local_yadisk_files(ya_token,
                                                                                       handled_files_avito,
                                                                                       local_save_dir_avito,
                                                                                       'avito', ya_logger)
        # заглушка загрузки avito
        # files_to_process_avito, error_file_loading_avito = [], False

        # чтение и сохранение в local_save_dir файлов из ядиска cian
        files_to_process_cian, error_file_loading_cian = download_local_yadisk_files(ya_token,
                                                                                     handled_files_cian,
                                                                                     local_save_dir_cian,
                                                                                     'cian', ya_logger)
        # заглушка загрузки cian
        # files_to_process_cian, error_file_loading_cian = [], False

        # проверка успешности загрузки файлов
        if error_file_loading_avito or error_file_loading_cian:
            ya_logger.error('Ошибка при загрузке файлов из {}'.format(ya_link))
            # сохранение log файла, увед в тг
            error_loading_files_f(sql_server, sql_engine, ya_logger, env_value)
            break
        else:
            ya_logger.info('ready to process files {}, {}'.format(files_to_process_avito, files_to_process_cian))

        if len(files_to_process_avito) == 0 and len(files_to_process_cian) == 0:
            ya_logger.info('{} файлов для загрузки авито и {} файлов для циан'.format(len(files_to_process_avito),
                                                                                    len(files_to_process_cian)))
            # сохранение log файла
            move_logfile(local_save_dir_data, 'error', ya_logger, env_value)
            time.sleep(900)
            continue

        # создание df для отправки txt отчета в tg
        common_rep_df = create_common_rep()
        dadata_rep_df, error_creating_dadata_report = create_dadata_rep(sql_engine, env_value, ya_logger)
        if error_creating_dadata_report:
            # сохранение log файла, увед в тг
            error_creating_report_f(sql_server, sql_engine, ya_logger, env_value)
            break

        # создание df для ограничения количества запросов к dadata
        jkh_addr_df, err = count_jkh_addr(sql_engine, ya_logger, env_value)

        # обновление status в таблицах
        close_sql_connection(sql_server, sql_engine)
        sql_engine, ya_logger = get_sql_engine(ya_logger, env_value)
        error_updating_realty = update_status(sql_engine, ya_logger)
        if error_updating_realty:
            # сохранение log файла, увед в тг
            error_updating_realty_f(sql_server, sql_engine, ya_logger, env_value)
            break

        ### обработка файлов и загрузка данных в таблицу
        for filename in files_to_process_cian:
            # обработка realty циан
            ya_logger.info('обработка файла {} для cian'.format(filename))
            df_cian_realty, file_date, error_file_processing = process_realty(local_save_dir_cian, filename,
                                                                              sql_engine, 'cian', ya_logger)

            # проверка состояния
            if error_file_processing:
                ya_logger.error('Ошибка при обработке файлов из CIAN {}. Перезапуск скрипта...'.format(filename))
                # сохранение log файла, увед в тг
                error_processing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break
            else:
                ya_logger.info('Выгрузка в таблицу realty обработанного файла из cian: {}'.format(filename))

            # добавление инфо к отчету
            report_df_append(common_rep_df, 'parse_date', file_date)
            report_df_append(common_rep_df, 'c_total', len(df_cian_realty))
            report_df_append(common_rep_df, 'c_adr_total', len(df_cian_realty.addr.unique()))

            sql_server, sql_engine, error_db_con = check_sql_connection(sql_server, sql_engine, ya_logger, env_value)
            if error_db_con:
                # сохранение log файла, увед в тг
                error_db_con_f(sql_server, sql_engine, ya_logger, env_value)
                break

            # выгрузка и обновление данных в таблице realty
            error_create_temp_realty, error_getting_ad_id, error_loading_into_realty, error_updating_realty = \
                load_and_update_realty_db(sql_engine, df_cian_realty, filename, common_rep_df,
                                          dadata_rep_df, jkh_addr_df, 'cian', ya_logger, env_value)

            if any([error_create_temp_realty, error_getting_ad_id,
                    error_loading_into_realty, error_updating_realty]):
                close_sql_connection(sql_server, sql_engine)
                ya_logger.error('Ошибка: {}'.format(show_error([error_create_temp_realty, error_getting_ad_id,
                                                             error_loading_into_realty, error_updating_realty])))
                ya_logger.error('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('realty'))
                # сохранение log файла, увед в тг
                error_writing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break

            # проверка подключения к базе
            sql_server, sql_engine, error_db_con = check_sql_connection(sql_server, sql_engine, ya_logger, env_value)
            if error_db_con:
                # сохранение log файла, увед в тг
                error_db_con_f(sql_server, sql_engine, ya_logger, env_value)
                break

            # обработка prices cian из realty cian
            df_cian_prices, error_file_processing = create_prices(df_cian_realty, filename, sql_engine, 'cian', ya_logger)

            # проверка состояния
            if error_file_processing:
                ya_logger.error('Ошибка при обработке файлов для prices из cian {}. Перезапуск скрипта...'.format(filename))
                # сохранение log файла, увед в тг
                error_processing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break
            else:
                ya_logger.info('Выгрузка в таблицу prices обработанного файла из cian: {}'.format(filename))
            # загрузка и обновление prices в таблицу на сервере, запись названия файла в .txt
            try:
                df_cian_prices.to_sql(name='prices', con=sql_engine, if_exists='append',
                                      chunksize=7000, method='multi', index=False)
                # df_cian_prices.to_csv(f'{filename}_test_cian_prices.csv')
                write_saved_file_names(Path(filename).stem, env_value, 'cian')
                ya_logger.info('данные cian загружены успешно')
            except Exception as exc:
                ya_logger.error(traceback.format_exc())
                ya_logger.error('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('prices'))
                # сохранение log файла, увед в тг
                error_writing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break

        # проверка подключения к базе
        sql_server, sql_engine, error_db_con = check_sql_connection(sql_server, sql_engine, ya_logger, env_value)
        if error_db_con:
            # сохранение log файла, увед в тг
            error_db_con_f(sql_server, sql_engine, ya_logger, env_value)
            break

        for filename in files_to_process_avito:
            # обработка realty avito
            ya_logger.info('обработка файла {} для авито'.format(filename))
            df_avito_realty, file_date, error_file_processing = process_realty(local_save_dir_avito, filename,
                                                                               sql_engine, 'avito', ya_logger)
            # ограничение городов авито
            df_avito_realty = df_avito_realty[df_avito_realty.city_id.isin([4, 18, 12, 7, 17, 2, 23, 3, 24, 25, 26])]

            # проверка состояния
            if error_file_processing:
                ya_logger.error('Ошибка при обработке файлов из авито {}. Перезапуск скрипта...'.format(filename))
                # сохранение log файла, увед в тг
                error_processing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break
            else:
                ya_logger.info('Выгрузка в таблицу realty обработанного файла из авито: {}'.format(filename))

            # добавление инфо к отчету
            report_df_append(common_rep_df, 'av_total', len(df_avito_realty))
            report_df_append(common_rep_df, 'av_adr_total', len(df_avito_realty.addr.unique()))

            # выгрузка и обновление данных в таблице realty
            error_create_temp_realty, error_getting_ad_id, error_loading_into_realty, error_updating_realty = \
                load_and_update_realty_db(sql_engine, df_avito_realty, filename, common_rep_df,
                                          dadata_rep_df, jkh_addr_df, 'avito', ya_logger, env_value)

            if any([error_create_temp_realty, error_getting_ad_id,
                    error_loading_into_realty, error_updating_realty]):
                close_sql_connection(sql_server, sql_engine)
                ya_logger.error('Ошибка: {}'.format(show_error([error_create_temp_realty, error_getting_ad_id,
                                                              error_loading_into_realty, error_updating_realty])))
                ya_logger.error('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('realty'))
                # сохранение log файла, увед в тг
                error_writing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break

            # проверка подключения к базе
            sql_server, sql_engine, error_db_con = check_sql_connection(sql_server, sql_engine, ya_logger, env_value)
            if error_db_con:
                # сохранение log файла, увед в тг
                error_db_con_f(sql_server, sql_engine, ya_logger, env_value)
                break

            # обработка prices avito из realty avito
            df_avito_prices, error_file_processing = create_prices(df_avito_realty, filename, sql_engine, 'avito', ya_logger)

            # проверка состояния
            if error_file_processing:
                ya_logger.error('Ошибка при обработке файлов для prices из авито {}. Перезапуск скрипта...'.format(filename))
                # сохранение log файла, увед в тг
                error_processing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break
            else:
                ya_logger.info('Выгрузка в таблицу prices обработанного файла из авито: {}'.format(filename))
            # загрузка prices в таблицу, запись названия файла в .txt
            try:
                df_avito_prices.to_sql(name='prices', con=sql_engine, if_exists='append',
                                       chunksize=7000, method='multi', index=False)
                write_saved_file_names(Path(filename).stem, env_value, 'avito')
                ya_logger.info('данные avito загружены успешно')
            except Exception as exc:
                ya_logger.error(traceback.format_exc())
                ya_logger.error('Не удалось добавить данные в таблицу {}. Перезапуск скрипта...'.format('prices'))
                # сохранение log файла, увед в тг
                error_writing_files_f(sql_server, sql_engine, ya_logger, env_value)
                break

        drop_temp_table(sql_engine, 'temp_realty_new', ya_logger, env_value)
        drop_temp_table(sql_engine, 'temp_jkh_houses', ya_logger, env_value)

        try:
            close_sql_connection(sql_server, sql_engine)
            ya_logger.info('Новые данные загружены по файлам {} из авито и {} из циана за {}'
                         .format(files_to_process_avito, files_to_process_cian, datetime.now() - st_time))
            print('Новые данные загружены по файлам {} из авито и {} из циана за {}'
                  .format(files_to_process_avito, files_to_process_cian, datetime.now() - st_time))
            # проверка баланса dadata
            dadata_balance = check_balance()
            # создание и отправка отчета
            if env_value == None:
                report_txt = report_text(common_rep_df, dadata_rep_df, jkh_addr_df, dadata_balance, ya_logger)
                if report_txt:
                    run_bot_send_msg(report_txt, ya_logger)
                    run_bot_send_msg('✅ Завершен процесс: импорт объявлений', ya_logger, monitoring_bot=True)
                else:
                    pass
            move_logfile(local_save_dir_data, 'success', ya_logger, env_value)
            logff = logging_module.getLogger()
            for i in range(len(logff.handlers)):
                logff.removeHandler(logff.handlers[i])
            logging_module.shutdown()
            break
        except:
            close_sql_connection(sql_server, sql_engine)
            ya_logger.info('ошибка, чек лог')
            run_bot_send_msg('❌ Завершен процесс: импорт объявлений', ya_logger, monitoring_bot=True)
            move_logfile(local_save_dir_data, 'no_new_file', ya_logger, env_value)
            logff = logging_module.getLogger()
            for i in range(len(logff.handlers)):
                logff.removeHandler(logff.handlers[i])
            logging_module.shutdown()
            time.sleep(1100)
            continue