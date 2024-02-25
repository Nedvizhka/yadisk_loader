import time
import numpy as np

from dadata import Dadata
from tqdm import tqdm


from logging_utils import *
from report_utils import *

import argparse
# запрос к дадата

def create_ddt_save_dir(source):
    try:
        Path.mkdir(Path.cwd() / f'saved_{source}_csv')
    except:
        pass
    save_dir = (Path.cwd() / f'saved_{source}_csv').as_posix()
    return save_dir

def filter_addr_for_dadata(addrString, cnt_df, source):
    addrList = addrString.split('; ')
    min_len = 3 if source == 'avito' else 4
    city_name = addrList[0 if source == 'avito' else 1]

    # короткий адрес
    flag_less = len(addrList) < min_len
    # номер дома corrupt
    flag_notHouse = len(addrList[-1]) > 10
    # нет номера дома
    flag_notHouseNum = not any(del_el in addrList[-1] for del_el in [str(n) for n in range(0, 10)])
    # города нет в jkh
    flag_notKnownCity = city_name not in cnt_df.name.unique()
    # переполнение лимита запросов
    if not flag_notKnownCity:
        flag_overlimit = cnt_df[cnt_df.name == city_name][['cnt', 'cnt_ddt']].diff(axis=1).iloc[0, 1] > 0
        if flag_overlimit:
            cnt_df.loc[cnt_df['name'].isin([city_name]), 'cnt_left_after_limit'] += 1
    else:
        # если города нет - будет 2 флага прерывания
        flag_overlimit = True

    if flag_less or flag_notHouse or flag_notHouseNum or flag_notKnownCity or flag_overlimit:
        return None
    else:
        if source == 'cian':
            addrList = list(filter(lambda c: 'мкр' not in c, addrList))
        else:
            addrList = list(filter(lambda c: not any(del_el in c for del_el in [str(i) for i in range(0,10)] if 'мкр' in c), addrList))
            # addrList = list(filter(lambda c: 'мкр' in c, addrList))
        return '; '.join(addrList)


def dadata_request(df, file_date, jkh_cnt_df, source, logging):
    st_time = datetime.now()
    file_date = str(file_date)
    # dadata_credentials
    token = "f288b25edb6d05b5ceb4d957376104a181c4adee"
    secret = "9d337ae6b9901a6708802eaca6d7055ce2c64772"
    dadata = Dadata(token, secret)
    # create df for dadata_house_loading
    local_save_dir_data = create_ddt_save_dir('data')
    try:
        logging.info('загрузка данных dadata из {}'.format(local_save_dir_data + f'/{source}_dadata_request.csv'))
        dh_df = pd.DataFrame(columns=['house_fias_id', 'data', 'geo_lat', 'geo_lon', 'street',
                                      'house', 'qc', 'result', 'qc_geo', 'addr', 'parsed_now']) # parsed now для отчета
        dh_df_hist = pd.read_csv(local_save_dir_data + f'/{source}_dadata_request.csv',
                            index_col=0,
                            encoding='cp1251')
        dh_df_hist.drop(['ad_id'], axis=1, inplace=True)
        dh_df_hist.drop_duplicates(inplace=True)
        dh_df_hist.reset_index(drop=True, inplace=True)
        
        exist_ddt_addr = dh_df_hist.addr.unique().tolist()
        logging.info('удалось загрузить исторические данные dadata')
        df_for_count = df.drop_duplicates(subset='addr')
        count_zapros = df_for_count[~df_for_count.addr.isin(exist_ddt_addr)]
    except:
        logging.error('нет исторических данных {} - будет создан новый df для запросов к dadata'.format(
            local_save_dir_data + f'/{source}_dadata_request.csv'))
        dh_df = pd.DataFrame(columns=['house_fias_id', 'data', 'geo_lat', 'geo_lon', 'street',
                                      'house', 'qc', 'result', 'qc_geo', 'addr', 'parsed_now'])
        dh_df_hist = pd.DataFrame(columns=['house_fias_id', 'data', 'geo_lat', 'geo_lon', 'street',
                                           'house', 'qc', 'result', 'qc_geo', 'addr', 'parsed_now'])
        exist_ddt_addr = False
        count_zapros = df.drop_duplicates(subset='addr')
        time.sleep(3)
    # count bad addr and missed queries
    bad_addr = 0
    # to upload dadata result right away when query crashes
    uploading_cnt = 0
    # create dir for bad_addr to store
    bad_addr_txt_root = (
                Path(local_save_dir_data) / f'{source}_bad_addr_{file_date[:10].replace("-", "_")}.txt').as_posix()
    bad_req_txt_root = (
                Path(local_save_dir_data) / f'{source}_bad_request_{file_date[:10].replace("-", "_")}.txt').as_posix()
    # tqdm to logger for dadata request
    # logger = logging.getLogger()
    tqdm_out = TqdmToLogger(logging, level=logging_module.INFO)
    # dadata request
    time.sleep(2)
    logging.info('количество запросов к dadata составит: {}'.format(len(set(df.addr.unique().tolist())
                                                                        - set(dh_df.addr.unique().tolist())
                                                                        )
                                                                    )
                 )

    # определение аргументов для запуска скрипта на препрод/прод (--env 'preprod' в запуске)
    parser = argparse.ArgumentParser(description='parse arguments to run script on prod or preprod')
    parser.add_argument("--env")
    args = parser.parse_args()
    env_value = args.env

    for i, row in tqdm(df.drop_duplicates(subset='addr').iterrows(), total=count_zapros.shape[0],
                       file=tqdm_out, mininterval=10):
        if exist_ddt_addr:
            if row.addr in exist_ddt_addr:
                df_row = dh_df_hist.loc[dh_df_hist.addr == row.addr].values.tolist()[0]
                df_row.append(False)
                dh_df.loc[len(dh_df)] = df_row
                continue
        try:
            addr = filter_addr_for_dadata(row.addr, jkh_cnt_df, source)
            if addr:
                # обновление счетчика для ограничения запросов
                if env_value == None: # обращение к dadata только в случае запуска на проде:
                    d_res = dadata.clean("address", addr)
                    jkh_cnt_df.loc[jkh_cnt_df['name'].isin([addr.split('; ')[0 if source == 'avito' else 1]]), 'cnt_ddt'] += 1
                    dh_df.loc[len(dh_df.index)] = [d_res['house_fias_id'], str(d_res), d_res['geo_lat'], d_res['geo_lon'],
                                                   d_res['street'], d_res['house'], d_res['qc'], d_res['result'],
                                                   d_res['qc_geo'], row.addr, True]
                else:
                    pass
            else:
                # write ad_id and addr to txt
                with open(bad_addr_txt_root, 'a') as wr:
                    wr.writelines(f"{row.ad_id}: {row.addr}" + ',\n')
                wr.close()
                bad_addr += 1
        except Exception as exc:
            try:
                d_balance = dadata.get_balance()
            except:
                d_balance = 99999
            if d_balance > 10:
                logging.error('{}, try reconnect'.format(traceback.format_exc()))
                if uploading_cnt == 0:
                    dh_df.to_csv(
                        local_save_dir_data + f'/{source}_dadata_request_err_{file_date[:10].replace("-", "_")}.csv',
                        encoding='cp1251')
                    uploading_cnt += 1
                # write ad_id and addr to txt
                with open(bad_req_txt_root, 'a') as wr:
                    wr.writelines(f"{row.ad_id}: {row.addr}" + ',\n')
                wr.close()
                bad_addr += 1
                time.sleep(1)
                try:
                    dadata.close()
                    dadata = Dadata(token, secret)
                except:
                    pass
            else:
                logging.info(f'закончились деньги на Dadata: остаток {d_balance} Р')
                break

    tqdm_out.flush()

    try:
        dadata.close()
        logging.info('dadata con closed succesfully')
    except:
        logging.error('dadata con was not closed succesfully')

    logging.info('обращение к DDT по {}/{} адресам из {} заняло {}'.format(len(df.drop_duplicates(subset='addr')) - bad_addr,
                                                                           len(df.drop_duplicates(subset='addr')),
                                                                           source, datetime.now() - st_time))
    dh_df = dh_df[dh_df['addr'].isin(df.addr.unique().tolist())]

    dh_df = df.merge(dh_df, on=['addr'], how='left')
    dh_df.drop_duplicates(inplace=True)
    dh_df_to_tg = dh_df[['house_fias_id', 'data', 'geo_lat', 'geo_lon', 'street', 'house', 'qc', 'result', 'qc_geo', 'ad_id', 'addr', 'parsed_now']]
    dh_df = dh_df[['house_fias_id', 'data', 'geo_lat', 'geo_lon', 'street', 'house', 'qc', 'result', 'qc_geo', 'ad_id', 'addr']]
    dh_df_filtered = dh_df[~dh_df.data.isna()]
    dh_df_filtered.reset_index(drop=True, inplace=True)

    logging.info('обращение к DDT выполнено для {}/{} новых объявлений, найден fias для {}'.format(len(dh_df_filtered),
                                                                                                   len(dh_df),
                                                                                                   len(dh_df.query('not house_fias_id.isnull()'))))
    df_ddt_common = pd.concat([dh_df_filtered, dh_df_hist], ignore_index=True)
    df_ddt_common.reset_index(drop=True, inplace=True)
    df_ddt_common.to_csv(local_save_dir_data + f'/{source}_dadata_request.csv', encoding='cp1251')
    return dh_df_filtered, dh_df_to_tg

# получение данных район, house_id, jkh_id, dadata_houses_id

def get_districts_from_house(df, engine, logging, env_value=None):
    unique_ad_id = tuple(df.dropna(subset='ad_id').ad_id.unique())
    get_districts_query = \
    """select dh.house_fias_id, dh.ad_id, jh.district_id, h.id as house_id, jh.id as jkh_id, dh.id as dadata_houses_id
            from dadata_houses dh 
            left outer join jkh_houses jh 
            on (jh.house_fias_id = dh.house_fias_id and dh.ad_id is not null)
            left outer join houses h 
            on (h.jkh_id = jh.id and jh.id is not null and h.jkh_id is not null)
            where dh.ad_id in {}""".format(unique_ad_id if len(unique_ad_id) != 0 else '(0)')
    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine(logging, env_value)
            logging.info('подключение к базе восстановлено')
        except Exception as exc:
            logging.error('не удается подключиться к базе: {}'.format(traceback.format_exc()))
            return None, exc
    try:
        time.sleep(3)
        con_obj = engine.connect()
        districts_db = pd.read_sql(text(get_districts_query), con=con_obj)
        con_obj.close()
        exc = None
    except Exception as exc:
        logging.error('get districts from house connection failed')
        logging.error(traceback.format_exc())
        districts_db = None
    return districts_db, exc


def get_index_temp_jkh_houses(engine, logging):
    index_show_query = \
        f"""SHOW indexes FROM temp_jkh_houses"""
    try:
        time.sleep(3)
        con_obj = engine.connect()
        index_db = pd.read_sql(text(index_show_query), con=con_obj)
        con_obj.close()
        exc_code = None
    except Exception as exc:
        logging.error('get index connection failed')
        logging.error(traceback.format_exc())
        index_db = None
        exc_code = exc.code
    return index_db, exc_code

def load_df_into_sql_table_jkh(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=5000, method='multi', index=False)
    return

def create_temp_jkh_houses(engine, logging):
    create_table_query = \
        """CREATE TABLE `temp_jkh_houses` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `jkh_id` int(11) NOT NULL,
          `new_distr` int(11) DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;"""

    index_create_query = \
        f"""CREATE INDEX index_jkh_id_temp
        ON temp_jkh_houses (jkh_id);"""

    clear_temp_table_query = \
        """DELETE FROM temp_jkh_houses"""

    try:
        time.sleep(3)
        con_obj = engine.connect()
        con_obj.execute(text(create_table_query))
        con_obj.commit()
        con_obj.close()
        if 'index_jkh_id_temp' in get_index_temp_jkh_houses(engine, logging)[0].Key_name.to_list():
            logging.info('Временная таблица temp_jkh_houses уже была создана')
            pass
        else:
            con_obj = engine.connect()
            con_obj.execute(text(index_create_query))
            con_obj.commit()
            con_obj.close()
        logging.info('Временная таблица temp_jkh_houses создана')
        return None
    except:
        try:
            con_obj = engine.connect()
            con_obj.execute(text(clear_temp_table_query))
            con_obj.commit()
            con_obj.close()
            logging.info('Временная таблица temp_jkh_houses очищена')
            return None
        except:
            return True


def update_jkh_houses(engine, df, logging, env_value):
    clear_temp_table_query = \
        """DELETE FROM temp_jkh_houses"""
    try:
        engine.connect()
    except:
        server, engine = get_sql_engine(logging, env_value)

    try:
        # выгрузка данных в таблицу на сервере
        load_df_into_sql_table_jkh(df, 'temp_jkh_houses', engine)

        time.sleep(3)
        con_obj = engine.connect()
        common_ids = tuple(df.jkh_id)
        if len(common_ids) == 1:
            common_ids = (f'({common_ids[0]})')

        update_table_query = f"""update jkh_houses join temp_jkh_houses on jkh_houses.id=temp_jkh_houses.jkh_id
                                    set jkh_houses.district_id = temp_jkh_houses.new_distr,
                                        jkh_houses.geo_district = 0 
                                    WHERE jkh_houses.id in {common_ids if len(common_ids) != 0 else '(0)'} 
                                    and (isnull(jkh_houses.geo_district) = 1 or jkh_houses.geo_district = 1)"""

        con_obj.execute(text(update_table_query))
        con_obj.commit()
        con_obj.close()

        # очистка данных из temp_realty
        con_obj = engine.connect()
        con_obj.execute(text(clear_temp_table_query))
        con_obj.commit()
        con_obj.close()
        return None
    except Exception as exc:
        logging.error(traceback.format_exc())
        try:
            con_obj = engine.connect()
            con_obj.execute(text(clear_temp_table_query))
            con_obj.commit()
            con_obj.close()
            return True
        except:
            logging.error('Не удалось подключиться к БД')
            return True


def update_jkh_district(df_realty, df_districts, engine, logging, env_value):
    df_realty_check_upd = df_realty.copy()
    distr_to_update = pd.DataFrame(columns=['jkh_id', 'new_distr'])
    logging.info('начат процесс обновления jkh_distr')
    for i, val in df_realty.iterrows():
        try:
            flagNan = np.isnan(val.district_id)
        except:
            flagNan = val.district_id == None
        if flagNan or val.district_id == 'unknown_district' or val.district_id == None:
            try:
                df_realty.at[i, 'district_id'] = df_districts[df_districts.ad_id == val.ad_id].district_id.iloc[0]
            except:
                df_realty.at[i, 'district_id'] = None
        else:
            try:
                new_district = df_districts[df_districts.ad_id == val.ad_id].district_id.iloc[0]
                if val.district_id != new_district:
                    # print('distr', val.district_id, 'in current', val.ad_id, 'but', new_district, 'in new_df')
                    distr_to_update.loc[len(distr_to_update)] = [int(val.jkh_id), val.district_id]
            except:
                pass
    distr_to_update.drop_duplicates(inplace=True)
    # для статистики
    pre_distr_cnt = len(df_realty_check_upd[~df_realty_check_upd.district_id.isna()])
    post_distr_cnt = len(df_realty[~df_realty.district_id.isna()])
    distr_diff_cnt = post_distr_cnt - pre_distr_cnt
    logging.info('Добавлено районов к объявлениям realty: {} - было {} стало {}'.format(distr_diff_cnt,
                                                                                        pre_distr_cnt,
                                                                                        post_distr_cnt))
    logging.info('Есть несовпадения районов c jkh, обновление районов в jkh для {} записей'.format(len(distr_to_update)))

    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine(logging, env_value)
            logging.info('подключение к базе восстановлено')
        except Exception:
            logging.error('не удается подключиться к базе {}'.format(traceback.format_exc()))
            return None, False, False

    error_create_temp_jkh_houses = create_temp_jkh_houses(engine, logging)
    if error_create_temp_jkh_houses:
        error_create_temp_jkh_houses = True
        return None, error_create_temp_jkh_houses, False
    else:
        error_create_temp_jkh_houses = False

    error_updating_jkh_houses = update_jkh_houses(engine, distr_to_update, logging, env_value)
    if error_updating_jkh_houses:
        error_updating_jkh_houses = True
        return None, False, error_updating_jkh_houses
    else:
        logging.info('Обновление данных jkh_houses завершено')
        error_updating_jkh_houses = False

    return df_realty, error_create_temp_jkh_houses, error_updating_jkh_houses


def find_new_addr(cities, engine, logging, env_value):
    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine(logging, env_value)
            logging.info('подключение к базе восстановлено')
        except Exception as exc:
            logging.error('не удается подключиться к базе {}'.format(traceback.format_exc()))
            return None, exc
    addr_query = f"""SELECT city_id, addr, district_id, house_id, jkh_id, dadata_houses_id
                     FROM realty 
                     where city_id in {cities}"""
    try:
        con_obj = engine.connect()
        addr_db = pd.read_sql(text(addr_query), con=con_obj)
        con_obj.close()
        exc_str = None
    except Exception as exc:
        print('a')
        logging.error(traceback.format_exc())
        addr_db = None
        exc_str = exc
    return addr_db, exc_str

def find_empty_districts(house_ids, engine, logging, env_value):
    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine(logging, env_value)
            logging.info('подключение к базе восстановлено')
        except Exception as exc:
            logging.error('не удается подключиться к базе {}'.format(traceback.format_exc()))
            return None, exc
    find_distr_query = f"""SELECT h.district_id as district_id, h.id as house_id
                        from houses h
                        where h.id in {house_ids if len(house_ids) != 0 else '(0)'}"""
    try:
        con_obj = engine.connect()
        distr_db = pd.read_sql(text(find_distr_query), con=con_obj)
        con_obj.close()
        exc_str = None
    except Exception as exc:
        logging.error(traceback.format_exc())
        distr_db = None
        exc_str = exc
    return distr_db, exc_str


def count_jkh_addr(engine, logging, env_value):
    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine(logging, env_value)
            logging.info('подключение к базе восстановлено')
        except Exception as exc:
            logging.error('не удается подключиться к базе {}'.format(traceback.format_exc()))
            return None, exc

    
    count_jkh_addr_query = \
        """SELECT city_id, c.name, c.url_avito, count(*) as cnt_jkh
            from jkh_houses jh 
            left join city c 
            on c.id = city_id 
            group by city_id """
    try:
        con_obj = engine.connect()
        count_jkh_addr_db = pd.read_sql(text(count_jkh_addr_query), con=con_obj)
        count_jkh_addr_db['cnt'] = count_jkh_addr_db['cnt_jkh'] * 0.05 
        # 1 is no limit for dadata 0.05 is 5% of jkh addr count is limit for dadata request
        count_jkh_addr_db['cnt'] = count_jkh_addr_db['cnt'].astype('int')
        count_jkh_addr_db['cnt_ddt'] = 0
        count_jkh_addr_db['cnt_left_after_limit'] = 0
        con_obj.close()
        exc_code = None
    except Exception as exc:
        logging.error(traceback.format_exc())
        count_jkh_addr_db = None
        exc_code = exc
    return count_jkh_addr_db, exc_code

