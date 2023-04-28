import time
import pandas as pd
import numpy as np

from dadata import Dadata
from sqlalchemy import text
from tqdm import tqdm


from logging_utils import *
from sql_config_utils import *

# запрос к дадата

def create_ddt_save_dir(source):
    try:
        Path.mkdir(Path.cwd() / f'saved_{source}_csv')
    except:
        pass
    save_dir = (Path.cwd() / f'saved_{source}_csv').as_posix()
    return save_dir

def filter_addr_for_dadata(addrString, source):
    addrList = addrString.split('; ')
    min_len = 3 if source == 'avito' else 4
    flag_less = len(addrList) < min_len
    flag_notHouse = len(addrList[-1]) > 10
    flag_notHouseNum = not any(del_el in addrList[-1] for del_el in [str(n) for n in range(0,10)])
    if flag_less or flag_notHouse or flag_notHouseNum:
        return None
    else:
        if source == 'cian':
            addrList = list(filter(lambda c: 'мкр' not in c, addrList))
        else:
            addrList = list(filter(lambda c: not any(del_el in c for del_el in [str(i) for i in range(0,10)] if 'мкр' in c), addrList))
            # addrList = list(filter(lambda c: 'мкр' in c, addrList))
        return '; '.join(addrList)

def dadata_request(df, file_date, source):
    st_time = datetime.now()
    # dadata_credentials
    token = "f288b25edb6d05b5ceb4d957376104a181c4adee"
    secret = "9d337ae6b9901a6708802eaca6d7055ce2c64772"
    dadata = Dadata(token, secret)
    # create df for dadata_house_loading
    # ТЕСТОВЫЙ ЗАПУСК УДАЛИТЬ
    dh_df = pd.DataFrame(columns=['house_fias_id', 'data', 'geo_lat', 'geo_lon', 'street', 'house', 'qc', 'result', 'qc_geo', 'ad_id'])
    # try:
    #     dh_df = pd.read_csv(Path.cwd() / 'saved_data_csv/cian_dadata_request_27_04_2023.csv', encoding='cp1251', index_col=0)
    #     logging.info('удалось загрузить исторические данные dadata')
    # except:
    #     logging.error('НЕ удалось загрузить исторические данные dadata')
    #     time.sleep(10)
    # exist_ddt_ad_id = dh_df.ad_id.astype(int).to_list()
    
    # count bad addr and missed queries
    bad_addr = 0
    # to upload dadata result right away when query crashes
    uploading_cnt = 0
    # create dir for bad_addr to store
    local_save_dir_data = create_ddt_save_dir('data')
    bad_addr_txt_root = (Path(local_save_dir_data)/f'{source}_bad_addr_{file_date[:10].replace("-", "_")}.txt').as_posix()
    bad_req_txt_root = (Path(local_save_dir_data)/f'{source}_bad_request_{file_date[:10].replace("-", "_")}.txt').as_posix()
    # tqdm to logger for dadata request
    logger = logging.getLogger()
    tqdm_out = TqdmToLogger(logger, level=logging.INFO)
    # dadata request
    time.sleep(10) 
    for i, row in tqdm(df.iterrows(), total=df.shape[0], file=tqdm_out, mininterval=10):
        try:
            addr = filter_addr_for_dadata(row.addr, source)
            if addr:
                d_res = dadata.clean("address", addr)
                dh_df.loc[len(dh_df.index)] = [d_res['house_fias_id'], str(d_res), d_res['geo_lat'], d_res['geo_lon'], d_res['street'],
                                               d_res['house'], d_res['qc'], d_res['result'], d_res['qc_geo'], row.ad_id]
            else:
                # write ad_id and addr to txt
                with open(bad_addr_txt_root, 'a') as wr:
                    wr.writelines(f"{row.ad_id}: {row.addr}" + ',\n')
                wr.close()
                bad_addr += 1
        except Exception as exc:
            logging.error('{}, try reconnect'.format(traceback.format_exc()))
            if uploading_cnt == 0:
                dh_df.to_csv(local_save_dir_data + f'/{source}_dadata_request_{file_date[:10].replace("-", "_")}.csv',
                             encoding='cp1251')
                uploading_cnt += 1
            # write ad_id and addr to txt
            with open(bad_req_txt_root, 'a') as wr:
                wr.writelines(f"{row.ad_id}: {row.addr}" + ',\n')
            wr.close()
            bad_addr += 1
            time.sleep(3)
            dadata.close()
            token = "f288b25edb6d05b5ceb4d957376104a181c4adee"
            secret = "9d337ae6b9901a6708802eaca6d7055ce2c64772"
            dadata = Dadata(token, secret)
    logging.info('обращение к дадата по {}/{} записям из {} заняло {}'.format(len(df) - bad_addr, len(df), source, datetime.now() - st_time))
    dh_df.to_csv(local_save_dir_data+f'/{source}_dadata_request_{file_date[:10].replace("-", "_")}.csv', encoding='cp1251')
    return dh_df

# получение данных район, house_id, jkh_id, dadata_houses_id

def get_districts_from_house(df, engine):
    unique_fias = tuple(df.dropna(subset='house_fias_id').house_fias_id.unique())
    get_districts_query = \
        """select dh.house_fias_id, dh.ad_id, jh.district_id, h.id as house_id, jh.id as jkh_id, dh.id as dadata_houses_id
            from dadata_houses dh 
            left join jkh_houses jh on jh.house_fias_id = dh.house_fias_id 
            left join houses h on h.jkh_id = jh.id
            where h.jkh_id != 0
            and jh.district_id is not null
            and dh.ad_id is not null
            and dh.house_fias_id in {}""".format(unique_fias)

    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine()
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


def get_index_temp_jkh_houses(engine):
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

def create_temp_jkh_houses(engine):
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
        if 'index_jkh_id_temp' in get_index_temp_jkh_houses(engine)[0].Key_name.to_list():
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


def update_jkh_houses(engine, df):
    clear_temp_table_query = \
        """DELETE FROM temp_jkh_houses"""
    try:
        engine.connect()
    except:
        server, engine = get_sql_engine()

    try:
        # выгрузка данных в таблицу на сервере
        load_df_into_sql_table_jkh(df, 'temp_jkh_houses', engine)

        time.sleep(3)
        con_obj = engine.connect()
        common_ids = tuple(df.jkh_id)

        update_table_query = f"""update jkh_houses join temp_jkh_houses on jkh_houses.id=temp_jkh_houses.jkh_id
                                            set jkh_houses.district_id = temp_jkh_houses.new_distr,
                                                jkh_houses.geo_district = 0 
                                            WHERE jkh_houses.id in {common_ids}"""

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

def update_jkh_district(df_realty, df_districts, engine):
    distr_to_update = pd.DataFrame(columns=['jkh_id', 'new_distr'])
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
                if val.district_id != str(new_district):
                    # print('distr', val.district_id, 'in current', val.ad_id, 'but', new_district, 'in new_df')
                    distr_to_update.loc[len(distr_to_update)] = [int(val.jkh_id), val.district_id]
            except:
                pass
    logging.info('Получены несовпадения районов c jkh, обновление районов в jkh_houses для {} записей'.format(len(distr_to_update)))

    try:
        con_obj = engine.connect()
        con_obj.close()
    except:
        try:
            server, engine = get_sql_engine()
            logging.info('подключение к базе восстановлено')
        except Exception as exc:
            logging.error('не удается подключиться к базе {}'.format(traceback.format_exc()))
            return False, False

    error_create_temp_jkh_houses = create_temp_jkh_houses(engine)
    if error_create_temp_jkh_houses:
        error_create_temp_jkh_houses = True
        return error_create_temp_jkh_houses, False
    else:
        error_create_temp_jkh_houses = False

    error_updating_jkh_houses = update_jkh_houses(engine, distr_to_update)
    if error_updating_jkh_houses:
        error_updating_jkh_houses = True
        return False, error_updating_jkh_houses
    else:
        logging.info('Обновление данных jkh_houses завершено')
        error_updating_jkh_houses = False

    return error_create_temp_jkh_houses, error_updating_jkh_houses

