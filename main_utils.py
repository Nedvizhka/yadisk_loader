import configparser
import io
import os
import shutil
import traceback
import warnings
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import yadisk
from sqlalchemy import create_engine, text, NullPool
from sshtunnel import SSHTunnelForwarder

warnings.filterwarnings("ignore")

dict_realty_cian_avito = {
    '1': ['Комната'],
    '2': ['Квартира-студия', 'Апартаменты-студия', 'Гостинка', 'Студия'],
    '3': ['1-к. квартира', '1-к. апартаменты', 'Своб. планировка', '1-комнатная', '1 комн',
          'Свободная планировка'],
    '4': ['2-к. квартира', '2-к. апартаменты', '2-комнатная', '2 комн'],
    '5': ['3-к. квартира', '3-к. апартаменты', '3-комнатная', '3 комн'],
    '6': ['4-к. квартира', '4-к. апартаменты', '4-комнатная', '4 комн'],
    '7': ['5-к. квартира', '5-к. апартаменты', '5-комнатная', '5 комн'],
    '8': ["6 комнат и более", '6 комн']
    # 9 аукцион, доля
}

list_realty_cols = ['source_id', 'ad_id', 'city_id', 'district_id', 'type_id', 'addr', 'square', 'floor',
                    'house_floors', 'link', 'date', 'status', 'version', 'offer_from', 'status_new']



def get_today_date():
    return datetime.today().strftime(format="%d/%m/%Y, %H:%M:%S")


def create_load_save_dir(source):
    try:
        Path.mkdir(Path.cwd() / f'saved_{source}_csv')
    except:
        pass
    save_dir = (Path.cwd() / f'saved_{source}_csv').as_posix()
    return save_dir


def get_config(get_only_start_time=False):
    config = configparser.ConfigParser()
    config.read('config.ini')

    ssh_host = config['database']['ssh_host_test']  # переключить на ssh_host_main для работы на прод сервере
    ssh_port = int(config['database']['ssh_port'])
    ssh_username = config['database']['ssh_username']
    ssh_password = config['database']['ssh_password']
    database_username = config['database']['database_username']
    database_password = config['database']['database_password']
    database_name = config['database']['database_name']
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

def show_error(list_errors):
    err_list = ['error_create_temp_realty',
                'error_getting_ad_id',
                'error_loading_into_realty',
                'error_updating_realty']
    try:
        array = np.array(list_errors)
        indices = np.where(array == True)[0]
        return [err_list[i] for i in indices]
    except:
        return 'ошибок нет'

def get_sql_engine(ssh_host, ssh_port, ssh_username, ssh_password, localhost,
                   localhost_port, database_username, database_password, database_name):
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

    return sql_server, sql_engine


def get_direct_link(yandex_api, sharing_link):
    pk_request = requests.get(yandex_api.format(sharing_link))
    # None если не удается преоброазовать ссылку
    return pk_request.json().get('href')


def get_saved_files_names(source):
    try:
        with open(f'uploaded_files_{source}.txt', 'r') as uploaded_txt:
            list_loaded_files = uploaded_txt.read()
        uploaded_txt.close()
        list_loaded_files = list_loaded_files.split('\n')[:-1]
    except:
        list_loaded_files = []
    return list_loaded_files


def write_saved_file_names(filename, source):
    with open(f'uploaded_files_{source}.txt', 'a+') as uploaded_txt:
        uploaded_txt.write(f"{filename}\n")
    uploaded_txt.close()


def download_local_yadisk_files(yandex_api_token, handled_files, save_dir):
    y = yadisk.YaDisk(token=yandex_api_token)
    if y.check_token():
        print('yandex token valid')
        disk_files_info = list(y.listdir("/cian"))
        disk_files_routes = [disk_files_info[i].path for i in range(len(disk_files_info))]
        saved_files = []
        cnt = 0
        for file in disk_files_routes:
            if Path(file).stem not in handled_files:
                y.download(file, save_dir + '/' + Path(file).name)
                saved_files.append(Path(file).name)
                cnt += 1
        print('Успешно загружено {} файлов с личного диска: {}'.format(cnt, saved_files))
        is_error = False
        return saved_files, is_error
    else:
        print('yandex token invalid')
        print(
            'перейдите по ссылке: https://oauth.yandex.ru/authorize?response_type=token&client_id=<> вставив свой айди приложения')
        print('id приложения можно получить в: https://oauth.yandex.ru/ - пользователь yanedvizhkatop')
        print('мануал: https://ramziv.com/article/8')
        saved_files = []
        is_error = False
        return saved_files, is_error


def download_yadisk_files(yandex_api, sharing_link, handled_files, save_dir):
    direct_link = get_direct_link(yandex_api, sharing_link)
    try:
        saved_files = []
        if direct_link:
            download = requests.get(direct_link)
            zips = zipfile.ZipFile(io.BytesIO(download.content))
            cnt = 0
            for member in zips.namelist():
                filename = os.path.basename(member)
                if not filename or Path(filename).stem in handled_files or 'циан' in filename:
                    continue
                src = zips.open(member)
                target = open(os.path.join(save_dir, filename), 'wb')
                with src, target:
                    shutil.copyfileobj(src, target)
                    cnt += 1
                    saved_files.append(filename)
                target.close()
            print('Успешно загружено {} файлов с диска {}: {}'.format(cnt, sharing_link, saved_files))
            is_error = False
            return saved_files, is_error
        else:
            print('Failed to download files from "{}"'.format(sharing_link))
            saved_files = []
            is_error = True
            return saved_files, is_error
    except Exception:
        print(traceback.format_exc())
        saved_files = []
        is_error = True
        return saved_files, is_error


def get_local_files(save_dir):
    p = Path(save_dir).glob('**/*')
    saved_files_list = [x.as_posix() for x in p if x.is_file()]
    return saved_files_list

def load_df_into_sql_table(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=7000, method='multi', index=False)
    return

def get_index_temp(engine):
    index_show_query = \
        f"""SHOW indexes FROM temp_realty"""
    try:
        con_obj = engine.connect()
        index_db = pd.read_sql(text(index_show_query), con=con_obj)
        con_obj.close()
        exc_code = None
    except Exception as exc:
        print(traceback.format_exc())
        index_db = None
        exc_code = exc.code
    return index_db, exc_code

def create_temp_realty(engine):
    create_table_query = \
        """CREATE TABLE `temp_realty` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `source_id` int(11) DEFAULT NULL,
          `ad_id` BIGINT DEFAULT NULL,
          `city_id` int(11) DEFAULT NULL,
          `district_id` int(11) DEFAULT NULL,
          `type_id` int(11) DEFAULT NULL,
          `addr` varchar(255) DEFAULT NULL,
          `square` float DEFAULT '0',
          `floor` int(11) DEFAULT NULL,
          `house_floors` int(11) DEFAULT NULL,
          `link` varchar(512) DEFAULT NULL,
          `date` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
          `status` int(11) DEFAULT '0',
          `version` int(11) DEFAULT '0',
          `offer_from` varchar(255) DEFAULT NULL,
          `status_new` int(1) NOT NULL DEFAULT '0',
          `house_id` int(11) DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 AVG_ROW_LENGTH=2048 ROW_FORMAT=DYNAMIC;"""

    index_create_query = \
        f"""CREATE INDEX index_ad_id_temp
        ON temp_realty (ad_id);"""

    clear_temp_table_query = \
        """DELETE FROM temp_realty"""

    try:
        con_obj = engine.connect()
        con_obj.execute(text(create_table_query))
        if 'index_ad_id_temp' in get_index_temp(engine)[0].Key_name.to_list():
            pass
        else:
            con_obj.execute(text(index_create_query))
        con_obj.commit()
        con_obj.close()
        print('Временная таблица создана')
        return None
    except:
        try:
            con_obj = engine.connect()
            con_obj.execute(text(clear_temp_table_query))
            con_obj.commit()
            con_obj.close()
            print('Временная таблица очищена')
            return None
        except:
            return True

def get_exist_ad_id(engine, source):
    source_id = 2 if source == 'cian' else 3
    ad_id_query = f'SELECT ad_id FROM realty where source_id = {source_id}'
    try:
        con_obj = engine.connect()
        ad_id_db = pd.read_sql(text(ad_id_query), con=con_obj)
        con_obj.close()
        exc_code = None
    except Exception as exc:
        print(traceback.format_exc())
        ad_id_db = None
        exc_code = exc.code
    return ad_id_db, exc_code

def update_realty(engine, df):
    clear_temp_table_query = \
        """DELETE FROM temp_realty"""
    try:
        # выгрузка данных в таблицу на сервере
        load_df_into_sql_table(df, 'temp_realty', engine)

        con_obj = engine.connect()
        common_ids = tuple(df.ad_id)

        update_table_query = f"""update realty join temp_realty on realty.ad_id=temp_realty.ad_id
                                            set realty.source_id = temp_realty.source_id,
                                                realty.city_id = temp_realty.city_id,
                                                realty.district_id = temp_realty.district_id,
                                                realty.type_id = temp_realty.type_id,
                                                realty.addr = temp_realty.addr,
                                                realty.square = temp_realty.square,
                                                realty.floor = temp_realty.floor,
                                                realty.house_floors = temp_realty.house_floors,
                                                realty.link = temp_realty.link,
                                                realty.date = temp_realty.date,
                                                realty.version = temp_realty.version,
                                                realty.offer_from = temp_realty.offer_from,
                                                realty.status_new = temp_realty.status_new,
                                                realty.house_id = temp_realty.house_id
                                             WHERE realty.ad_id in {common_ids}"""

        con_obj.execute(text(update_table_query))
        con_obj.commit()
        con_obj.close()

        # очистка данных из temp_realty
        con_obj = engine.connect()
        con_obj.execute(text(clear_temp_table_query))
        con_obj.commit()
        con_obj.close()
        return None
    except:
        try:
            con_obj = engine.connect()
            con_obj.execute(text(clear_temp_table_query))
            con_obj.commit()
            con_obj.close()
            return True
        except:
            print('Не удалось подключиться к БД')
            return True


def load_and_update_realty_db(engine, df, source):
    # создание временной таблицы для обновления данных
    error_create_temp_realty = create_temp_realty(engine)
    if error_create_temp_realty:
        error_create_temp_realty = True
        return error_create_temp_realty, False, False, False
    else:
        error_create_temp_realty = False

    # разбивка полученных данных на новые и существующие по ad_id
    exist_ad_id, error_getting_ad_id = get_exist_ad_id(engine, source)[0].ad_id.to_list()
    if error_getting_ad_id:
        error_getting_ad_id = True
        return False, error_getting_ad_id, False, False
    else:
        error_getting_ad_id = False
        print('Загрузка новых объявлений в таблицу')
    df_realty_exist = df[df.ad_id.isin(exist_ad_id)][list_realty_cols]
    df_realty_new = df[~df.ad_id.isin(exist_ad_id)][list_realty_cols]

    # выгрузка данных в таблицу на сервере
    try:
        load_df_into_sql_table(df_realty_new, 'realty', engine)
        error_loading_into_realty = False
    except:
        error_loading_into_realty = True
        return False, False, error_loading_into_realty, False
    print('новые объявления добавлены, переход к обновлению существующих')

    # обновление данных
    if len(df_realty_exist) != 0:
        error_updating_realty = update_realty(engine, df_realty_exist)
        if error_updating_realty:
            error_updating_realty = True
            return False, False, False, error_updating_realty
        else:
            error_updating_realty = False
            print('Обновление данных завершено')
    else:
        error_updating_realty = False
        print('не было обнаружено пересечений в данных, переход к добавлению цен в prices')
    return error_create_temp_realty, error_getting_ad_id, error_loading_into_realty, error_updating_realty



def get_tables_info(engine):
    cities_query = 'SELECT * FROM city'
    source_query = 'SELECT id, name FROM sources'
    try:
        con_obj = engine.connect()
        city_db = pd.read_sql(text(cities_query), con=con_obj)
        source_db = pd.read_sql(text(source_query), con=con_obj)
        con_obj.close()
        exc_code = None
    except Exception as exc:
        print(traceback.format_exc())
        city_db = None
        source_db = None
        exc_code = exc.code
    return city_db, source_db, exc_code


def get_version_db(engine, today_date, source_id):
    last_version = f"SELECT Max(version) FROM realty WHERE date < '{today_date}' AND source_id = {source_id}"
    try:
        con_obj = engine.connect()
        last_version_db = pd.read_sql(text(last_version), con=con_obj)
        con_obj.close()
        exc_code = None
    except Exception as exc:
        print(traceback.format_exc())
        last_version_db = None
        exc_code = exc.code
    return last_version_db, exc_code


def source_id_from_link(link_value, source_dict, source_name):
    try:
        if source_name == 'avito':
            return list(source_dict.keys())[list(source_dict.values()).index(link_value.split('.')[1])]
        else:
            return list(source_dict.keys())[list(source_dict.values()).index(link_value)]
    except:
        return None


def city_id_from_link(city_value, city_dict):
    try:
        return int(list(city_dict.keys())[list(city_dict.values()).index(city_value)])
    except Exception as e:
        return None


def type_id_from_room(room_value, type_dict):
    try:
        for ind, values in type_dict.items():
            if room_value in values:
                tid = ind
                return tid
        if any(elem in room_value for elem in ['Доля', 'Аукцион']):
            # tid = 9 # аукцион
            return None
        elif all(elem in room_value for elem in ['Комната ', ' в ', '-к']):
            tid = 1  # комната в квартире
            return tid
        elif any(elem in room_value for elem in
                 ['6-к.', '7-к.', '8-к.', '9-к.', '10 и более-к.', '10 комнат и больше']):
            tid = 8  # 6 комнат и более
            return tid
        else:
            return None  # none
    except:
        return None


# создание поля addr из данных avito
# поле должно совпасть с форматом cian
# формат: Томская область; Томск; р-н Кировский; мкр. Преображенский; улица Дзержинского; 36
def avito_addr_from_row(row, city_df):
    # добавление города для совпадения форматов
    try:
        addr = city_df[city_df.url_avito == row['city']]['name'].values[0]
        addr += '; '
    except:
        addr = ''
    # добавляем район, в авито он может быть None
    try:
        if len(row['Район']) == 0:
            pass
        else:
            addr += row['Район']
            addr += '; '
    except:
        pass
    # добавляем улицу, дом, пр. в авито он может быть None
    try:
        for value in row['Улица'].split(', '):
            addr += value
            addr += '; '
    except:
        pass
    # drop duplicates
    addr = addr[:-2]
    if len(addr.split('; ')) != len(set(addr.split('; '))):
        addr = '; '.join(list(dict.fromkeys(addr.split('; '))))
    return addr


def get_districts(engine):
    districts_query = f"SELECT DISTINCT id, city_id, name FROM districts"
    try:
        con_obj = engine.connect()
        districts_query_db = pd.read_sql(text(districts_query), con=con_obj)
        con_obj.close()
    except Exception as exc:
        print(exc)
        districts_query_db = None
    return districts_query_db


def district_from_rn_mkrn(realty_row, all_districts, sql_engine):
    # для записи новых районов создадим датафрейм и получим последний id района
    add_districts_df = pd.DataFrame(columns=['city_id', 'name'])
    current_max_id = all_districts.id.max()
    # для работы функции создадим объекты текущего р-н и мкр
    current_rn = None
    current_mkrn = None
    if not np.isnan(realty_row['city_id']):
        for addr_part in realty_row.addr.split(';')[:4]:
            if 'мкр.' in addr_part:
                current_mkrn = addr_part.replace(' мкр. ', '')
                continue
            elif 'р-н' in addr_part:
                current_rn = addr_part.replace(' р-н ', '')
                continue
            else:
                continue
        if current_rn == None and current_mkrn == None:
            return None
        else:
            district_name = current_mkrn if current_mkrn != None else current_rn
            district_intersection = all_districts[
                (all_districts.city_id == realty_row.city_id) & (all_districts.name == district_name)]
            if len(district_intersection) == 0:
                # обновление данных из districts на случай если дистрикт добавлялся во время исполнения скрипта
                all_districts_upd = get_districts(sql_engine)
                district_intersection_upd = all_districts_upd[
                    (all_districts_upd.city_id == realty_row.city_id) & (all_districts_upd.name == district_name)]
                current_max_id = int(all_districts_upd.id.max())
                # если в обновленном districts нет данных о районе - добавляем
                if len(district_intersection_upd) == 0:
                    add_districts_df.loc[len(add_districts_df)] = [realty_row.city_id, district_name]
                    add_districts_df.to_sql(name='districts', con=sql_engine, if_exists='append', chunksize=7000,
                                            method='multi', index=False)
                    print('добавлен новый район "{}" в districts функцией district_from_rn_mkrn'.format(district_name))
                    return current_max_id + 1
                # если данные о районе есть, возвращаем id
                else:
                    return district_intersection_upd.iloc[0, 0]
            else:
                return district_intersection.iloc[0, 0]
    else:
        return None


def square_from_ploshad(square_value):
    try:
        return float(square_value[:-3].replace(',', '.'))
    except:
        return None


def floor_floors_from_etazh(etazh_value):
    try:
        return [etazh_value.split('/')[0], etazh_value.split('/')[1].replace(' эт.', '')]
    except:
        return [None, None]


def get_date_from_name(fname):
    try:
        return datetime.strptime(fname.replace("(без дублей)", "").replace("циан", "").replace(" ", ""), "%d-%m-%y")
    except:
        return datetime.strptime(Path(fname).stem.replace("(без дублей)", "").replace("циан", "").replace(" ", ""),
                                 "%d-%m-%y")


def create_realty(df, fname, sql_engine, source, dict_realty_type=dict_realty_cian_avito):
    if source == 'avito':
        try:
            # сбор данных из таблиц в бд
            city_df, source_df, exc_code = get_tables_info(sql_engine)

            # удаление корявых данных, дубликатов
            index_to_drop = df[
                (df['Площадь'].isna()) | (df['Заголовок'] == df['Комнат']) | (df['Цена'].isna())].index.tolist()
            if len(index_to_drop) != 0:
                df.drop(index_to_drop, inplace=True)
            df.drop_duplicates(subset=['Ссылка'], keep='last', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # city
            df['city'] = df['Ссылка'].apply(lambda x: x.split('/')[3])

            # source_id
            source_df_dict = source_df.set_index('id').to_dict()['name']
            df['source_id'] = df['Ссылка'].apply(lambda x: source_id_from_link(x, source_df_dict, 'avito'))

            # ad_id
            df['ad_id'] = df['Ссылка'].apply(lambda x: x.split('_')[-1])

            # city_id
            city_df_dict = city_df.set_index('id').to_dict()['url_avito']
            df['city_id'] = df['city'].apply(lambda x: city_id_from_link(x, city_df_dict))
            # df.dropna(subset=['city_id'], inplace=True)
            # df.reset_index(drop=True, inplace=True)
            # df.city_id = df.city_id.astype(int)

            # district_id
            df['district_id'] = None

            # type_id

            df['type_id'] = df['Комнат'].apply(lambda x: type_id_from_room(x, dict_realty_type))
            # удалить записи аукционов и других объяв где не опредлен type_id, преобразовать цены в int
            df.dropna(subset=['type_id'], inplace=True)
            df.reset_index(drop=True, inplace=True)
            df['Цена'] = df['Цена'].astype(float)

            # addr
            df['addr'] = df.apply(lambda row: avito_addr_from_row(row, city_df), axis=1)

            # square
            df['square'] = df['Площадь'].apply(lambda x: square_from_ploshad(x))

            # floor
            df['floor'], df['house_floors'] = zip(*df['Этаж'].apply(floor_floors_from_etazh))

            # link
            df['link'] = df['Ссылка']

            # date
            file_date = get_date_from_name(fname)
            df['date'] = file_date

            # status
            df['status'] = None

            # version
            current_source = df.source_id.unique()[0]
            current_date = datetime.now().date()
            last_version = get_version_db(sql_engine, current_date, current_source)[0].iloc[0, 0]
            if last_version == None:
                current_version = 1
            else:
                current_version = last_version + 1

            df['version'] = current_version

            # offer_from
            df['offer_from'] = df['Тип продавца']

            # status_new
            df['status_new'] = 1

            # delete rows where one of important values isna
            df_check = df[['source_id', 'ad_id', 'type_id', 'addr', 'link', 'date', 'version']]
            values_na_ind = df_check[df_check.isna().any(axis=1)].index
            if len(values_na_ind) > 0:
                df = df.drop(values_na_ind)

            realty_avito_to_return = df[['source_id', 'ad_id', 'city_id', 'district_id', 'type_id', 'addr',
                                         'square', 'floor', 'house_floors', 'link', 'date', 'status', 'version',
                                         'offer_from', 'status_new', 'Цена', 'Цена за м2']]
        except Exception as ex:
            print(traceback.format_exc())
            realty_avito_to_return = pd.DataFrame()
            file_date = None
        return realty_avito_to_return, file_date
    else:  # cian
        try:
            # new table
            cian_realty = pd.DataFrame()

            # сбор данных из таблиц в бд
            city_df, source_df, exc_code = get_tables_info(sql_engine)

            # удаление дубликатов
            df.drop_duplicates(subset=['link'], keep='last', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # source_id
            source_df_dict = source_df.set_index('id').to_dict()['name']
            cian_realty['source_id'] = df['source'].apply(lambda x: source_id_from_link(x, source_df_dict, 'cian'))

            # ad_id
            cian_realty['ad_id'] = df['ad_id']

            # city_id
            city_df_dict = city_df.set_index('id').to_dict()['name']
            cian_realty['city_id'] = df['city'].apply(lambda x: city_id_from_link(x, city_df_dict))

            cian_realty['city_id'] = df['city'].apply(lambda x: city_id_from_link(x, city_df_dict))
            # удалить записи где city_id пустой (записи городов, которые не присутствуют в списке городов для парсинга)
            # ind_to_drop = cian_realty[(cian_realty['city_id'].isna())].index.tolist()
            # cian_realty.drop(ind_to_drop, inplace=True)
            # df.drop(ind_to_drop, inplace=True)
            # cian_realty.city_id = cian_realty.city_id.astype(int)

            # type_id

            cian_realty['type_id'] = df['type'].apply(lambda x: type_id_from_room(x, dict_realty_type))
            # удалить записи аукционов и других объяв где не опредлен type_id
            ind_to_drop = cian_realty[(cian_realty['type_id'].isna())].index.tolist()
            cian_realty.drop(ind_to_drop, inplace=True)
            df.drop(ind_to_drop, inplace=True)
            cian_realty.type_id = cian_realty.type_id.astype(int)

            # addr
            cian_realty['addr'] = df['addr']

            # district_id после addr потому что в нем используется адрес
            all_districts = get_districts(sql_engine)
            cian_realty['district_id'] = cian_realty.apply(
                lambda row: district_from_rn_mkrn(row, all_districts, sql_engine), axis=1)

            # square
            cian_realty['square'] = df['square']

            # floor
            cian_realty['floor'], cian_realty['house_floors'] = df['floor'], df['house_floors']

            # link
            cian_realty['link'] = df['link']

            # date
            file_date = get_date_from_name(fname)
            cian_realty['date'] = file_date

            # status
            cian_realty['status'] = None

            # version
            current_source = cian_realty.source_id.unique()[0]
            current_date = datetime.now().date()
            last_version = get_version_db(sql_engine, current_date, current_source)[0].iloc[0, 0]
            if last_version == None:
                current_version = 1
            else:
                current_version = last_version + 1

            cian_realty['version'] = current_version

            # offer_from
            cian_realty['offer_from'] = df['offer_from']

            # status_new
            cian_realty['status_new'] = 1

            # add fields of price and price_sqrm
            cian_realty['price'], cian_realty['price_sqrm'] = df['price'], df['price_sqrm']

            # delete rows where one of important values isna
            cian_realty_check = cian_realty[['source_id', 'ad_id', 'type_id', 'addr', 'link', 'date', 'version']]
            values_na_ind = cian_realty_check[cian_realty_check.isna().any(axis=1)].index
            if len(values_na_ind) > 0:
                cian_realty = cian_realty.drop(values_na_ind)

            cian_realty_to_return = cian_realty[['source_id', 'ad_id', 'city_id', 'district_id', 'type_id',
                                                 'addr', 'square', 'floor','house_floors', 'link', 'date',
                                                 'status', 'version', 'offer_from', 'status_new', 'price', 'price_sqrm']]
        except Exception as ex:
            print(traceback.format_exc())
            cian_realty_to_return = pd.DataFrame()
            file_date = None
        return cian_realty_to_return, file_date


def process_realty(local_dir, file_to_process, sql_engine, source):
    saved_files = get_local_files(local_dir)
    realty_df = pd.DataFrame()
    file_date = None
    for f_ind in range(len(saved_files)):
        if Path(saved_files[f_ind]).name == file_to_process:
            try:
                if source == 'avito':
                    saved_file = pd.read_csv(saved_files[f_ind], delimiter=';', encoding='utf8')
                    realty_df, file_date = create_realty(saved_file, Path(saved_files[f_ind]).stem,
                                                         sql_engine, 'avito')
                elif source == 'cian':
                    saved_file = pd.read_csv(saved_files[f_ind], delimiter=',', encoding='cp1251')
                    realty_df, file_date = create_realty(saved_file, Path(saved_files[f_ind]).stem,
                                                         sql_engine, 'cian')
            except Exception as ex:
                print(traceback.format_exc())
                print('Не удалось прочитать файл из {}: '.format(source), Path(saved_files[f_ind]))
        else:
            pass
    if len(realty_df) != 0:
        print('Успешно обработан файл {} для realty'.format(file_to_process))
        error_status = False
        return realty_df, file_date, error_status
    else:
        error_status = True
        print('Файл {} не удалось обнаружить для realty'.format(file_to_process))
        return realty_df, file_date, error_status


def get_realty_ids(engine, today_date, source_id):
    id_query = f"SELECT link, id AS 'realty_id' FROM realty WHERE date = '{today_date}' AND source_id = {source_id}"
    try:
        con_obj = engine.connect()
        id_db = pd.read_sql(text(id_query), con=con_obj)
        con_obj.close()
        exc_code = None
    except Exception as exc:
        print(traceback.format_exc())
        id_db = None
        exc_code = exc.code
    return id_db, exc_code


def create_prices(df, filename, sql_engine, source):
    try:
        if source == 'avito':
            # create df with link and realty_id
            current_source_id = df.source_id.unique()[0]
            prices_df = get_realty_ids(sql_engine, get_date_from_name(filename), current_source_id)[0]

            # price
            prices_df = pd.merge(prices_df, df[['link', 'Цена']], on='link', how='left')\
                .rename(columns={'Цена': 'price'})

            # price_sqrm
            prices_df = pd.merge(prices_df, df[['link', 'Цена за м2']], on='link', how='left')\
                .rename(columns={'Цена за м2': 'price_sqrm'})

            # date
            file_date = get_date_from_name(filename)
            prices_df['date'] = file_date

            # filter df
            prices_df = prices_df[['realty_id', 'price', 'price_sqrm', 'date']]

        else:  # source == 'cian':
            # create df with link and realty_id
            current_source_id = df.source_id.unique()[0]
            prices_df = get_realty_ids(sql_engine, get_date_from_name(filename), current_source_id)[0]

            # price
            prices_df = pd.merge(prices_df, df[['link', 'price']], on='link', how='left')

            # price_sqrm
            prices_df = pd.merge(prices_df, df[['link', 'price_sqrm']], on='link', how='left')

            # date
            file_date = get_date_from_name(filename)
            prices_df['date'] = file_date

            # filter df
            prices_df = prices_df[['realty_id', 'price', 'price_sqrm', 'date']]

    except Exception as ex:
        prices_df = pd.DataFrame()
        print(traceback.format_exc())
        print('Не удалось обработать для prices файл', filename)

    if len(prices_df) != 0:
        print('Успешно обработан файл {} для prices'.format(filename))
        error_status = False
        return prices_df, error_status
    else:
        error_status = True
        print('Файл {} не удалось обработать для prices'.format(filename))
        return prices_df, error_status


def delete_files(pth):
    pth = Path(pth)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            shutil.rmtree(child)


def close_sql_connection(server, engine):
    server.stop()
    engine.dispose()
