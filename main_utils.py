import requests
import zipfile
import io
import os
import pandas as pd
import shutil

from pathlib import Path
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, text, NullPool

import warnings

warnings.filterwarnings("ignore")


def get_today_date():
    return datetime.today().strftime(format="%d/%m/%Y, %H:%M:%S")


def create_load_save_dir():
    try:
        Path.mkdir(Path.cwd() / 'saved_csv')
    except:
        pass
    save_dir = (Path.cwd() / 'saved_csv').as_posix()
    return save_dir


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


def get_db_saved_files(engine):
    filenames_query = """SELECT DISTINCT file_name FROM test_avito_parsed"""
    try:
        con_obj = engine.connect()
        filenames_list = pd.read_sql(sql=text(filenames_query), con=con_obj)
        filenames_list = filenames_list.file_name.to_list()
        con_obj.close()
        exc_code = None
    except Exception as exc:
        if exc.code == 'f405':
            print(exc)
            print('\nТаблица не существует и это нормально, она создастся при записи csv-файла!')
            filenames_list = []
            exc_code = None
        else:
            print(exc)
            filenames_list = []
            exc_code = exc.code
    return filenames_list, exc_code


def get_direct_link(yandex_api, sharing_link):
    pk_request = requests.get(yandex_api.format(sharing_link))
    # None если не удается преоброазовать ссылку
    return pk_request.json().get('href')


def download_yadisk_files(yandex_api, sharing_link, list_file_exist, save_dir):
    direct_link = get_direct_link(yandex_api, sharing_link)
    if direct_link:
        download = requests.get(direct_link)
        zips = zipfile.ZipFile(io.BytesIO(download.content))
        cnt = 0
        for member in zips.namelist():
            filename = os.path.basename(member)
            if not filename or Path(filename).stem in list_file_exist:
                continue
            src = zips.open(member)
            target = open(os.path.join(save_dir, filename), 'wb')
            with src, target:
                shutil.copyfileobj(src, target)
                cnt += 1
            target.close()
        print('Succesfully downloaded {} files from "{}"'.format(cnt, sharing_link))
        return None
    else:
        print('Failed to download files from "{}"'.format(sharing_link))
        return True

def get_local_files(save_dir):
    p = Path(save_dir).glob('**/*')
    saved_files_list = [x.as_posix() for x in p if x.is_file()]
    return saved_files_list


def create_csv(save_directory):
    saved_files = get_local_files(save_directory)
    cnt_files = 0
    for f_ind in range(len(saved_files)):
        try:
            saved_files_new = pd.read_csv(saved_files[f_ind], delimiter=';', encoding='utf8')
            saved_files_new['file_name'] = Path(saved_files[f_ind]).stem
            parsed_csv = parsed_csv.append(saved_files_new)
        except:
            parsed_csv = pd.read_csv(saved_files[f_ind], delimiter=';', encoding='utf8')
            parsed_csv['file_name'] = Path(saved_files[f_ind]).stem
        cnt_files += 1

    parsed_csv.reset_index(drop=True, inplace=True)
    print('Succesfully opened {} files'.format(cnt_files))

    return parsed_csv


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
