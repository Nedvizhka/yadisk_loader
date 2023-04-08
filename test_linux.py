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

files_avito_current = get_saved_files_names('avito')

print(files_avito_current)

write_saved_file_names('24-03-23 (без дублей)', 'avito')

files_avito_current = get_saved_files_names('avito')

print(files_avito_current)

from main_utils import *

ssh_host, ssh_port, ssh_username, ssh_password, \
database_username, database_password, database_name, \
localhost, localhost_port, table_name, ya_token, ya_api, ya_link = get_config()

sql_server, sql_engine = get_sql_engine(ssh_host, ssh_port, ssh_username, ssh_password, localhost,
                                        localhost_port, database_username, database_password, database_name)

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

exist_ad_id, error_getting_ad_id = get_exist_ad_id(sql_engine, 'cian')
exist_ad_id = exist_ad_id.ad_id.to_list()
print(len(exist_ad_id))

exist_ad_id, error_getting_ad_id = get_exist_ad_id(sql_engine, 'avito')
exist_ad_id = exist_ad_id.ad_id.to_list()
print(len(exist_ad_id))

if 'index_ad_id_temp' in get_index_temp(sql_engine)[0].Key_name.to_list():
        print('a')
else:
    print('b')
