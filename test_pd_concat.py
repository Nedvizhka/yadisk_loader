import pandas as pd
a = pd.read_csv('saved_data_csv/avito_dadata_request_2023_04_28.csv', encoding='cp1251', index_col=0)
b = pd.read_csv('saved_data_csv/avito_dadata_request_2023_04_30.csv', encoding='cp1251', index_col=0)
a = a.append(b)
a.loc[len(a.index)] = ['house_fias_id', 'data', 'geo_lat', 'geo_lon', 'street', 'house', 'qc', 'result', 'qc_geo', 'ad_id']
a.to_csv('abc.csv', encoding='cp1251')