import logging
import traceback
import telebot

from dadata import Dadata as ddt_check

import pandas as pd
from sqlalchemy import text

from sql_config_utils import get_sql_engine


def create_common_rep():
    return pd.DataFrame(columns=['parse_date', 'c_total', 'av_total', 'c_new', 'av_new',
                                 'c_adr_total', 'av_adr_total', 'c_adr_new', 'av_adr_new'])

def report_df_append(df, col_name, value):
    df.at[0, col_name] = value


def get_city_table(engine):
    cities_query = 'SELECT * FROM city'
    try:
        con_obj = engine.connect()
        city_db = pd.read_sql(text(cities_query), con=con_obj)
        con_obj.close()
        exc_code = None
    except Exception as exc:
        logging.error(traceback.format_exc())
        city_db = None
        exc_code = exc.code
    return city_db, exc_code


def create_dadata_rep(engine):
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
    city_df, err_c = get_city_table(engine)
    city_df['ddt_avito'] = 0
    city_df['ddt_cian'] = 0
    city_df['ddt_total'] = 0
    city_df = city_df[['id', 'name', 'ddt_total', 'ddt_cian', 'ddt_avito']]
    return city_df, None


def fill_dadata_report(city_df, df_ddt, source):
    try:
        if source == 'avito':
            df_ddt['city'] = df_ddt.addr.apply(lambda x: x.split('; ')[0])
            df_ddt = df_ddt[df_ddt.city.isin(city_df.name.to_list())]
            city_list = df_ddt.city.unique().tolist()
            for city_name in city_list:
                ddt_cnt = len(df_ddt[df_ddt.city == city_name].addr.unique())
                city_idx = city_df.loc[city_df.name == city_name].index[0]
                city_df.at[city_idx, 'ddt_avito'] = ddt_cnt
        else:
            df_ddt['city'] = df_ddt.addr.apply(lambda x: x.split('; ')[1])
            df_ddt = df_ddt[df_ddt.city.isin(city_df.name.to_list())]
            city_list = df_ddt.city.unique().tolist()
            for city_name in city_list:
                ddt_cnt = len(df_ddt[df_ddt.city == city_name].addr.unique())
                city_idx = city_df.loc[city_df.name == city_name].index[0]
                city_df.at[city_idx, 'ddt_cian'] = ddt_cnt
        return
    except:
        logging.error('не удалось создать отчет из данных от dadata')
        logging.error(traceback.format_exc())
        return True


def check_balance():
    token = "f288b25edb6d05b5ceb4d957376104a181c4adee"
    secret = "9d337ae6b9901a6708802eaca6d7055ce2c64772"
    ddt = ddt_check(token, secret)
    result = ddt.get_balance()
    ddt.close()
    return result


def report_text(common_rep_df, dadata_rep_df, limits_df, dadata_balance):
    try:
        # переменная для отчета
        rep_text = str()
        rep_text += f'Обработаны объявления от {common_rep_df.iloc[0, 0]}\n'
        rep_text += f'формат: общее количество (циан/авито)\n\n'

        # первая часть отчета с общей статой 

        temp = pd.DataFrame(columns=['text', 'total', 'c', 'av'])
        c_r = common_rep_df.copy().to_dict('r')[0]
        temp.loc[len(temp)] = ['всего', c_r['c_total'] + c_r['av_total'], c_r['c_total'], c_r['av_total']]
        temp.loc[len(temp)] = ['new', c_r['c_new'] + c_r['av_new'], c_r['c_new'], c_r['av_new']]
        temp.loc[len(temp)] = ['адр.', c_r['c_adr_total'] + c_r['av_adr_total'], c_r['c_adr_total'], c_r['av_adr_total']]
        temp.loc[len(temp)] = ['адр. new', c_r['c_adr_new'] + c_r['av_adr_new'], c_r['c_adr_new'], c_r['av_adr_new']]
        temp['br1'] = '('
        temp['br2'] = ')'
        temp['sl'] = '/'
        temp['sp1'] = ' - '
        temp['sp'] = '$'
        temp = temp[['text', 'sp1', 'total', 'sp', 'br1', 'c', 'sl', 'av', 'br2']]

        t_txt = temp.to_string(index=False, header=False)
        t_txt_1 = [i.split(' - ') for i in t_txt.split('\n')]
        const = max([len(i[0]) for i in t_txt_1])
        for i in t_txt_1:
            i[0] = "".join(i[0].replace(' ', '').split())
            i[1] = " ".join(i[1].replace(' ', '').replace('$', ' ').split())
            i.insert(1, "\xa0" * (const - 2 - len(i[0])))
            i.insert(3, "\xa0")
        for j in [' '.join(elele) for elele in t_txt_1]:
            rep_text += j + '\n'

        rep_text += '\n'

        # вторая часть отчета со статой дадаты

        temp_ddt = dadata_rep_df.copy()
        temp_ddt['ddt_total'] = temp_ddt['ddt_cian'] + temp_ddt['ddt_avito']
        temp_ddt = temp_ddt[temp_ddt.ddt_total != 0]
        temp_ddt.replace(to_replace=0, value='-', inplace=True)

        temp_ddt['br1'] = '('
        temp_ddt['br2'] = ')'
        temp_ddt['sl'] = '/'
        temp_ddt['sp'] = ' '
        temp_ddt['sp1'] = '$'
        temp_ddt = temp_ddt[['id', 'name', 'sp1', 'ddt_total', 'sp', 'br1', 'ddt_cian', 'sl', 'ddt_avito', 'br2']]

        rep_text += 'dadata:' + '\n'

        t_ddt_txt = temp_ddt.to_string(index=False, header=False)
        t_ddt_1 = [i.split('$') for i in t_ddt_txt.split('\n')]
        const = max([len(i[0]) for i in t_ddt_1])
        try:
            for i in t_ddt_1:
                i[0] = " ".join(i[0].split())
                i[1] = " ".join(i[1].replace(' ', '').replace('(', ' (').split())
                i.insert(1, "\xa0" * (const - 2 - len(i[0])))
            t_ddt_2 = [' '.join(ele) for ele in t_ddt_1]
            for j in t_ddt_2:
                rep_text += j + '\n'
        except:
            logging.error(traceback.format_exc())
            err_txt = 'Ошибка запроса к Dadata, проверь баланс'
            rep_text += err_txt + '\n'

        # третья часть с остатоком dadata
        # rep_text += f'\nОстаток баланса {dadata_balance}'

        # четвертая часть с уведомлением об оверлимитах

        report_limits_df = limits_df[limits_df.cnt < limits_df.cnt_ddt]
        if len(report_limits_df) > 0:

            rep_text += f'\nдостигнуты лимиты при обращении к dadata для:\n'
            rep_text += f'формат: город (успешных запросов/новых адресов)\n'

            report_limits_df['cnt_new'] = report_limits_df.cnt_ddt + report_limits_df.cnt_left_after_limit

            report_limits_df['br1'] = '('
            report_limits_df['br2'] = ')'
            report_limits_df['sl'] = '/'
            report_limits_df['sp'] = '$'
            t_limit_txt = report_limits_df[['name', 'sp', 'br1', 'cnt_ddt', 'sl', 'cnt_new', 'br2']].to_string(index=False,
                                                                                                               header=False)

            t_limit_1 = [i.split('$') for i in t_limit_txt.split('\n')]
            const = max([len(i[0]) for i in t_limit_1])
            try:
                for i in t_limit_1:
                    i[0] = " ".join(i[0].split())
                    i[1] = " ".join(i[1].replace(' ', '').replace('(', ' (').split())
                    i.insert(1, "\xa0" * (const - 1 - len(i[0])))
                t_limit_2 = [' '.join(ele) for ele in t_limit_1]
                for j in t_limit_2:
                    rep_text += j + '\n'
            except:
                logging.error(traceback.format_exc())
                err_txt = 'Ошибка формирования отчета по переполнению лимитов'
                rep_text += err_txt + '\n'

        # запись в txt

        with open(f'saved_data_csv/tg_report_{str(c_r["parse_date"])[:10]}.txt', 'w') as rep_file:
            rep_file.write(rep_text)
        rep_file.close()

        # добавим уведомление о балансе

        # try:
            # rep_text += f' ₽ {"❌" if dadata_balance < 500 else "✅"}'
        # except:
            # rep_text += f' Р {"XXX" if dadata_balance < 500 else "OK"}'
        return rep_text
    except:
        logging.error('Ошибка при создании отчета для tg')
        logging.error(traceback.format_exc())
        return


def run_bot_send_msg(msg_txt):
    bot = telebot.TeleBot('5384767557:AAGBM487FW6lZq6D3z7ct44ADqyMjjjXpxc')
    chat_id = '-1001683557389'
    bot.send_message(chat_id, f"`{msg_txt}`", parse_mode='Markdown')
    logging.info('отчет отправлен в tg')
    bot.stop_bot()