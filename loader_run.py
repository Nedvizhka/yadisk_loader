import datetime
import time
import argparse
import threading

from main_loader import run_ya_loader
from main_utils import get_config, check_balance, run_bot_send_msg

if __name__ == '__main__':
    # определение аргументов для запуска скрипта на препрод/прод (--env 'preprod' в запуске)

    parser = argparse.ArgumentParser(description='parse arguments to run script on prod or preprod')
    parser.add_argument("--env")
    args = parser.parse_args()
    env_value = args.env
    print("скрипт запущен с аргументом: ", str(env_value))

    while True:
        # ежедневный запуск скрипта происходит только в определенный час start_time в config файле
        if datetime.datetime.now().hour != get_config(env_value=env_value, get_only_start_time=True):
            # проверка баланса за 2 часа до запуска
            if env_value == None and datetime.datetime.now().hour == get_config(env_value=env_value, get_only_start_time=True) - 2:
                dadata_balance = check_balance()
                balance_txt = f'Остаток баланса {dadata_balance} ₽ {"❌" if dadata_balance < 500 else "✅"}'
                run_bot_send_msg(balance_txt, logg=None)
                print(f'Баланс {dadata_balance} отправлен в tg')
                time.sleep(3601)
                continue
            else:
                time.sleep(100)
                continue
        # если текущий час совпадает с заданным start_time - запускаем скрипт
        else:
            try:
                thread = threading.Thread(target=run_ya_loader, args=(env_value))
                thread.start()
                thread.join()
            except Exception as exc:
                print(f'Ошибка: {exc}')
