from datetime import datetime, timedelta
from io import StringIO
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import numpy as np
import seaborn as sns
import pandas as pd
import pandahouse
import io
import matplotlib.pyplot as plt
import sys
import os

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-baburina-9',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 5),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

class Getch:
    def __init__(self, query, db='simulator_20220720'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)

   
# Функция реализует поиск аномалий в данных, используя межквартильный размах
def check_anomaly_interquartile_range(df, metric, a=4, n=5):
    df['q25']= df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75']= df[metric].shift(1).rolling(n).quantile(0.75)
    df['IQR'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['IQR']
    df['low'] = df['q25'] - a*df['IQR']
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    # Если метрика находится за пределами доверительного интервала, флаг поднимаем
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1] :
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

# Функция реализует поиск аномалий в данных, используя правило 3 сигм. Можно изменять кол-во стандартных отклонений, переменная - a
def check_anomaly_sigma(df, metric, a=3, n=5):
   
    df['Std']= df[metric].shift(1).rolling(n).std()
    df['Mean']= df[metric].shift(1).rolling(n).mean()
    
    df['up'] = df['Mean'] + a*df['Std']
    df['low'] = df['Mean'] - a*df['Std']
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
        
    # Если метрика находится за пределами доверительного интервала, флаг поднимаем
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1] :
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alerts_baburina():
    
    # Функция системы алертов для ленты
    @task()
    def run_alerts_feed(chat=None):
        chat_id = chat or -788021703
        bot = telegram.Bot(token = '5459195150:AAFvzrjCRiweqxr31LlCYTfiLboDV4CPgYY')
        data = Getch('''SELECT
                           toStartOfFifteenMinutes(time) as ts,
                           toDate(time) as date,
                           formatDateTime(ts, '%R') as hm,
                           uniqExact(user_id) as users_feed,
                           countIf(user_id, action = 'view') as views,
                           countIf(user_id, action = 'like') as likes,
                           round(likes/views,2) as CTR
                           FROM {db}.feed_actions 
                           WHERE time >= today() -1 and time < toStartOfFifteenMinutes(now())
                           GROUP BY ts, date, hm
                           ORDER BY ts''').df

        print(data)

        metrics_list=['users_feed', 'views', 'likes', 'CTR']

        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly_interquartile_range(df, metric)

            # Если метрика находится за пределами доверительного интервала, срабатывает алерт, посылается сообщение

            if is_alert == 1:
                msg = '''Interquartile_range:\n{metric} metric:\ncurrent value: {current_val:.2f}\ndeviation from the previous value: {last_val_diff:.2%}\nhttp://superset.lab.karpov.courses/r/1721'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))
                sns.set(rc={'figure.figsize': (16,10)})
                # Рисуем график
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label = 'low')

                # Рисуем график, скрывая некоторые деления, чтобы не загромождать график
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                            label.set_visible(False)

                            ax.set(xlabel='time')
                            ax.set(ylabel=metric)
                            ax.set_title(metric)
                            ax.set(ylim=(0, None))

                            # Создаем файловый объект, чтобы не выгружать файл локально
                            plot_object =io.BytesIO()
                            ax.figure.savefig(plot_object)
                            # Перемещаем курсор в начало строки файлового объекта
                            plot_object.seek(0)
                            # Называем объект
                            plot_object.name = 'Plot.png'.format(metric)
                            # Закрываем matplotlib.pyplot
                            plt.close()
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return

    # Функция системы алертов для мессенджера
    @task()
    def run_alerts_messenger(chat=None):
        chat_id = chat or -788021703
        bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))

        data = Getch('''SELECT
                                toStartOfFifteenMinutes(time) as ts,
                                toDate(time) as date,
                                formatDateTime(ts, '%R') as hm,
                                uniqExact(user_id) as users_messenger,
                                count(user_id) as sent_messages
                            FROM {db}.message_actions
                            WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts''').df

        print(data)

        metrics_list = ['users_messenger', 'sent_messages']

        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly_interquartile_range(df, metric)

            # Если метрика находится за пределами доверительного интервала, срабатывает алерт, посылается сообщение

            if is_alert == 1:
                msg = '''Interquartile_range:\n{metric} metric:\ncurrent value: {current_val:.2f}\ndeviation from the previous value: {last_val_diff:.2%}\nhttp://superset.lab.karpov.courses/r/1721'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))
                sns.set(rc={'figure.figsize': (16,10)})
                # Рисуем график
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label = 'low')

                # Рисуем график, скрывая некоторые деления, чтобы не загромождать график
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                            label.set_visible(False)

                            ax.set(xlabel='time')
                            ax.set(ylabel=metric)
                            ax.set_title(metric)
                            ax.set(ylim=(0, None))

                            # Создаем файловый объект, чтобы не выгружать файл локально
                            plot_object =io.BytesIO()
                            ax.figure.savefig(plot_object)
                            # Перемещаем курсор в начало строки файлового объекта
                            plot_object.seek(0)
                            # Называем объект
                            plot_object.name = 'Plot.png'.format(metric)
                            # Закрываем matplotlib.pyplot
                            plt.close()
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return
    
    # Функция системы алертов для ленты - правило 3 сигм
    @task()
    def run_alerts_feed_sigma(chat=None):
        chat_id = chat or -788021703
        bot = telegram.Bot(token = os.environ.get("REPORT_BOT_TOKEN"))
        data = Getch('''SELECT
                           toStartOfFifteenMinutes(time) as ts,
                           toDate(time) as date,
                           formatDateTime(ts, '%R') as hm,
                           uniqExact(user_id) as users_feed,
                           countIf(user_id, action = 'view') as views,
                           countIf(user_id, action = 'like') as likes,
                           round(countIf(action = 'like')/countIf(action = 'view'),2) as CTR
                           FROM {db}.feed_actions 
                           WHERE time >= today() -1 and time < toStartOfFifteenMinutes(now())
                           GROUP BY ts, date, hm
                           ORDER BY ts''').df

        print(data)

        metrics_list=['users_feed', 'views', 'likes', 'CTR']

        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly_sigma(df, metric)

            # Если метрика находится за пределами доверительного интервала, срабатывает алерт, посылается сообщение

            if is_alert == 1:
                msg = '''Three-sigma rule:\n{metric} metric:\ncurrent value: {current_val:.2f}\ndeviation from the previous value: {last_val_diff:.2%}\nhttp://superset.lab.karpov.courses/r/1721'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))
                sns.set(rc={'figure.figsize': (16,10)})
                # Рисуем график
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label = 'low')

                # Рисуем график, скрывая некоторые деления, чтобы не загромождать график
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                            label.set_visible(False)

                            ax.set(xlabel='time')
                            ax.set(ylabel=metric)
                            ax.set_title(metric)
                            ax.set(ylim=(0, None))

                            # Создаем файловый объект, чтобы не выгружать файл локально
                            plot_object =io.BytesIO()
                            ax.figure.savefig(plot_object)
                            # Перемещаем курсор в начало строки файлового объекта
                            plot_object.seek(0)
                            # Называем объект
                            plot_object.name = 'Plot.png'.format(metric)
                            # Закрываем matplotlib.pyplot
                            plt.close()
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return
    
    # Функция системы алертов для мессенджера - правило 3 сигм
    @task()
    def run_alerts_messenger_sigma(chat=None):
        chat_id = chat or -788021703
        bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))

        data = Getch('''SELECT
                                toStartOfFifteenMinutes(time) as ts,
                                toDate(time) as date,
                                formatDateTime(ts, '%R') as hm,
                                uniqExact(user_id) as users_messenger,
                                count(user_id) as sent_messages
                            FROM {db}.message_actions
                            WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts''').df

        print(data)

        metrics_list = ['users_messenger', 'sent_messages']

        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly_sigma(df, metric)

            # Если метрика находится за пределами доверительного интервала, срабатывает алерт, посылается сообщение

            if is_alert == 1:
                msg = '''Three-sigma rule:\n{metric} metric:\ncurrent value: {current_val:.2f}\ndeviation from the previous value: {last_val_diff:.2%}\nhttp://superset.lab.karpov.courses/r/1721'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))
                sns.set(rc={'figure.figsize': (16,10)})
                # Рисуем график
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label = 'low')

                # Рисуем график, скрывая некоторые деления, чтобы не загромождать график
                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                            label.set_visible(False)

                            ax.set(xlabel='time')
                            ax.set(ylabel=metric)
                            ax.set_title(metric)
                            ax.set(ylim=(0, None))

                            # Создаем файловый объект, чтобы не выгружать файл локально
                            plot_object =io.BytesIO()
                            ax.figure.savefig(plot_object)
                            # Перемещаем курсор в начало строки файлового объекта
                            plot_object.seek(0)
                            # Называем объект
                            plot_object.name = 'Plot.png'.format(metric)
                            # Закрываем matplotlib.pyplot
                            plt.close()
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return
    
    run_alerts_feed()
    run_alerts_messenger()
    run_alerts_feed_sigma()
    run_alerts_messenger_sigma()
    
dag_alerts_baburina = dag_alerts_baburina()
