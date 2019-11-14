import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone

import math

import plotly
import re

from telethon.tl.types import UserStatusOnline, UserStatusOffline, PeerUser, User
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from helper_functions import MainHelper


class StatusController:

    def __init__(self, tg_client):
        self.tg_client = tg_client
        self.db_conn = tg_client.db_conn

    @staticmethod
    def init_db(cur):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS "activity" (
                "user_id" INTEGER NOT NULL,
                "taken_at" DATETIME NOT NULL,
                "expires" DATETIME NULL,
                "signal_type" TEXT
            );
        """)

    @staticmethod
    def datetime_from_str(string, date_format='%Y-%m-%d %H:%M:%S'):
        res = None
        try:
            if string and (string != '-'):
                res = datetime.strptime(string, date_format)
        except:
            res = None
        return res

    @staticmethod
    def datetime_to_str(dtime, date_format='%Y-%m-%d %H:%M:%S'):
        return dtime.strftime(date_format)

    @staticmethod
    def now_local_datetime():
        return datetime.now().replace(microsecond=0).astimezone(tz=None)

    @staticmethod
    def tg_datetime_to_local_datetime(dtime):
        return dtime.replace(tzinfo=timezone.utc).astimezone(tz=None)

    @staticmethod
    def timestamp_to_local_datetime(stamp):
        return StatusController.tg_datetime_to_local_datetime(datetime.utcfromtimestamp(int(stamp)))

    @staticmethod
    def timestamp_to_str(stamp, format='%Y-%m-%d %H:%M:%S'):
        return StatusController.tg_datetime_to_local_datetime(datetime.utcfromtimestamp(int(stamp))).strftime(format)

    @staticmethod
    def timestamp_to_utc_str(stamp, format='%Y-%m-%d %H:%M:%S'):
        return datetime.utcfromtimestamp(int(stamp)).strftime(format)

    def has_user_activity(self, user_id):
        row = self.db_conn.execute("""
            SELECT * FROM `activity` WHERE `user_id` = ? LIMIT 1
        """, [str(user_id)]).fetchone()
        if row:
            return True
        return False

    def get_user_activity_sessions_count(self, user_id):
        row = self.db_conn.execute("""
            SELECT COUNT(1) as 'cnt' FROM `activity` WHERE `user_id` = ?
        """, [str(user_id)]).fetchone()
        if row:
            return int(row['cnt'])
        return 0

    def get_user_messages_entity_types(self, user_id):
        rows = self.db_conn.execute("""
            select DISTINCT(entity_type) as et, COUNT(1) as cnt from messages where from_id=? or to_id=? GROUP BY entity_type
        """, [str(user_id),str(user_id)]).fetchall()
        if rows:
            res = []
            for row in rows:
                et_name = row['et']
                if et_name == 'User':
                    et_name = 'Диалог'
                elif et_name == 'Chat':
                    et_name = 'Чат'
                elif et_name == 'Megagroup':
                    et_name = 'Мегагруппа'
                elif et_name == 'Channel':
                    et_name = 'Канал'
                res.append(et_name + ': ' + str(row['cnt']))
            return res
        return []

    @staticmethod
    def workdays(d, end, allowed_days):
        days = []
        while d.date() <= end.date():
            if d.isoweekday() in allowed_days:
                days.append(d)
            d += timedelta(days=1)
        return days

    async def print_user_activity(self, user_id, user_name, print_type="diap", date_activity=None, result_only_time=False):
        result = []

        date_activity2 = None

        week_days_str = None
        week_days = None
        if date_activity == "weekday":
            week_days = [1,2,3,4,5]
            date_activity = None
        elif date_activity == "weekend":
            week_days = [6,7]
            date_activity = None

        if week_days:
            week_days_str = ",".join(["'"+str(x)+"'" for x in week_days])

        if date_activity:
            if type(date_activity) is tuple:
                date_activity, date_activity2 = date_activity
                date_activity2 = StatusController.datetime_from_str(date_activity2, "%Y-%m-%d")
                date_activity2 = StatusController.datetime_to_str(date_activity2, "%Y-%m-%d")
            date_activity = StatusController.datetime_from_str(date_activity, "%Y-%m-%d")
            date_activity = StatusController.datetime_to_str(date_activity, "%Y-%m-%d")

        if week_days:
            activity_str = 'Weekday in [' + str(week_days) + ']'
            rows = self.db_conn.execute("""
                SELECT *, strftime('%w', `taken_at`) as 'weekday' FROM `activity` WHERE `user_id` = ? AND weekday IN ({}) ORDER BY `taken_at` ASC
            """.format(week_days_str), [str(user_id)]).fetchall()
        elif date_activity and date_activity2:
            activity_str = 'Date in [' + date_activity + ' .. ' + date_activity2 + ']'
            rows = self.db_conn.execute("""
                SELECT * FROM `activity` WHERE `user_id` = ? AND `taken_at`>=? AND `taken_at`<=? ORDER BY `taken_at` ASC
            """, [str(user_id), date_activity, date_activity2]).fetchall()
        elif date_activity:
            activity_str = 'Date=' + date_activity
            rows = self.db_conn.execute("""
                SELECT * FROM `activity` WHERE `user_id` = ? AND `taken_at` LIKE ? ORDER BY `taken_at` ASC
            """, [str(user_id), date_activity + " %"]).fetchall()
        else:
            activity_str = 'Date=All'
            rows = self.db_conn.execute("""
                SELECT * FROM `activity` WHERE `user_id` = ? ORDER BY `taken_at` ASC
            """, [str(user_id)]).fetchall()

        result.append('Printing user activity (User=' + str(user_id) + " Type=" + str(print_type) + " " + activity_str + " onlyTime=" + str(result_only_time) + ')')
        result.append('')

        current_year = datetime.now().year
        current_month = datetime.now().month
        current_day = datetime.now().day

        diaps = []
        diaps_full = []

        def add_diap(dia_start, dia_stop):
            if dia_stop == dia_start:
                dia_stop = dia_stop + timedelta(seconds=1)
            if result_only_time:
                diaps_full.append([dia_start, dia_stop])
                dia_start = dia_start.replace(year=current_year, month=current_month, day=current_day)
                dia_stop = dia_stop.replace(year=current_year, month=current_month, day=current_day)
                if dia_stop < dia_start:
                    diaps.append([datetime(year=current_year, month=current_month, day=current_day), dia_stop])
                    dia_stop = datetime(year=current_year, month=current_month, day=current_day) + timedelta(days=1)
            else:
                diaps_full.append([dia_start, dia_stop])
            diaps.append([dia_start, dia_stop])

        diap_start = None
        diap_expires = None
        diap_stop = None
        for row in rows:
            if row['signal_type'] == 'online':
                cur_start = self.datetime_from_str(row['taken_at'])
                if not diap_start:
                    diap_start = cur_start
                    if row['expires']:
                        diap_expires = self.datetime_from_str(row['expires'])
                elif diap_expires and (not diap_stop) and (diap_expires < cur_start) and (diap_expires > diap_start):
                    add_diap(diap_start, diap_start + timedelta(seconds=math.floor((diap_expires - diap_start).total_seconds() / 2)))
                    diap_expires = self.datetime_from_str(row['expires'])
                    diap_start = cur_start
                elif diap_expires and (diap_expires > cur_start):
                    diap_expires = self.datetime_from_str(row['expires'])
            elif row['signal_type'] == 'offline':
                diap_stop = self.datetime_from_str(row['taken_at'])
                if diap_start and diap_stop and (diap_stop >= diap_start):
                    add_diap(diap_start, diap_stop)
                diap_expires = None
                diap_start = None
                diap_stop = None

        diaps.sort(key=lambda x: x[0])
        diaps_full.sort(key=lambda x: x[0])

        if result_only_time:
            full_dt_format = '%H:%M:%S'
            full_dt_format_excel = '%H:%M'
            x_title = 'Time'
        else:
            full_dt_format = '%Y-%m-%d %H:%M:%S'
            full_dt_format_excel = '%Y-%m-%d %H:%M'
            x_title = 'Date & time'

        if len(diaps) < 1:
            result.append('No data!')
        elif print_type == "diap":
            for diap in diaps:
                result.append(self.datetime_to_str(diap[0], full_dt_format) + ' --- ' + self.datetime_to_str(diap[1], full_dt_format) + '  =  ' + str(
                    round((diap[1] - diap[0]).total_seconds())) + 's')
        elif print_type in ["excel", "plot", "plot_img"]:
            is_csv = (print_type=='excel')
            is_plot = (print_type in ['plot', 'plot_img'])

            first_date = diaps_full[0][0].replace(microsecond=0, second=0, minute=0, hour=0)
            last_date = diaps_full[len(diaps_full) - 1][0].replace(microsecond=0, second=0, minute=0, hour=0)
            end_date = last_date + timedelta(days=1)
            delta_seconds = (end_date - first_date).total_seconds()

            if week_days:
                days_count_full = len(self.workdays(first_date, end_date, week_days))
                if days_count_full < 1:
                    days_count_full = 1
            else:
                days_count_full = math.ceil(delta_seconds / 86400)

            first_date = diaps[0][0].replace(microsecond=0, second=0, minute=0, hour=0)
            last_date = diaps[len(diaps) - 1][0].replace(microsecond=0, second=0, minute=0, hour=0)
            end_date = last_date + timedelta(days=1)
            delta_seconds = (end_date - first_date).total_seconds()
            days_count = math.ceil(delta_seconds / 86400)
            if result_only_time:
                sum_divider = days_count_full
            else:
                sum_divider = 1
            bars_count = 24 * days_count
            seconds_inc = round(delta_seconds / bars_count)
            cur_date = first_date
            bar_index = 1
            if is_csv:
                result.append('Number;Datetime from;Datetime to;Hour;Seconds using Telegram;Times Telegram was used')
            plot_title = 'Activity of user "' + user_name + '"'

            if week_days:
                plot_title = plot_title + ', WeekDays: ' + str(week_days)
            elif date_activity and date_activity2:
                plot_title = plot_title + ', Date: ' + date_activity + " .. " + date_activity2
            elif date_activity:
                if days_count_full == 1:
                    plot_title = plot_title + ', Date: ' + date_activity
            else:
                plot_title = plot_title + ', full log data'

            if days_count_full == 1:
                plot_title = plot_title + ' (1 day)'
            else:
                plot_title = plot_title + ' (' + str(days_count_full) + ' days)'

            if result_only_time:
                plot_title = plot_title + ', only time'

            plot_data = {
                'title': plot_title,
                'x_full_title': x_title,
                'x_full': [],
                'y_time': [],
                'y_intervals': [],
                'y_interval_time': [],
                'x_days': [],
                'y_days_time': [],
                'y_days_intervals': [],
                'y_days_interval_time': [],
            }
            while cur_date < end_date:
                cur_next_date = cur_date + timedelta(seconds=seconds_inc)
                curr_hour = float(self.datetime_to_str(cur_date, '%H')) + float(
                    self.datetime_to_str(cur_date, '%M')) / 60
                curr_hour = round(curr_hour * 100) * 0.01
                (sum_time, diap_cnt) = self.get_summary_time_in_interval(diaps, cur_date, cur_next_date)
                if is_csv:
                    res_str = str(bar_index) + ";" + self.datetime_to_str(cur_date, full_dt_format_excel) + ";" + \
                              self.datetime_to_str(cur_next_date, full_dt_format_excel) + ";" + \
                              str(curr_hour) + ";" + str(sum_time / sum_divider) + ";" + str(diap_cnt / sum_divider)
                    result.append(res_str)
                if is_plot:
                    plot_data['x_full'].append(self.datetime_to_str(cur_date, full_dt_format_excel))
                    plot_data['y_time'].append((sum_time / sum_divider) / 60)
                    plot_data['y_intervals'].append(diap_cnt / sum_divider)
                    if diap_cnt > 0:
                        plot_data['y_interval_time'].append((sum_time / diap_cnt) / 60)
                    else:
                        plot_data['y_interval_time'].append(0)
                cur_date = cur_next_date
                bar_index = bar_index + 1

            if is_plot:
                bars_count = days_count
                seconds_inc = round(delta_seconds / bars_count)
                cur_date = first_date
                bar_index = 1
                while cur_date < end_date:
                    cur_next_date = cur_date + timedelta(seconds=seconds_inc)
                    (sum_time, diap_cnt) = self.get_summary_time_in_interval(diaps, cur_date, cur_next_date)
                    plot_data['x_days'].append(self.datetime_to_str(cur_date, full_dt_format_excel))
                    plot_data['y_days_time'].append((sum_time / sum_divider) / 60)
                    plot_data['y_days_intervals'].append(diap_cnt / sum_divider)
                    if diap_cnt > 0:
                        plot_data['y_days_interval_time'].append((sum_time / diap_cnt) / 60)
                    else:
                        plot_data['y_days_interval_time'].append(0)
                    cur_date = cur_next_date
                    bar_index = bar_index + 1

            if is_plot:
                result.append(self.show_plot(plot_data, days_count, result_only_time, print_type=='plot_img'))

        result = "\n".join(result)

        print(result)
        return result

    def show_plot(self, plot_data, days_count, result_only_time, as_img=False):

        # https://plot.ly/python/renderers/

        if (days_count < 2) or result_only_time:
            fig = make_subplots(rows=2, cols=4, specs=[
                [{"colspan": 2}, None, {"colspan": 2}, None],
                [None, {"colspan": 2}, None, None]
            ])
            row2 = 1
            col2 = 3
            row3 = 2
            col3 = 2
        else:
            fig = make_subplots(rows=3, cols=6, specs=[
                [{"colspan": 6}, None, None, None, None, None],
                [{"colspan": 3}, None, None, {"colspan": 3}, None, None],
                [{"colspan": 2}, None, {"colspan": 2}, None, {"colspan": 2}, None]
            ])
            row2 = 2
            col2 = 1
            row3 = 2
            col3 = 4

        fig.update_layout(
            title_text=plot_data['title']
        )

        fig.add_trace(
            go.Bar(x=plot_data['x_full'], y=plot_data['y_time'], name="Activity time (minutes)"),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(x=plot_data['x_full'], y=plot_data['y_intervals'], name="Sessions count"),
            row=row2, col=col2
        )
        fig.add_trace(
            go.Bar(x=plot_data['x_full'], y=plot_data['y_interval_time'], name="Avg session time (minutes)"),
            row=row3, col=col3
        )

        fig.update_yaxes(title_text='Activity time', row=1, col=1)
        fig.update_yaxes(title_text='Sessions count', row=row2, col=col2)
        fig.update_yaxes(title_text='Avg session time', row=row3, col=col3)

        if (days_count >= 2) and not result_only_time:
            fig.add_trace(
                go.Bar(x=plot_data['x_days'], y=plot_data['y_days_time'], name="Days activity time (minutes)"),
                row=3, col=1
            )
            fig.update_yaxes(title_text='Activity time', row=3, col=1)

            fig.add_trace(
                go.Bar(x=plot_data['x_days'], y=plot_data['y_days_intervals'], name="Days sessions count"),
                row=3, col=3
            )
            fig.update_yaxes(title_text='Sessions count', row=3, col=3)

            fig.add_trace(
                go.Bar(x=plot_data['x_days'], y=plot_data['y_days_interval_time'], name="Days avg session time (minutes)"),
                row=3, col=5
            )
            fig.update_yaxes(title_text='Avg session time', row=3, col=5)

        # fig.update_yaxes(type="log")

        if as_img:
            if MainHelper().get_config_value('main', 'orca_path'):
                try:
                    plotly.io.orca.config.executable = MainHelper().get_config_value('main', 'orca_path')
                    fname = str(datetime.now().timestamp()) + '_' + hashlib.md5(plot_data['title'].encode('utf-8')).hexdigest()
                    fname = MainHelper().get_config_root_folder_value('main', 'files_folder') + "/" + fname + ".png"
                    fig.write_image(fname, format="png", width=1920, height=1080)
                    return fname
                except:
                    return 'Какая-то ошибка. Скорее всего некорректно настроен orca или не хватает прав на запись файла!'
            else:
                return 'Требуется сначала настроить orca!'
        else:
            fig.show()

        return ''

    async def get_user_aa_statistics_text(self, user_id, with_uname=True):

        stat_res = ''

        if user_id == self.tg_client.me_user_id:
            res = self.db_conn.execute(
                """
                    SELECT AVG(to_answer_sec) as 'answer_sec' FROM `entities` WHERE `to_answer_sec` is NOT NULL
                """,
                []
            )
        else:
            res = self.db_conn.execute(
                """
                    SELECT AVG(from_answer_sec) as 'answer_sec' FROM `entities` WHERE `from_answer_sec` is NOT NULL AND entity_id = ?
                """,
                [str(user_id)]
            )
        row = res.fetchone()

        if row and ('answer_sec' in row) and row['answer_sec']:
            if with_uname:
                user_name = await self.tg_client.get_entity_name(user_id, 'User')
                stat_res = 'Пользователь ' + user_name
            else:
                stat_res = 'Пользователь'

            answer_time = row['answer_sec'] / 60
            stat_res = stat_res + ' в среднем отвечает в течении {0:0.2f} мин.'.format(answer_time)

        return stat_res

    async def get_stat_user_messages(self, user_id, from_user_id=None):
        results = []
        res = self.db_conn.execute(
            """
                SELECT m.`entity_id` as 'entity_id', et.entity_type as 'entity_type'
                FROM `messages` m LEFT JOIN `entities` et ON et.entity_id == m.entity_id AND et.version = (SELECT MAX(version) FROM `entities` WHERE `entity_id` = m.entity_id)
                WHERE (m.`entity_id` = ? or m.`from_id` = ?) and (et.entity_type IS NOT NULL)
                GROUP BY m.entity_id
                ORDER BY m.entity_id DESC
            """,
            [str(user_id), str(user_id)])
        rows = list(res.fetchall())
        chat_ids_all = [x['entity_id'] for x in filter(lambda x: x['entity_type'] in ['Megagroup', 'Channel', 'Chat'], rows)]
        user_ids_all = [x['entity_id'] for x in filter(lambda x: x['entity_type'] in ['User'], rows)]
        res = self.db_conn.execute(
            """
                SELECT m.`entity_id` as 'entity_id', et.entity_type as 'entity_type'
                FROM `messages` m LEFT JOIN `entities` et ON et.entity_id == m.entity_id AND et.version = (SELECT MAX(version) FROM `entities` WHERE `entity_id` = m.entity_id)
                WHERE (m.`entity_id` = ? or m.`from_id` = ?) and (et.entity_type IS NOT NULL) and m.taken_at > ?
                GROUP BY m.entity_id
                ORDER BY m.entity_id DESC
            """,
            [str(user_id), str(user_id), self.datetime_to_str(datetime.now() + timedelta(days=-30), '%Y-%m-%d')])
        rows = list(res.fetchall())
        chat_ids_month = [x['entity_id'] for x in filter(lambda x: x['entity_type'] in ['Megagroup', 'Channel', 'Chat'], rows)]
        user_ids_month = [x['entity_id'] for x in filter(lambda x: x['entity_type'] in ['User'], rows)]
        res = self.db_conn.execute(
            """
                SELECT m.`entity_id` as 'entity_id', et.entity_type as 'entity_type'
                FROM `messages` m LEFT JOIN `entities` et ON et.entity_id == m.entity_id AND et.version = (SELECT MAX(version) FROM `entities` WHERE `entity_id` = m.entity_id)
                WHERE (m.`entity_id` = ? or m.`from_id` = ?) and (et.entity_type IS NOT NULL) and m.taken_at > ?
                GROUP BY m.entity_id
                ORDER BY m.entity_id DESC
            """,
            [str(user_id), str(user_id), self.datetime_to_str(datetime.now() + timedelta(days=-7), '%Y-%m-%d')])
        rows = list(res.fetchall())
        chat_ids_week = [x['entity_id'] for x in filter(lambda x: x['entity_type'] in ['Megagroup', 'Channel', 'Chat'], rows)]
        user_ids_week = [x['entity_id'] for x in filter(lambda x: x['entity_type'] in ['User'], rows)]

        if len(rows) > 0:
            results.append('**Активный участник (за последнюю неделю / месяц / всего): **')
            results.append('Чатов/мегагрупп: ' + str(len(chat_ids_week)) + ' / ' + str(len(chat_ids_month)) + ' / ' + str(len(chat_ids_all)))
            if len(user_ids_all) > 1:
                results.append('Диалогов с пользователями: ' + str(len(user_ids_week)) + ' / ' + str(len(user_ids_month)) + ' / ' + str(len(user_ids_all)))
            results.append('')

        insta = self.tg_client.entity_controller.get_user_instagram_name(user_id)
        if insta:
            results.append('Инстаграм: ' + str(insta))

        bot_ver = self.tg_client.entity_controller.get_user_bot_last_version(user_id)
        if bot_ver:
            bot_ver = float(bot_ver)
            results.append('Использует бота, последняя версия {0:0.2f}'.format(bot_ver))

        level_rt = await self.tg_client.bot_controller.get_user_rights_level_realtime(user_id)
        level = await self.tg_client.bot_controller.get_user_rights_level(user_id)
        if level_rt > -1:
            results.append('Недавно использовал бота')
        if level > -1:
            results.append('Уровень доступа к боту: {}'.format(level))

        results = "\n".join(results)
        return results

    @staticmethod
    def get_summary_time_in_interval(diaps, d_from, d_to):
        sum_time = 0
        diap_count = 0
        for diap in diaps:
            if (diap[0] >= d_from) and (diap[1] <= d_to):
                sum_time = sum_time + (diap[1] - diap[0]).total_seconds()
                diap_count = diap_count + 1
            elif (diap[0] <= d_from) and (diap[1] >= d_to):
                sum_time = sum_time + (d_to - d_from).total_seconds()
                diap_count = diap_count + 1
            elif (diap[0] <= d_from) and (diap[1] >= d_from) and (diap[1] <= d_to):
                sum_time = sum_time + (diap[1] - d_from).total_seconds()
                diap_count = diap_count + 1
            elif (diap[0] >= d_from) and (diap[0] <= d_to) and (diap[1] >= d_to):
                sum_time = sum_time + (d_to - diap[0]).total_seconds()
                diap_count = diap_count + 1
        return sum_time, diap_count

    async def add_current_status(self, status, user_id, login, use_last_time=False):
        current_time = StatusController.now_local_datetime()
        if type(status) == UserStatusOnline:
            exp_time = StatusController.tg_datetime_to_local_datetime(status.expires)
            await self.add_login_activity(user_id, login, current_time, exp_time, True)
        elif type(status) == UserStatusOffline:
            if use_last_time:
                last_time = StatusController.tg_datetime_to_local_datetime(status.was_online)
            else:
                last_time = current_time
            await self.add_login_activity(user_id, login, last_time, None, False)

    async def add_login_activity(self, user_id, login, date_activity, date_expires=None, is_begin=False):
        if is_begin:
            status_type = 'online'
        else:
            status_type = 'offline'
        date_activity = self.datetime_to_str(date_activity)
        if date_expires:
            date_expires = self.datetime_to_str(date_expires)
        print(date_activity + ' Activity: ' + login + ' ' + status_type)
        try:
            c = self.db_conn.cursor()
            if is_begin:
                row = self.db_conn.execute(
                    'SELECT `expires`, `signal_type` FROM `activity` WHERE `user_id` = ? ORDER BY `taken_at` DESC LIMIT 1',
                    [str(user_id)])
                row = row.fetchone()
                if row:
                    if (row['signal_type'] == "online") and (row['expires'] is not None) and (
                            row['expires'] < date_activity):
                        c.execute('INSERT INTO `activity` VALUES(?,?,?,?)',
                                  (str(user_id), row['expires'], None, 'offline'))
                        c.execute(
                            'UPDATE `activity` SET `expires` = NULL WHERE user_id = ? AND `signal_type` == "online" AND `expires` == ?',
                            (str(user_id), row['expires']))
            c.execute('INSERT INTO `activity` VALUES(?,?,?,?)',
                      (str(user_id), date_activity, date_expires, status_type))
            if not is_begin:
                c.execute(
                    'UPDATE `activity` SET `expires` = NULL WHERE user_id = ? AND `signal_type` == "online" AND `expires` > ?',
                    (str(user_id), date_activity))
            self.db_conn.commit()
        except sqlite3.IntegrityError:
            print('DB error!')
