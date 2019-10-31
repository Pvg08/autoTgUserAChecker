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


class StatusController:

    def __init__(self, tg_client):
        self.tg_client = tg_client
        self.db_conn = tg_client.db_conn
        self.morph = None
        self.normal_form_cache = {}
        self.word_type_form_cache = {}

    @staticmethod
    def init_db(cur):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS "activity" (
                "user_id" INTEGER NOT NULL,
                "login" TEXT,
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
            if self.tg_client.config['main']['orca_path']:
                try:
                    plotly.io.orca.config.executable = self.tg_client.config['main']['orca_path']
                    fname = str(datetime.now().timestamp()) + '_' + hashlib.md5(plot_data['title'].encode('utf-8')).hexdigest()
                    fname = self.tg_client.config['main']['files_folder'] + "/" + fname + ".png"
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
            stat_res = stat_res + ' в среднем отвечает в течении {0:0.001f} мин.'.format(answer_time)

        return stat_res

    def is_valid_word(self, string, allowed_types=None):
        if re.search(r"[a-zA-Z0-9]+", string) is not None:
            return False
        if len(string) < 2:
            return False
        if string in ['мочь', 'раз', 'есть', 'чаща', 'лвойственность']:
            return False
        if allowed_types is None:
            allowed_types = ['СУЩ', 'ПРИЛ', 'ИНФ', 'ПРИЧ', 'ГЛ', 'КР_ПРИЛ', 'ПРЕДК', 'МЕЖД']
        elif len(allowed_types) == 0:
            return True
        return self.get_word_type(string) in allowed_types

    def get_normal_form(self, word):
        if not self.morph:
            import pymorphy2
            self.morph = pymorphy2.MorphAnalyzer()
        if word not in self.normal_form_cache:
            ppparse = self.morph.parse(word)
            if ppparse and ppparse[0] and ppparse[0].normal_form:
                self.normal_form_cache[word] = ppparse[0].normal_form
            else:
                self.normal_form_cache[word] = word
        return self.normal_form_cache[word]

    def get_word_type(self, word):
        if not self.morph:
            import pymorphy2
            self.morph = pymorphy2.MorphAnalyzer()
        if word not in self.word_type_form_cache:
            self.word_type_form_cache[word] = self.morph.parse(word)[0].tag.cyr_repr.split(",")[0].split()[0]
        return self.word_type_form_cache[word]

    async def get_me_dialog_statistics(self, user_id, date_from=None, title='за всё время', only_last_dialog=False, skip_vocab=False):
        days_a = '?'
        if (not date_from) and (not only_last_dialog):
            res = self.db_conn.execute(
                """
                    SELECT *
                    FROM `activity`
                    ORDER BY taken_at ASC
                """,
                [])
            rows = list(res.fetchall())
            days_a = 1
            if len(rows) > 1:
                date1 = self.datetime_from_str(rows[0]['taken_at'])
                date2 = self.datetime_from_str(rows[len(rows) - 1]['taken_at'])
                days_a = round((date2 - date1).total_seconds() / (24 * 60 * 60))
        results = []
        last_dialogue_date = None
        try:
            user_entity = await self.tg_client.get_entity(PeerUser(user_id))
        except:
            user_entity = None
        if user_entity and (type(user_entity) == User):
            if not date_from:
                res = self.db_conn.execute(
                    """
                        SELECT *
                        FROM `messages`
                        WHERE `entity_id` = ? OR `entity_id` = ?
                        ORDER BY `taken_at` ASC
                    """,
                    [str(user_id), str(self.tg_client.me_user_id)]
                )
            else:
                date_from = self.datetime_to_str(date_from, '%Y-%m-%d')
                res = self.db_conn.execute(
                    """
                        SELECT *
                        FROM `messages`
                        WHERE (`entity_id` = ? OR `entity_id` = ?) AND (`taken_at` > ?)
                        ORDER BY `taken_at` ASC
                    """,
                    [str(user_id), str(self.tg_client.me_user_id), date_from]
                )
            rows = list(res.fetchall())

            me_name = await self.tg_client.get_entity_name(self.tg_client.me_user_id, 'User')
            another_name = await self.tg_client.get_entity_name(user_id, 'User')
            dialog_name = me_name + ' <-> ' + another_name

            results.append('**Диалог '+dialog_name+' ('+title+')'+':**')
            results.append('')
            results.append('Сообщений диалога в БД: ' + str(len(rows)))
            if len(rows) > 0:
                date_start = self.datetime_from_str(rows[0]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                if not only_last_dialog:
                    results.append('Самое раннее сообщение диалога в БД: ' + self.datetime_to_str(date_start))
                if len(rows) > 1:
                    date_end = self.datetime_from_str(rows[len(rows) - 1]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                    seconds_count = (date_end - date_start).total_seconds()
                    days_count = seconds_count / (24 * 60 * 60)
                    messages_count = len(rows)
                    if (not date_from) and (not only_last_dialog):
                        results.append('Длительность общения: {0:0.001f} суток'.format(days_count))
                    if not only_last_dialog:
                        results.append('Средняя частота сообщений: {0:0.001f} в сутки'.format(messages_count / days_count))

                    max_dialog_question_interval = round((24 * 60 * 60) * 1.25)
                    max_dialog_non_question_interval = round((24 * 60 * 60) * 0.75)
                    max_dialog_hello_as_second_message_offset = round((24 * 60 * 60) * 0.25)
                    dialog_hello_words = ['привет', 'приветствую', 'здравствуй', 'здравствуйте']
                    dialog_hello_stop_context = ['-привет', 'всем привет', 'привет»', 'привет"']

                    msg_len_me = 0
                    msg_me_cnt = 0
                    msg_me_max_len = 0
                    msg_len_another = 0
                    msg_another_cnt = 0
                    msg_another_max_len = 0

                    me_edits = 0
                    another_edits = 0
                    me_deletes = 0
                    another_deletes = 0

                    me_hello = 0
                    another_hello = 0

                    me_words = []
                    another_words = []

                    dialogues = []
                    active_dialog = []
                    last_msg_from_id = None
                    last_date = date_start
                    last_msg_is_question = False
                    for row in rows:
                        if not row['message']:
                            row['message'] = ''
                        if int(row['removed']) == 1:
                            if int(row['from_id']) == self.tg_client.me_user_id:
                                me_deletes = me_deletes + 1
                            else:
                                another_deletes = another_deletes + 1
                        else:
                            if int(row['version']) == 1:
                                message_lower = str(row['message']).lower()
                                message_words = re.sub("[^\w]", " ", message_lower).split()
                                message_words = list(filter(lambda x: x and self.is_valid_word(x, []), message_words))

                                message_hello_words = list(filter(lambda x: x in dialog_hello_words, message_words))
                                message_stop_contexts = list(filter(lambda x: message_lower.find(x) >= 0, dialog_hello_stop_context))

                                msg_from_id = int(row['from_id'])
                                msg_is_question = str(row['message']).find('?') >= 0
                                msg_is_hello = (len(message_hello_words) > 0) and (len(message_stop_contexts) == 0)

                                if not skip_vocab:
                                    nform_list = [self.get_normal_form(x) for x in message_words]
                                    if msg_from_id == self.tg_client.me_user_id:
                                        me_words = me_words + nform_list
                                    else:
                                        another_words = another_words + nform_list

                                if msg_is_hello:
                                    if msg_from_id == self.tg_client.me_user_id:
                                        me_hello = me_hello + 1
                                    else:
                                        another_hello = another_hello + 1
                                msg_len = len(row['message'])
                                if msg_from_id == self.tg_client.me_user_id:
                                    msg_len_me = msg_len_me + msg_len
                                    msg_me_cnt = msg_me_cnt + 1
                                    if msg_len > msg_me_max_len:
                                        msg_me_max_len = msg_len
                                else:
                                    msg_len_another = msg_len_another + msg_len
                                    msg_another_cnt = msg_another_cnt + 1
                                    if msg_len > msg_another_max_len:
                                        msg_another_max_len = msg_len
                                msg_date = self.datetime_from_str(row['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                                if (
                                        (len(active_dialog) == 0) or (
                                            last_msg_is_question and
                                            ((msg_date - last_date).total_seconds() > max_dialog_question_interval)
                                        ) or (
                                            not last_msg_is_question and
                                            ((msg_date - last_date).total_seconds() > max_dialog_non_question_interval)
                                        )
                                ):
                                    if len(active_dialog) > 0:
                                        dialogues.append(active_dialog)
                                        active_dialog = []
                                active_dialog.append(row)
                                last_date = msg_date
                                if last_msg_from_id != msg_from_id:
                                    last_msg_is_question = msg_is_question
                                else:
                                    last_msg_is_question = last_msg_is_question or msg_is_question
                                last_msg_from_id = msg_from_id
                            else:
                                if int(row['from_id']) == self.tg_client.me_user_id:
                                    me_edits = me_edits + 1
                                else:
                                    another_edits = another_edits + 1

                    if len(active_dialog) > 0:
                        dialogues.append(active_dialog)
                        active_dialog = []

                    if only_last_dialog:
                        dialogues = [dialogues[len(dialogues) - 1]]
                    else:
                        if len(dialogues) > 0:
                            last_dialogue_date = self.datetime_from_str(dialogues[len(dialogues) - 1][0]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                        else:
                            last_dialogue_date = None

                    answers_me = 0
                    answers_wait_seconds_me = 0
                    answers_another = 0
                    answers_wait_seconds_another = 0

                    longest_len = 0
                    longest_dialog = None
                    shortest_len = 0

                    dia_me_start = 0
                    dia_another_start = 0

                    dia_me_finish = 0
                    dia_another_finish = 0

                    dia_between_seconds = 0
                    dia_between_max = 0
                    dia_between_max_from = None
                    dia_between_max_to = None
                    dia_between_cnt = 0

                    last_dia_end = None
                    for dial in dialogues:
                        dial_len = len(dial)

                        if (shortest_len == 0) or (dial_len < shortest_len):
                            shortest_len = dial_len
                        if (longest_len == 0) or (dial_len > longest_len):
                            longest_len = dial_len
                            longest_dialog = dial

                        if dial_len > 0:
                            first_dial = dial[0]
                            last_dial = dial[len(dial) - 1]
                            if int(first_dial['from_id']) == self.tg_client.me_user_id:
                                dia_me_start = dia_me_start + 1
                            else:
                                dia_another_start = dia_another_start + 1
                            if int(last_dial['from_id']) == self.tg_client.me_user_id:
                                dia_me_finish = dia_me_finish + 1
                            else:
                                dia_another_finish = dia_another_finish + 1
                            if last_dia_end:
                                curr_first_dia_begin = self.datetime_from_str(first_dial['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                                dia_between_seconds_curr = (curr_first_dia_begin - last_dia_end).total_seconds()
                                dia_between_seconds = dia_between_seconds + dia_between_seconds_curr
                                dia_between_cnt = dia_between_cnt + 1
                                if dia_between_seconds_curr > dia_between_max:
                                    dia_between_max = dia_between_seconds_curr
                                    dia_between_max_from = last_dia_end
                                    dia_between_max_to = curr_first_dia_begin
                            last_dia_end = self.datetime_from_str(last_dial['taken_at'], '%Y-%m-%d %H:%M:%S%z')

                        last_msg_id = None
                        last_msg_date = None
                        for dia in dial:
                            msg_date = self.datetime_from_str(dia['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                            if last_msg_id and (last_msg_id != int(dia['from_id'])):
                                seconds_between = (msg_date - last_msg_date).total_seconds()
                                if seconds_between > (60 * 60 * 4) and ((last_msg_date.time().hour < 6) or (last_msg_date.time().hour >= 22)):
                                    # somebody just sleep
                                    last_msg_date = msg_date
                                    last_msg_id = int(dia['from_id'])
                                    continue
                                if last_msg_id == self.tg_client.me_user_id:
                                    answers_another = answers_another + 1
                                    answers_wait_seconds_another = answers_wait_seconds_another + seconds_between
                                else:
                                    answers_me = answers_me + 1
                                    answers_wait_seconds_me = answers_wait_seconds_me + seconds_between
                            last_msg_date = msg_date
                            last_msg_id = int(dia['from_id'])

                    if dia_between_cnt > 0:
                        dia_between_time = dia_between_seconds / dia_between_cnt
                        dia_between_time = "{0:0.01f} сут.".format(dia_between_time / (60 * 60 * 24))
                        dia_between_time_max = "{0:0.01f} сут.".format(dia_between_max / (60 * 60 * 24))
                        dia_between_time_max = self.datetime_to_str(dia_between_max_from) + ' --- ' + self.datetime_to_str(dia_between_max_to) + ' ('+dia_between_time_max+')'
                    else:
                        dia_between_time = '?'
                        dia_between_time_max = '?'

                    if answers_another > 0:
                        answers_wait_seconds_another = answers_wait_seconds_another / answers_another
                        another_answer_time = "{0:0.01f} мин.".format(answers_wait_seconds_another / 60)
                    else:
                        another_answer_time = '?'

                    if answers_me > 0:
                        answers_wait_seconds_me = answers_wait_seconds_me / answers_me
                        me_answer_time = "{0:0.01f} мин.".format(answers_wait_seconds_me / 60)
                    else:
                        me_answer_time = '?'

                    if not date_from:
                        self.tg_client.entity_controller.set_entity_answer_sec(user_id, answers_wait_seconds_me, answers_wait_seconds_another)

                    longest_dates = ''
                    longest_hours = 0
                    if longest_len > 1:
                        msg_date1 = self.datetime_from_str(longest_dialog[0]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                        msg_date2 = self.datetime_from_str(longest_dialog[longest_len - 1]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                        longest_hours = (msg_date2 - msg_date1).total_seconds() / (24 * 60 * 60)
                        longest_dates = self.datetime_to_str(msg_date1) + ' --- ' + self.datetime_to_str(msg_date2)

                    valid_word_types = ['СУЩ', 'МЕЖД']
                    valid_word_types_str = (",".join(valid_word_types)).lower()

                    me_top_10 = None
                    me_last_cnt = None
                    me_words_count = 0
                    if (not skip_vocab) and (len(me_words) > 0):
                        all_words = {}
                        for word in me_words:
                            if word and (word not in all_words):
                                all_words[word] = True
                        me_words_count = len(all_words)

                        me_words = list(filter(lambda x: x and self.is_valid_word(x, valid_word_types), me_words))
                        wordlist = sorted(me_words)
                        wordfreq = [wordlist.count(p) for p in wordlist]
                        dic = dict(zip(wordlist, wordfreq))
                        words = list(sorted(dic.items(), key=lambda x: x[1], reverse=True))
                        half_words = round(len(words) / 2)
                        if half_words < 3:
                            half_words = 3
                        elif half_words > 15:
                            half_words = 15
                        top_10 = list(map(lambda x: '**' + str(x[0]) + '** (' + str(x[1]) + ')', words[0:half_words]))
                        last_word, last_cnt = words[len(words) - 1]
                        last_cnt_words = map(lambda x: x[0], filter(lambda s: s[1] == last_cnt, words))
                        last_cnt_words = list(sorted(last_cnt_words, key=lambda x: len(x), reverse=True))
                        last_cnt_cnt = len(last_cnt_words) - half_words
                        if last_cnt_cnt < 3:
                            last_cnt_cnt = 3
                        elif last_cnt_cnt > 10:
                            last_cnt_cnt = 10
                        last_cnt_words = last_cnt_words[0:last_cnt_cnt]
                        me_top_10 = top_10
                        me_last_cnt = last_cnt_words

                    another_top_10 = None
                    another_last_cnt = None
                    another_words_count = 0
                    if (not skip_vocab) and (len(another_words) > 0):
                        all_words = {}
                        for word in another_words:
                            if word and (word not in all_words):
                                all_words[word] = True
                        another_words_count = len(all_words)

                        another_words = list(filter(lambda x: x and self.is_valid_word(x, valid_word_types), another_words))
                        wordlist = sorted(another_words)
                        wordfreq = [wordlist.count(p) for p in wordlist]
                        dic = dict(zip(wordlist, wordfreq))
                        words = list(sorted(dic.items(), key=lambda x: x[1], reverse=True))
                        half_words = round(len(words) / 2)
                        if half_words < 3:
                            half_words = 3
                        elif half_words > 15:
                            half_words = 15
                        top_10 = list(map(lambda x: '**' + str(x[0]) + '** (' + str(x[1]) + ')', words[0:half_words]))
                        last_word, last_cnt = words[len(words) - 1]
                        last_cnt_words = map(lambda x: x[0], filter(lambda s: s[1] == last_cnt, words))
                        last_cnt_words = list(sorted(last_cnt_words, key=lambda x: len(x), reverse=True))
                        last_cnt_cnt = len(last_cnt_words) - half_words
                        if last_cnt_cnt < 3:
                            last_cnt_cnt = 3
                        elif last_cnt_cnt > 10:
                            last_cnt_cnt = 10
                        last_cnt_words = last_cnt_words[0:last_cnt_cnt]
                        another_top_10 = top_10
                        another_last_cnt = last_cnt_words

                    results.append('Сообщений '+another_name+': {0:0.001f} Kb.'.format(msg_len_another/1024))
                    results.append('Сообщений '+me_name+': {0:0.001f} Kb.'.format(msg_len_me/1024))
                    if msg_another_cnt > 0:
                        results.append('Средняя длина сообщения '+another_name+': {0:0.01f} сим.'.format(msg_len_another / msg_another_cnt))
                    if msg_me_cnt > 0:
                        results.append('Средняя длина сообщения '+me_name+': {0:0.01f} сим.'.format(msg_len_me / msg_me_cnt))
                    results.append('Самое длинное сообщение '+another_name+': ' + str(msg_another_max_len) + ' сим.')
                    results.append('Самое длинное сообщение '+me_name+': ' + str(msg_me_max_len) + ' сим.')
                    results.append('Число приветствий от '+another_name+': ' + str(another_hello))
                    results.append('Число приветствий от '+me_name+': ' + str(me_hello))

                    if not only_last_dialog:
                        results.append('')
                        results.append('Число диалогов: ' + str(len(dialogues)))
                        results.append('Сообщений в самом коротком диалоге: ' + str(shortest_len))
                        results.append('Сообщений в самом длинном диалоге: ' + str(longest_len))
                        results.append('Самый длинный диалог: ' + longest_dates + ' ({0:0.001f} сут)'.format(longest_hours))
                        if len(dialogues) > 0:
                            if dia_between_time != '?':
                                results.append('Среднее время между диалогами: ' + str(dia_between_time))
                            if dia_between_time_max != '?':
                                results.append('Самое большое время между диалогами: ' + str(dia_between_time_max))
                            results.append('Инициатор диалога ' + another_name + ': {0:0.001f} %'.format(100 * dia_another_start / len(dialogues)))
                            results.append('Инициатор диалога ' + me_name + ': {0:0.001f} %'.format(100 * dia_me_start / len(dialogues)))
                            results.append('Завершитель диалога ' + another_name + ': {0:0.001f} %'.format(100 * dia_another_finish / len(dialogues)))
                            results.append('Завершитель диалога ' + me_name + ': {0:0.001f} %'.format(100 * dia_me_finish / len(dialogues)))
                    else:
                        results.append('Сообщений в диалоге: ' + str(longest_len))
                        results.append('Продолжительность диалога: ' + longest_dates + ' ({0:0.001f} сут)'.format(longest_hours))
                        if dia_me_start > 0:
                            results.append('Инициатор: ' + me_name)
                        elif dia_another_start > 0:
                            results.append('Инициатор: ' + another_name)
                        if dia_me_finish > 0:
                            results.append('Завершитель: ' + me_name)
                        elif dia_another_finish > 0:
                            results.append('Завершитель: ' + another_name)

                    if another_answer_time != '?':
                        results.append('В среднем ' + another_name + ' отвечает за: ' + another_answer_time)
                    if me_answer_time != '?':
                        results.append('В среднем ' + me_name + ' отвечает за: ' + me_answer_time)
                    results.append('')
                    if (not date_from) and (not only_last_dialog):
                        results.append('За время активности скрипта ('+str(days_a)+' сут.):')
                        results.append('Правок сообщений ' + another_name + ': ' + str(another_edits))
                        results.append('Правок сообщений ' + me_name + ': ' + str(me_edits))
                        results.append('Удалений сообщений ' + another_name + ': ' + str(another_deletes))
                        results.append('Удалений сообщений ' + me_name + ': ' + str(me_deletes))

                    if me_top_10 or another_top_10:
                        results.append('')
                        results.append('Различных слов ' + another_name + ' в диалогах: ' + str(another_words_count))
                        results.append('Различных слов ' + me_name + ' в диалогах: ' + str(me_words_count))
                        results.append('')
                        results.append('Самые частые слова ('+valid_word_types_str+') ' + another_name + ' в диалогах: ' + (", ".join(another_top_10)) + '')
                        results.append('')
                        results.append('Самые частые слова ('+valid_word_types_str+') ' + me_name + ' в диалогах: ' + (", ".join(me_top_10)) + '')
                        results.append('')
                        results.append('Самые редкие слова ('+valid_word_types_str+') ' + another_name + ' в диалогах: ' + (", ".join(another_last_cnt)) + '')
                        results.append('')
                        results.append('Самые редкие слова ('+valid_word_types_str+') ' + me_name + ' в диалогах: ' + (", ".join(me_last_cnt)) + '')

        return {
            'results': results,
            'last_dialogue_date': last_dialogue_date
        }

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
            results.append('Использует бота, последняя версия {0:0.01f}'.format(bot_ver))

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
                        c.execute('INSERT INTO `activity` VALUES(?,?,?,?,?)',
                                  (str(user_id), str(login), row['expires'], None, 'offline'))
                        c.execute(
                            'UPDATE `activity` SET `expires` = NULL WHERE user_id = ? AND `signal_type` == "online" AND `expires` == ?',
                            (str(user_id), row['expires']))
            c.execute('INSERT INTO `activity` VALUES(?,?,?,?,?)',
                      (str(user_id), str(login), date_activity, date_expires, status_type))
            if not is_begin:
                c.execute(
                    'UPDATE `activity` SET `expires` = NULL WHERE user_id = ? AND `signal_type` == "online" AND `expires` > ?',
                    (str(user_id), date_activity))
            self.db_conn.commit()
        except sqlite3.IntegrityError:
            print('DB error!')
