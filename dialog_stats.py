import difflib
import re

from telethon.tl.types import PeerUser, User

from helper_functions import CacheHelper
from status_controller import StatusController


class DialogStats:

    def __init__(self, tg_client):
        self.tg_client = tg_client
        self.db_conn = tg_client.db_conn
        self.morph = None
        self.normal_form_cache = {}
        self.word_type_form_cache = {}

    def find_message_by_id_date(self, from_id, date):
        date = StatusController.tg_datetime_to_local_datetime(date)
        date = StatusController.datetime_to_str(date, '%Y-%m-%d %H:%M:%S')
        res = self.db_conn.execute(
            """
                SELECT m.*, 
                (SELECT version FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'max_version',
                (SELECT removed FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'is_removed'
                FROM `messages` m
                WHERE m.`from_id` = ? AND m.`taken_at` LIKE ?
                ORDER BY m.`taken_at` ASC
                LIMIT 1
            """,
            [str(from_id), date + '%']
        )
        row = res.fetchone()
        return row

    def get_message_edits(self, entity_id, message_id):
        res = self.db_conn.execute(
            """
                SELECT m.*, 
                (SELECT version FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'max_version',
                (SELECT removed FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'is_removed'
                FROM `messages` m
                WHERE m.`entity_id` = ? AND m.`message_id` = ?
                ORDER BY m.`taken_at` ASC
            """,
            [str(entity_id), str(message_id)]
        )
        rows = list(res.fetchall())
        return rows

    def remove_message_tags(self, text):
        text = re.sub(r"\[document[^\]]+\]", ' ', text)
        text = re.sub(r"\[photo[^\]]+\]", ' ', text)
        text = text.strip()
        return text

    def get_str_similarity_ratio(self, str1, str2):
        seq = difflib.SequenceMatcher(None, str(str1).strip().lower(), str(str2).strip().lower())
        return seq.ratio()

    def get_str_difference_ratio(self, str1, str2):
        return 1.0 - self.get_str_similarity_ratio(str1, str2)

    def is_valid_word(self, string, allowed_types=None):
        if re.search(r"[a-zA-Z0-9]+", string) is not None:
            return False
        if len(string) < 2:
            return False
        if string in ['мочь', 'раз', 'есть', 'чаща', 'лвойственность', 'ия']:
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

    def cut_text(self, text, max_len=100):
        text = str(text).replace('\n', ' ')
        if len(text) > max_len:
            text = text[:max_len] + '...'
        return '__' + text + '__'

    def get_str_difference_counts(self, str1, str2):
        inserts_count_edit = 0
        replaces_count_edit = 0
        deletes_count_edit = 0
        seq = difflib.SequenceMatcher(None, self.remove_message_tags(str(str1)).lower(), self.remove_message_tags(str(str2)).lower())
        opcodes = seq.get_opcodes()
        for opcode in opcodes:
            if opcode[0] == 'insert':
                inserts_count_edit = inserts_count_edit + 1
            elif opcode[0] == 'replace':
                replaces_count_edit = replaces_count_edit + 1
            elif opcode[0] == 'delete':
                deletes_count_edit = deletes_count_edit + 1
        return {
            'inserts_count_edit': inserts_count_edit,
            'replaces_count_edit': replaces_count_edit,
            'deletes_count_edit': deletes_count_edit,
        }

    def get_edit_stats(self, edited_messages: dict, edited_sequence_interrupted: dict):

        edits_count = 0
        max_1_message_edits = 0
        max_1_message_diff = 0
        mid_1_message_edits = 0.0
        message_edit_mid_full_percent = 0
        message_edit_mid_summ_percent = 0
        message_edit_mid_time_sec = 0
        edited_messages_count = len(edited_messages)
        max_edits_message = ''
        max_changed_message = ''

        replaces_count_avg = 0
        inserts_count_avg = 0
        deletes_count_avg = 0
        sequence_interrupted_cnt = 0

        if edited_messages_count > 0:
            for edit_rows in edited_messages.values():
                # print([(x['taken_at'] + ': "' + x['message'] + '"') for x in edit_rows])
                edit_ratio_summ = 0
                last_version = None
                summ_edits_time = 0
                message_edit_cnt = len(edit_rows) - 1
                mid_1_message_edits = mid_1_message_edits + message_edit_cnt
                if message_edit_cnt >= max_1_message_edits:
                    max_1_message_edits = message_edit_cnt
                    max_edits_message = self.remove_message_tags(edit_rows[len(edit_rows) - 1]['message'])

                replaces_count_edit = 0
                inserts_count_edit = 0
                deletes_count_edit = 0
                for edit_row in edit_rows:
                    if last_version:
                        curr_time = StatusController.datetime_from_str(edit_row['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                        last_time = StatusController.datetime_from_str(last_version['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                        summ_edits_time = summ_edits_time + abs((curr_time - last_time).total_seconds())
                        edits_count = edits_count + 1
                        edit_ratio = self.get_str_difference_ratio(self.remove_message_tags(last_version['message']), self.remove_message_tags(edit_row['message']))
                        edit_ratio_summ = edit_ratio_summ + edit_ratio

                        diff_counts = self.get_str_difference_counts(last_version['message'], edit_row['message'])
                        inserts_count_edit = inserts_count_edit + diff_counts['inserts_count_edit']
                        replaces_count_edit = replaces_count_edit + diff_counts['replaces_count_edit']
                        deletes_count_edit = deletes_count_edit + diff_counts['deletes_count_edit']

                    last_version = edit_row
                edit_ratio_full = self.get_str_difference_ratio(self.remove_message_tags(edit_rows[0]['message']), self.remove_message_tags(last_version['message']))
                if edit_ratio_full >= max_1_message_diff:
                    max_1_message_diff = edit_ratio_full
                    max_changed_message = self.remove_message_tags(edit_rows[len(edit_rows) - 1]['message'])
                message_edit_mid_full_percent = message_edit_mid_full_percent + edit_ratio_full
                message_edit_mid_summ_percent = message_edit_mid_summ_percent + edit_ratio_summ
                message_edit_mid_time_sec = message_edit_mid_time_sec + (summ_edits_time / message_edit_cnt)

                replaces_count_avg = replaces_count_avg + replaces_count_edit
                inserts_count_avg = inserts_count_avg + inserts_count_edit
                deletes_count_avg = deletes_count_avg + deletes_count_edit

                msg_id = edit_rows[0]['message_id']
                if (msg_id in edited_sequence_interrupted) and edited_sequence_interrupted[msg_id]:
                    sequence_interrupted_cnt = sequence_interrupted_cnt + 1

            mid_1_message_edits = mid_1_message_edits / edited_messages_count
            message_edit_mid_full_percent = 100 * message_edit_mid_full_percent / edited_messages_count
            message_edit_mid_summ_percent = 100 * message_edit_mid_summ_percent / edited_messages_count
            message_edit_mid_time_sec = message_edit_mid_time_sec / edited_messages_count
            replaces_count_avg = replaces_count_avg / edited_messages_count
            inserts_count_avg = inserts_count_avg / edited_messages_count
            deletes_count_avg = deletes_count_avg / edited_messages_count
            max_1_message_diff = 100 * max_1_message_diff

        return {
            "edits_count": edits_count,
            "sequence_interrupted_cnt": sequence_interrupted_cnt,
            "edited_messages_count": edited_messages_count,
            "max_1_message_edits": max_1_message_edits,
            "max_1_message_diff_percent": max_1_message_diff,
            "mid_1_message_edits": mid_1_message_edits,
            "message_edit_mid_full_percent": message_edit_mid_full_percent,
            "message_edit_mid_summ_percent": message_edit_mid_summ_percent,
            "message_edit_mid_time_sec": message_edit_mid_time_sec,
            "max_edits_message": max_edits_message,
            "max_changed_message": max_changed_message,
            "replaces_count_avg": replaces_count_avg,
            "inserts_count_avg": inserts_count_avg,
            "deletes_count_avg": deletes_count_avg,
        }

    async def get_me_dialog_statistics(self, user_id, date_from=None, title='за всё время', only_last_dialog=False, skip_vocab=False):

        new_normal_form_cache = CacheHelper().get_from_cache('normal_forms', 'dialog_stats', False)
        if new_normal_form_cache:
            self.normal_form_cache = new_normal_form_cache

        new_word_type_form_cache = CacheHelper().get_from_cache('word_type_forms', 'dialog_stats', False)
        if new_word_type_form_cache:
            self.word_type_form_cache = new_word_type_form_cache

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
                date1 = StatusController.datetime_from_str(rows[0]['taken_at'])
                date2 = StatusController.datetime_from_str(rows[len(rows) - 1]['taken_at'])
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
                        SELECT m.*, 
                        (SELECT version FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'max_version',
                        (SELECT removed FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'is_removed'
                        FROM `messages` m
                        WHERE m.`entity_id` = ? OR m.`entity_id` = ?
                        ORDER BY m.`taken_at` ASC
                    """,
                    [str(user_id), str(self.tg_client.me_user_id)]
                )
            else:
                date_from = StatusController.datetime_to_str(date_from, '%Y-%m-%d')
                res = self.db_conn.execute(
                    """
                        SELECT m.*, 
                        (SELECT version FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'max_version',
                        (SELECT removed FROM `messages` m1 WHERE m1.`entity_id` = m.`entity_id` AND m1.message_id = m.message_id AND m1.from_id = m.from_id ORDER BY version DESC LIMIT 1) as 'is_removed'
                        FROM `messages` m
                        WHERE (m.`entity_id` = ? OR m.`entity_id` = ?) AND (m.`taken_at` > ?)
                        ORDER BY m.`taken_at` ASC
                    """,
                    [str(user_id), str(self.tg_client.me_user_id), date_from]
                )
            rows = list(res.fetchall())

            me_name = self.tg_client.me_user_entity_name
            another_name = await self.tg_client.get_entity_name(user_id, 'User')
            dialog_name = me_name + ' <-> ' + another_name

            results.append('**Диалог '+dialog_name+' ('+title+')'+':**')
            results.append('')
            results.append('Сообщений диалога в БД: ' + str(len(rows)))
            if len(rows) > 0:
                date_start = StatusController.datetime_from_str(rows[0]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                if not only_last_dialog:
                    results.append('Самое раннее сообщение диалога в БД: ' + StatusController.datetime_to_str(date_start))
                if len(rows) > 1:
                    date_end = StatusController.datetime_from_str(rows[len(rows) - 1]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                    seconds_count = (date_end - date_start).total_seconds()
                    days_count = seconds_count / (24 * 60 * 60)
                    messages_count = len(rows)
                    if (not date_from) and (not only_last_dialog):
                        results.append('Длительность общения: {0:0.2f} суток'.format(days_count))
                    if not only_last_dialog:
                        results.append('Средняя частота сообщений: {0:0.2f} в сутки'.format(messages_count / days_count))

                    max_dialog_question_interval = round((24 * 60 * 60) * 1.25)
                    max_dialog_non_question_interval = round((24 * 60 * 60) * 0.75)
                    max_dialog_hello_as_second_message_offset = round((24 * 60 * 60) * 0.25)
                    dialog_hello_words = ['привет', 'приветствую', 'здравствуй', 'здравствуйте']
                    dialog_hello_phrases = ['доброе утро', 'доброго утра', 'добрый день', 'доброго дня', 'добрый вечер', 'доброго вечера']
                    dialog_hello_stop_context = ['-привет', 'всем привет', 'привет»', 'привет"']

                    msg_len_me = 0
                    msg_me_cnt = 0
                    msg_me_max_len = 0
                    msg_len_another = 0
                    msg_another_cnt = 0
                    msg_another_max_len = 0

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

                    edited_messages_me = {}
                    edited_messages_another = {}
                    edited_sequence_interrupted = {}

                    last_message_row = None

                    for row in rows:
                        if not row['message']:
                            row['message'] = ''
                        if int(row['removed']) == 1 or int(row['is_removed']) == 1:
                            if int(row['version']) == int(row['max_version']):
                                if int(row['from_id']) == self.tg_client.me_user_id:
                                    me_deletes = me_deletes + 1
                                else:
                                    another_deletes = another_deletes + 1
                        else:
                            if row['message_id'] not in edited_sequence_interrupted:
                                for k_int in edited_sequence_interrupted.keys():
                                    if type(edited_sequence_interrupted[k_int]) == dict:
                                        if edited_sequence_interrupted[k_int]['from_id'] != row['from_id']:
                                            edited_sequence_interrupted[k_int]['interrupts'].append(row['message_id'])

                            if int(row['max_version']) > 1:
                                if int(row['version']) < int(row['max_version']):
                                    if row['message_id'] not in edited_sequence_interrupted:
                                        edited_sequence_interrupted[row['message_id']] = {
                                            'from_id': row['from_id'],
                                            'interrupts': []
                                        }
                                else:
                                    if row['message_id'] in edited_sequence_interrupted:
                                        edited_sequence_interrupted[row['message_id']] = len(edited_sequence_interrupted[row['message_id']]['interrupts']) > 0

                            if last_message_row and (int(last_message_row['version']) < int(last_message_row['max_version'])) and (row['from_id'] != last_message_row['from_id']):
                                edited_sequence_interrupted[row['message_id']] = True
                            if int(row['version']) == int(row['max_version']):
                                last_message_row = row

                            if int(row['max_version']) > 1:
                                if int(row['from_id']) == self.tg_client.me_user_id:
                                    if row['message_id'] not in edited_messages_me:
                                        edited_messages_me[row['message_id']] = []
                                    edited_messages_me[row['message_id']].append(row)
                                else:
                                    if row['message_id'] not in edited_messages_another:
                                        edited_messages_another[row['message_id']] = []
                                    edited_messages_another[row['message_id']].append(row)
                            if int(row['version']) == int(row['max_version']):
                                message_lower = str(row['message']).lower()
                                message_words = re.sub("[^\w]", " ", message_lower).split()
                                message_words = list(filter(lambda x: x and self.is_valid_word(x, []), message_words))

                                message_hello_words = list(filter(lambda x: x in dialog_hello_words, message_words))
                                message_stop_contexts = list(filter(lambda x: message_lower.find(x) >= 0, dialog_hello_stop_context))

                                msg_from_id = int(row['from_id'])
                                msg_is_question = str(row['message']).find('?') >= 0
                                msg_is_hello = (len(message_hello_words) > 0) and (len(message_stop_contexts) == 0)

                                if not msg_is_hello:
                                    for d_ph in dialog_hello_phrases:
                                        if message_lower.find(d_ph) >= 0:
                                            msg_is_hello = True
                                            break

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
                                msg_date = StatusController.datetime_from_str(row['taken_at'], '%Y-%m-%d %H:%M:%S%z')
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

                    for k_int in edited_sequence_interrupted.keys():
                        if type(edited_sequence_interrupted[k_int]) == dict:
                            edited_sequence_interrupted[k_int] = len(edited_sequence_interrupted[k_int]['interrupts']) > 0

                    me_edit_stats = self.get_edit_stats(edited_messages_me, edited_sequence_interrupted)
                    another_edit_stats = self.get_edit_stats(edited_messages_another, edited_sequence_interrupted)

                    if len(active_dialog) > 0:
                        dialogues.append(active_dialog)
                        active_dialog = []

                    if only_last_dialog:
                        dialogues = [dialogues[len(dialogues) - 1]]
                    else:
                        if len(dialogues) > 0:
                            last_dialogue_date = StatusController.datetime_from_str(dialogues[len(dialogues) - 1][0]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
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
                                curr_first_dia_begin = StatusController.datetime_from_str(first_dial['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                                dia_between_seconds_curr = (curr_first_dia_begin - last_dia_end).total_seconds()
                                dia_between_seconds = dia_between_seconds + dia_between_seconds_curr
                                dia_between_cnt = dia_between_cnt + 1
                                if dia_between_seconds_curr > dia_between_max:
                                    dia_between_max = dia_between_seconds_curr
                                    dia_between_max_from = last_dia_end
                                    dia_between_max_to = curr_first_dia_begin
                            last_dia_end = StatusController.datetime_from_str(last_dial['taken_at'], '%Y-%m-%d %H:%M:%S%z')

                        last_msg_id = None
                        last_msg_date = None
                        for dia in dial:
                            msg_date = StatusController.datetime_from_str(dia['taken_at'], '%Y-%m-%d %H:%M:%S%z')
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
                        dia_between_time = "{0:0.2f} сут.".format(dia_between_time / (60 * 60 * 24))
                        dia_between_time_max = "{0:0.2f} сут.".format(dia_between_max / (60 * 60 * 24))
                        dia_between_time_max = StatusController.datetime_to_str(dia_between_max_from) + ' --- ' + StatusController.datetime_to_str(dia_between_max_to) + ' ('+dia_between_time_max+')'
                    else:
                        dia_between_time = '?'
                        dia_between_time_max = '?'

                    if answers_another > 0:
                        answers_wait_seconds_another = answers_wait_seconds_another / answers_another
                        another_answer_time = "{0:0.2f} мин.".format(answers_wait_seconds_another / 60)
                    else:
                        another_answer_time = '?'

                    if answers_me > 0:
                        answers_wait_seconds_me = answers_wait_seconds_me / answers_me
                        me_answer_time = "{0:0.2f} мин.".format(answers_wait_seconds_me / 60)
                    else:
                        me_answer_time = '?'

                    if not date_from:
                        self.tg_client.entity_controller.set_entity_answer_sec(user_id, answers_wait_seconds_me, answers_wait_seconds_another)

                    longest_dates = ''
                    longest_hours = 0
                    if longest_len > 1:
                        msg_date1 = StatusController.datetime_from_str(longest_dialog[0]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                        msg_date2 = StatusController.datetime_from_str(longest_dialog[longest_len - 1]['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                        longest_hours = (msg_date2 - msg_date1).total_seconds() / (24 * 60 * 60)
                        longest_dates = StatusController.datetime_to_str(msg_date1) + ' --- ' + StatusController.datetime_to_str(msg_date2)

                    valid_word_types = ['СУЩ', 'МЕЖД']
                    valid_word_types_str = (",".join(valid_word_types)).lower()

                    me_top_10 = None
                    me_last_cnt = None
                    me_words_count = 0
                    all_words_me = {}
                    if (not skip_vocab) and (len(me_words) > 0):
                        for word in me_words:
                            if word and (word not in all_words_me):
                                all_words_me[word] = True
                        me_words_count = len(all_words_me)

                        me_words = list(filter(lambda x: x and self.is_valid_word(x, valid_word_types), me_words))
                        wordlist = sorted(me_words)
                        wordfreq = [wordlist.count(p) for p in wordlist]
                        dic = dict(zip(wordlist, wordfreq))
                        me_words = list(sorted(dic.items(), key=lambda x: x[1], reverse=True))
                        words = me_words
                        half_words = round(len(words) / 2)
                        if half_words < 3:
                            half_words = 3
                        elif half_words > 15:
                            half_words = 15
                        top_10 = list(filter(lambda x: x[1] > 1, words[0:half_words]))
                        top_10 = list(map(lambda x: '**' + str(x[0]) + '** (' + str(x[1]) + ')', top_10))
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
                    all_words_another = {}
                    if (not skip_vocab) and (len(another_words) > 0):
                        for word in another_words:
                            if word and (word not in all_words_another):
                                all_words_another[word] = True
                        another_words_count = len(all_words_another)

                        another_words = list(filter(lambda x: x and self.is_valid_word(x, valid_word_types), another_words))
                        wordlist = sorted(another_words)
                        wordfreq = [wordlist.count(p) for p in wordlist]
                        dic = dict(zip(wordlist, wordfreq))
                        another_words = list(sorted(dic.items(), key=lambda x: x[1], reverse=True))
                        words = another_words
                        half_words = round(len(words) / 2)
                        if half_words < 3:
                            half_words = 3
                        elif half_words > 15:
                            half_words = 15
                        top_10 = list(filter(lambda x: x[1] > 1, words[0:half_words]))
                        top_10 = list(map(lambda x: '**' + str(x[0]) + '** (' + str(x[1]) + ')', top_10))
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

                    me_not_another_words_count = 0
                    me_not_another_top = None

                    another_not_me_words_count = 0
                    another_not_me_top = None

                    if (not skip_vocab) and (len(me_words) > 0) and (len(another_words) > 0):
                        me_not_another_top = []
                        for word in me_words:
                            if word[0] not in all_words_another:
                                me_not_another_top.append(word)

                        me_not_another_words_count = len(me_not_another_top)
                        me_not_another_top = me_not_another_top[:15]
                        me_not_another_top = list(map(lambda x: '**' + str(x[0]) + '** (' + str(x[1]) + ')', me_not_another_top))

                        another_not_me_top = []
                        for word in another_words:
                            if word[0] not in all_words_me:
                                another_not_me_top.append(word)

                        another_not_me_words_count = len(another_not_me_top)
                        another_not_me_top = another_not_me_top[:15]
                        another_not_me_top = list(map(lambda x: '**' + str(x[0]) + '** (' + str(x[1]) + ')', another_not_me_top))

                    results.append('Сообщений '+another_name+': {0} ({1:0.3f} Kb.)'.format(msg_another_cnt, msg_len_another/1024))
                    results.append('Сообщений '+me_name+': {0} ({1:0.3f} Kb.)'.format(msg_me_cnt, msg_len_me/1024))
                    if msg_another_cnt > 0:
                        results.append('Средняя длина сообщения '+another_name+': {0:0.2f} сим.'.format(msg_len_another / msg_another_cnt))
                    if msg_me_cnt > 0:
                        results.append('Средняя длина сообщения '+me_name+': {0:0.2f} сим.'.format(msg_len_me / msg_me_cnt))
                    results.append('Самое длинное сообщение '+another_name+': ' + str(msg_another_max_len) + ' сим.')
                    results.append('Самое длинное сообщение '+me_name+': ' + str(msg_me_max_len) + ' сим.')
                    results.append('Число приветствий от '+another_name+': ' + str(another_hello))
                    results.append('Число приветствий от '+me_name+': ' + str(me_hello))

                    if not only_last_dialog:
                        results.append('')
                        results.append('Число диалогов: ' + str(len(dialogues)))
                        results.append('Сообщений в самом коротком диалоге: ' + str(shortest_len))
                        results.append('Сообщений в самом длинном диалоге: ' + str(longest_len))
                        results.append('Самый длинный диалог: ' + longest_dates + ' ({0:0.3f} сут)'.format(longest_hours))
                        if len(dialogues) > 0:
                            if dia_between_time != '?':
                                results.append('Среднее время между диалогами: ' + str(dia_between_time))
                            if dia_between_time_max != '?':
                                results.append('Самое большое время между диалогами: ' + str(dia_between_time_max))
                            results.append('Инициатор диалога ' + another_name + ': {0:0.2f} %'.format(100 * dia_another_start / len(dialogues)))
                            results.append('Инициатор диалога ' + me_name + ': {0:0.2f} %'.format(100 * dia_me_start / len(dialogues)))
                            results.append('Завершитель диалога ' + another_name + ': {0:0.2f} %'.format(100 * dia_another_finish / len(dialogues)))
                            results.append('Завершитель диалога ' + me_name + ': {0:0.2f} %'.format(100 * dia_me_finish / len(dialogues)))
                    else:
                        results.append('Сообщений в диалоге: ' + str(longest_len))
                        results.append('Продолжительность диалога: ' + longest_dates + ' ({0:0.3f} сут)'.format(longest_hours))
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
                    if me_edit_stats['edited_messages_count'] > 0 or another_edit_stats['edited_messages_count'] > 0:
                        if (not date_from) and (not only_last_dialog):
                            results.append('За время активности скрипта ('+str(days_a)+' сут.):')
                        results.append('Отредактировано сообщений {}: {}'.format(another_name, another_edit_stats['edited_messages_count']))
                        results.append('Отредактировано сообщений {}: {}'.format(me_name, me_edit_stats['edited_messages_count']))
                        results.append('Отредактировано сообщений {} после ответа на него: {}'.format(another_name, another_edit_stats['sequence_interrupted_cnt']))
                        results.append('Отредактировано сообщений {} после ответа на него: {}'.format(me_name, me_edit_stats['sequence_interrupted_cnt']))
                        results.append('Процент редактируемых сообщений {0}: {1:0.2f}%'.format(another_name, 100 * another_edit_stats['edited_messages_count'] / msg_another_cnt))
                        results.append('Процент редактируемых сообщений {0}: {1:0.2f}%'.format(me_name, 100 * me_edit_stats['edited_messages_count'] / msg_me_cnt))
                        results.append('Макс. число правок одного сообщения {}: {} ("{}")'.format(another_name, another_edit_stats['max_1_message_edits'], self.cut_text(another_edit_stats['max_edits_message'])))
                        results.append('Макс. число правок одного сообщения {}: {} ("{}")'.format(me_name, me_edit_stats['max_1_message_edits'], self.cut_text(me_edit_stats['max_edits_message'])))
                        results.append('Макс. процент правок одного сообщения {0}: {1:0.2f}% ("{2}")'.format(another_name, another_edit_stats['max_1_message_diff_percent'], self.cut_text(another_edit_stats['max_changed_message'])))
                        results.append('Макс. процент правок одного сообщения {0}: {1:0.2f}% ("{2}")'.format(me_name, me_edit_stats['max_1_message_diff_percent'], self.cut_text(me_edit_stats['max_changed_message'])))
                        results.append('')
                        results.append('В среднем правок на 1 сообщение {0}: {1:0.2f}'.format(another_name, another_edit_stats['mid_1_message_edits']))
                        results.append('В среднем правок на 1 сообщение {0}: {1:0.2f}'.format(me_name, me_edit_stats['mid_1_message_edits']))
                        results.append('Средний суммарный процент изменений редактируемых сообщений {0}: {1:0.3f}%'.format(another_name, another_edit_stats['message_edit_mid_summ_percent']))
                        results.append('Средний суммарный процент изменений редактируемых сообщений {0}: {1:0.3f}%'.format(me_name, me_edit_stats['message_edit_mid_summ_percent']))
                        results.append('Среднее время между правками одного сообщения {0}: {1:0.2f} мин.'.format(another_name, another_edit_stats['message_edit_mid_time_sec'] / 60))
                        results.append('Среднее время между правками одного сообщения {0}: {1:0.2f} мин.'.format(me_name, me_edit_stats['message_edit_mid_time_sec'] / 60))
                        results.append('')
                        results.append('Средняя правка (число замен / вставок / удалений) {0}: {1:0.2f} / {2:0.2f} / {3:0.2f}'.format(another_name, another_edit_stats['replaces_count_avg'], another_edit_stats['inserts_count_avg'], another_edit_stats['deletes_count_avg']))
                        results.append('Средняя правка (число замен / вставок / удалений) {0}: {1:0.2f} / {2:0.2f} / {3:0.2f}'.format(me_name, me_edit_stats['replaces_count_avg'], me_edit_stats['inserts_count_avg'], me_edit_stats['deletes_count_avg']))
                        results.append('')
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
                        if me_not_another_words_count > 0:
                            results.append('')
                            results.append('Слова ' + me_name + ' ('+valid_word_types_str+'), которые ' + another_name + ' ни разу не использовал: **' + str(me_not_another_words_count) +'** шт. Самые частые: ' + (", ".join(me_not_another_top)) + '')
                        if another_not_me_words_count > 0:
                            results.append('')
                            results.append('Слова ' + another_name + ' ('+valid_word_types_str+'), которые ' + me_name + ' ни разу не использовал: **' + str(another_not_me_words_count) +'** шт. Самые частые: ' + (", ".join(another_not_me_top)) + '')

        CacheHelper().save_to_cache('normal_forms', 'dialog_stats', self.normal_form_cache)
        CacheHelper().save_to_cache('word_type_forms', 'dialog_stats', self.word_type_form_cache)

        return {
            'results': results,
            'last_dialogue_date': last_dialogue_date
        }
