import traceback
import re
from datetime import datetime, timedelta

from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import User, PeerUser
from telethon.tl.types.messages import DialogsSlice

from bot_action_branch import BotActionBranch
from helper_functions import MainHelper
from status_controller import StatusController


class AutoAnswers(BotActionBranch):

    def __init__(self, tg_bot_controller, branch_parent, branch_code=None):
        super().__init__(tg_bot_controller, branch_parent, branch_code)

        self.use_timer = True

        self.setup_step = 0
        self.setup_user_id = None
        self.active_entity_client = None
        self.active_dialog_entity = None
        self.aa_for_users = {}
        self.aa_not_for_users = []
        self.aa_options = {
            'is_set': False,
            'from_mode': 0,
            'from_user_ids': [],
            'activate_after_minutes': 100000.0,
            'answer_after_minutes': 100000.0,
            'show_bot': True,
            'allow_bot_chat': False,
            'notify_entity_client': None,
            'notify_entity_dialog': None,
            'message': ''
        }

        self.max_commands = 4
        self.commands.update({
            '/auto_start': {
                'cmd': self.cmd_auto_start,
                'condition': self.is_not_active,
                'bot_button': {
                    'title': 'Настроить и запустить',
                    'position': [0, 0],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'начать настройку автоответчика'
            },
            '/auto_off': {
                'cmd': self.cmd_auto_off,
                'condition': self.is_active,
                'bot_button': {
                    'title': 'Отключить',
                    'position': [0, 0],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'отключить автоответчик'
            },
            '/auto_restart': {
                'cmd': self.cmd_auto_start,
                'condition': self.is_active,
                'bot_button': {
                    'title': 'Перенастроить',
                    'position': [0, 1],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'начать настройку повторно'
            },
            '/auto_set_time': {
                'cmd': self.cmd_auto_set_time,
                'condition': self.is_active,
                'bot_button': {
                    'title': 'Указать дату/время',
                    'position': [1, 0],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'указать дату/время прибытия заменой сообщения'
            },
            '/auto_reset_users': {
                'cmd': self.cmd_auto_reset_users,
                'condition': self.has_users_setup,
                'bot_button': {
                    'title': 'Сброс пользователей',
                    'position': [2, 0],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'сбросить флаги ответа пользователям'
            },
        })
        self.on_init_finish()

    def is_active(self, user_id):
        return self.aa_options['is_set']

    def is_not_active(self, user_id):
        return not self.aa_options['is_set']

    def has_users_setup(self, user_id):
        return self.aa_options['is_set'] and (len(self.aa_for_users) > 0)

    def reset_aa(self):
        self.aa_options['is_set'] = False
        self.setup_step = 0
        self.aa_for_users = {}
        self.aa_not_for_users = []

    def reset_user_setup(self):
        self.setup_step = 0
        self.setup_user_id = None
        self.active_entity_client = None
        self.active_dialog_entity = None

    async def start_user_setup(self, from_id):
        message_client = self.tg_bot_controller.get_user_message_client(from_id)
        dialog_entity = self.tg_bot_controller.get_user_dialog_entity(from_id)
        if not message_client or not dialog_entity:
            return False
        if (self.setup_user_id is not None) and self.active_entity_client and self.active_dialog_entity:
            str_user_name_old = await self.tg_bot_controller.tg_client.get_entity_name(self.setup_user_id, 'User')
            str_user_name = await self.tg_bot_controller.tg_client.get_entity_name(from_id, 'User')
            await self.active_entity_client.send_message(self.active_dialog_entity, 'Процесс настройки прерван ' + str_user_name)
            if self.setup_user_id != from_id:
                await self.return_to_main_branch(self.setup_user_id)
                await message_client.send_message(dialog_entity, 'Прервали настройку ' + str_user_name_old)
        self.active_entity_client = message_client
        self.active_dialog_entity = dialog_entity
        self.setup_user_id = from_id
        return True

    async def show_current_branch_commands(self, from_id, pre_text=None):
        if self.aa_options['is_set']:
            msg = 'Автоответчик для ' + self.tg_bot_controller.tg_client.me_user_name + ' настроен и запущен!\n\nПараметры:\n'
            msg = msg + '```'
            for k, v in self.aa_options.items():
                if k not in ['is_set', 'notify_entity_client', 'notify_entity_dialog']:
                    msg = msg + k + ' = ' + str(v) + '\n'
            msg = msg + '```\n'
            msg = msg + 'Пользователи: '
            if len(self.aa_for_users) > 0:
                for user_id, user_date in self.aa_for_users.items():
                    msg = msg + '\n'
                    msg = msg + (await self.tg_bot_controller.user_link(user_id))
                    msg = msg + ' **---** '
                    if user_date:
                        msg = msg + 'Ждёт с ' + StatusController.datetime_to_str(user_date)
                    else:
                        msg = msg + 'Сообщение отправлено'
            else:
                msg = msg + 'Отсутствуют'
            msg = msg + '\n'
        else:
            msg = ''
        msg_text = msg + "\n".join(await self.get_commands_description_list(from_id, pre_text))
        buttons = await self.get_commands_buttons_list(from_id)
        await self.send_message_to_user(from_id, msg_text.strip(), buttons=buttons, set_active=True)

    async def cmd_auto_start(self, from_id, params):
        if not await self.start_user_setup(from_id):
            return
        await self.active_entity_client.send_message(self.active_dialog_entity, 'Настраиваем автоответчик для ' + self.tg_bot_controller.tg_client.me_user_name)
        self.reset_aa()
        await self.next_setup_step()

    async def next_setup_step(self, next_step=None, from_user_id=None):
        if not from_user_id:
            from_user_id = self.setup_user_id
        if next_step is not None:
            self.setup_step = next_step
        else:
            self.setup_step = self.setup_step + 1
        if self.setup_step == 1:
            msg = '**Шаг 1**\n\n'
            msg = msg + 'Для сообщений от кого должен срабатывать автоответчик?\n\n'
            msg = msg + '  0 - ото всех\n'
            msg = msg + '  1 - от моих контактов\n'
            msg = msg + '  2 - от моих взаимных контактов\n'
            msg = msg + '  3 - от выбранных в общем списке пользователей\n'
            msg = msg + '  4 - указать логины/ID\n'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 2:
            msg = '**Шаг 1.1**\n\n'
            msg = msg + 'Перечислите логины/ID через запятую'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 3:
            msg = '**Шаг 2**\n\n'
            msg = msg + 'Сколько минут ' + self.tg_bot_controller.tg_client.me_user_name + ' должен быть не в сети для срабатывания автоответчика?'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 4:
            msg = '**Шаг 3**\n\n'
            msg = msg + 'Через сколько минут после сообщения автоответчик сработает?'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 5:
            msg = '**Шаг 4**\n\n'
            msg = msg + 'Показывать, что отвечает бот?'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 6:
            msg = '**Шаг 5**\n\n'
            msg = msg + 'После сообщения позволить боту продолжить диалог?'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 7:
            msg = '**Шаг 6**\n\n'
            msg = msg + 'Ну и наконец, введите текст сообщения.\n'
            msg = msg + 'Тире или 0 - использовать стандартное сообщение.\n'
            msg = msg + 'Время в формате 21:55 - использовать стандартное с уведомлением о времени прибытия.\n'
            msg = msg + 'Кроме того в тексте можно использовать следующие коды:\n'
            msg = msg + '[username] - подстановка имени пользователя для которого настраивается автоответчик.\n'
            msg = msg + '[statistics] - статистика активности и прогноз времени прибытия.\n'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 8:
            msg = '**Настройка завершена!!!**\n\n'
            self.aa_options['notify_entity_client'] = self.active_entity_client
            self.aa_options['notify_entity_dialog'] = self.active_dialog_entity
            self.reset_aa()
            self.reset_user_setup()
            self.aa_options['is_set'] = True
            await self.send_message_to_user(from_user_id, msg.strip())
        elif self.setup_step == 200:
            msg = '**Настройка даты/времени прибытия**\n\n'
            msg = msg + 'оно будет указано ботом в начальном сообщении вида:\n'
            msg = msg + '```'+MainHelper().get_config_value('chat_bot', 'bot_aa_default_message_time')+'```\n'
            msg = msg + 'Введите дату/время'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())

    async def on_bot_message(self, message, from_id):
        if self.setup_step == 0:
            return await super().on_bot_message(message, from_id)
        if (not self.is_in_current_branch(from_id)) or (self.setup_user_id != from_id):
            return False
        message = str(message).strip()
        if self.setup_step == 1:
            num_msg = int(message)
            if (num_msg >= 0) and (num_msg <= 4):
                self.aa_options['from_mode'] = num_msg
                self.aa_options['from_user_ids'] = []
                if num_msg == 4:
                    await self.next_setup_step(2)
                else:
                    await self.next_setup_step(3)
            else:
                await self.next_setup_step(1)
        elif self.setup_step == 2:
            self.aa_options['from_user_ids'] = []
            logins = message.lower().split(',')
            for login_s in logins:
                login = str(login_s).strip()
                entity = None
                try:
                    entity = await self.tg_bot_controller.tg_client.get_entity(login)
                except ValueError:
                    try:
                        entity = await self.tg_bot_controller.tg_client.get_entity(PeerUser(int(login)))
                    except:
                        pass
                if entity and (type(entity) == User):
                    if entity.id not in self.aa_options['from_user_ids']:
                        self.aa_options['from_user_ids'].append(entity.id)
                else:
                    await self.active_entity_client.send_message(self.active_dialog_entity, 'Сущность не опознана - ' + str(login))
            if len(self.aa_options['from_user_ids']) > 0:
                await self.next_setup_step(3)
            else:
                await self.next_setup_step(2)
        elif self.setup_step == 3:
            num_msg = float(message)
            if num_msg < 0.01:
                num_msg = 0.0
            elif num_msg > 999999999999:
                num_msg = 999999999999
            self.aa_options['activate_after_minutes'] = num_msg
            await self.next_setup_step()
        elif self.setup_step == 4:
            num_msg = float(message)
            if num_msg < 0.01:
                num_msg = 0.0
            elif num_msg > 999999999999:
                num_msg = 999999999999
            self.aa_options['answer_after_minutes'] = num_msg
            await self.next_setup_step()
        elif self.setup_step == 5:
            message = message.lower()
            self.aa_options['show_bot'] = message in self.yes_variants
            await self.next_setup_step()
        elif self.setup_step == 6:
            message = message.lower()
            self.aa_options['allow_bot_chat'] = message in self.yes_variants
            await self.next_setup_step()
        elif self.setup_step == 7:
            if not message or (message in self.no_variants):
                message = str(MainHelper().get_config_value('chat_bot', 'bot_aa_default_message'))
            elif re.match(r"[0-9]{1,2}:[0-9]{1,2}", message):
                d_msg = message
                message = str(MainHelper().get_config_value('chat_bot', 'bot_aa_default_message_time'))
                message = message.replace('[datetime]', d_msg)
            self.aa_options['message'] = message
            await self.next_setup_step()
        elif self.setup_step == 200:
            self.aa_options['message'] = str(MainHelper().get_config_value('chat_bot', 'bot_aa_default_message_time'))
            self.aa_options['message'] = self.aa_options['message'].replace('[datetime]', message)
            self.reset_user_setup()
            await self.send_message_to_user(from_id, 'Сообщение изменено!')
        return True

    async def send_message(self, to_id):
        message_text = self.aa_options['message']
        message = message_text.replace('[username]', self.tg_bot_controller.tg_client.me_user_name)

        if message.find('[statistics]') >= 0:
            stats_str = await self.tg_bot_controller.tg_client.status_controller.get_user_aa_statistics_text(self.tg_bot_controller.tg_client.me_user_id)
            message = message.replace('[statistics]', '\n' + stats_str)

        if self.aa_options['show_bot']:
            message = self.tg_bot_controller.text_to_bot_text(message, to_id, "dialog")
        else:
            message = self.tg_bot_controller.text_to_bot_text(message, to_id, "bot")
        to_username = await self.tg_bot_controller.tg_client.get_entity_name(to_id, 'User')
        msg_log = StatusController.datetime_to_str(datetime.now()) + ' Sending AA message to user "'+to_username+'"'
        msg_log = msg_log + '\n' + '<<< ' + message
        print(msg_log)
        await self.tg_bot_controller.tg_client.send_message(PeerUser(to_id), message)
        if self.aa_options['notify_entity_client'] and self.aa_options['notify_entity_dialog']:
            msg_log = StatusController.datetime_to_str(datetime.now()) + ' Отправляем сообщение пользователю ' + (await self.tg_bot_controller.user_link(to_id, to_username))
            msg_log = msg_log + '\n' + '``` ' + message + '```'
            await self.aa_options['notify_entity_client'].send_message(self.aa_options['notify_entity_dialog'], msg_log)

    async def on_user_message_to_me(self, from_id, message_text):
        if not self.aa_options['is_set']:
            return
        if from_id in self.aa_not_for_users:
            return
        if (from_id in self.aa_for_users) and self.aa_for_users[from_id]:
            return
        try:
            entity = await self.tg_bot_controller.tg_client.get_entity(PeerUser(int(from_id)))
        except:
            return
        if (not entity) or (type(entity) != User):
            return
        user_level = self.tg_bot_controller.get_entity_rights_level(entity, self.aa_options['from_user_ids'])
        if user_level < self.aa_options['from_mode']:
            if from_id not in self.aa_not_for_users:
                self.aa_not_for_users.append(from_id)
            return
        self.aa_for_users[from_id] = StatusController.now_local_datetime()
        check_user_name = await self.tg_bot_controller.tg_client.get_entity_name(from_id, 'User')
        print(StatusController.datetime_to_str(datetime.now()) + ' Adding AA schedule for user "' + check_user_name + '"')
        if self.aa_options['answer_after_minutes'] <= 0.05:
            await self.do_on_timer([from_id])

    async def on_timer(self):
        await self.do_on_timer()

    async def do_on_timer(self, check_ids=None):
        try:
            if not self.aa_options['is_set']:
                return
            if ((datetime.now() - self.tg_bot_controller.tg_client.me_last_activity).total_seconds() / 60.0) < self.aa_options['activate_after_minutes']:
                return
            if not check_ids:
                check_ids = list(self.aa_for_users.keys())
            dialogs = None
            for check_id in check_ids:
                if (check_id in self.aa_for_users) and self.aa_for_users[check_id]:
                    if ((StatusController.now_local_datetime() - self.aa_for_users[check_id]).total_seconds() / 60.0) >= self.aa_options['answer_after_minutes']:

                        if not dialogs:
                            input_peer = await self.tg_bot_controller.tg_client.get_input_entity(PeerUser(check_id))
                            dialogs = await self.tg_bot_controller.tg_client(GetDialogsRequest(
                                limit=0,
                                offset_date=None,
                                offset_id=0,
                                offset_peer=input_peer,
                                hash=0,
                                folder_id=0
                            ))
                            if type(dialogs) == DialogsSlice:
                                dialogs = dialogs.dialogs

                        c_dialog = None
                        if dialogs:
                            for dialog in dialogs:
                                if (type(dialog.peer) == PeerUser) and (dialog.peer.user_id == check_id) and (dialog.read_inbox_max_id > 0):
                                    c_dialog = dialog
                                    break

                        do_remove_aa_user_record = False
                        if (not c_dialog) or (c_dialog.unread_count > 0):
                            unread_cnt_not_me = 0
                            t_messages = await self.tg_bot_controller.tg_client.get_messages(PeerUser(check_id), limit=10)
                            for t_message in t_messages:
                                message_date = StatusController.tg_datetime_to_local_datetime(t_message.date)
                                user_date = self.aa_for_users[check_id]
                                if t_message.from_id == self.tg_bot_controller.tg_client.me_user_id:
                                    if (message_date > user_date) and ((message_date - user_date).total_seconds() > 1):
                                        do_remove_aa_user_record = True
                                        break
                                elif c_dialog and (t_message.id > c_dialog.read_inbox_max_id):
                                    unread_cnt_not_me = unread_cnt_not_me + 1
                            if c_dialog and (unread_cnt_not_me == 0):
                                do_remove_aa_user_record = True
                        elif c_dialog and (c_dialog.unread_count == 0):
                            do_remove_aa_user_record = True

                        if do_remove_aa_user_record:
                            del self.aa_for_users[check_id]
                            check_user_name = await self.tg_bot_controller.tg_client.get_entity_name(check_id, 'User')
                            print(StatusController.datetime_to_str(datetime.now()) + ' Removing AA schedule for user "'+check_user_name+'"')
                            continue

                        self.aa_for_users[check_id] = None
                        self.aa_not_for_users.append(check_id)
                        await self.send_message(check_id)
                        if self.aa_options['allow_bot_chat']:
                            await self.tg_bot_controller.init_chat_for_user(check_id, check_id, False, self.aa_options['show_bot'])
        except:
            traceback.print_exc()

    async def on_me_write_message_to_user(self, to_user_id):
        if not self.aa_options['is_set']:
            return
        if (to_user_id in self.aa_for_users) and self.aa_for_users[to_user_id]:
            del self.aa_for_users[to_user_id]
            check_user_name = await self.tg_bot_controller.tg_client.get_entity_name(to_user_id, 'User')
            print(StatusController.datetime_to_str(datetime.now()) + ' Removing AA schedule for user "' + check_user_name + '"')

    async def force_add_user(self, user_id, message=None):
        if not self.aa_options['is_set']:
            self.tg_bot_controller.tg_client.me_last_activity = datetime.now()  + timedelta(minutes=-10)
            if not message:
                message = str(MainHelper().get_config_value('chat_bot', 'bot_aa_default_message'))
            self.reset_aa()
            self.reset_user_setup()
            self.aa_options = {
                'is_set': True,
                'from_mode': 4,
                'from_user_ids': [],
                'activate_after_minutes': 10.0,
                'answer_after_minutes': 2.0,
                'show_bot': True,
                'allow_bot_chat': True,
                'notify_entity_client': None,
                'notify_entity_dialog': None,
                'message': ''
            }
        self.aa_options['from_mode'] = 4
        if user_id not in self.aa_options['from_user_ids']:
            self.aa_options['from_user_ids'].append(user_id)
        if user_id in self.aa_not_for_users:
            self.aa_not_for_users.remove(user_id)
        if user_id in self.aa_for_users:
            del self.aa_for_users[user_id]
        if message:
            self.aa_options['message'] = message
        await self.on_user_message_to_me(user_id, '---')

    async def cmd_auto_off(self, from_id, params):
        self.reset_aa()
        self.reset_user_setup()
        await self.send_message_to_user(from_id, 'Автоответчик отключен!')
        await self.return_to_main_branch(from_id)

    async def cmd_auto_reset_users(self, from_id, params):
        if not await self.start_user_setup(from_id):
            return
        self.aa_for_users = {}
        self.aa_not_for_users = []
        self.reset_user_setup()
        await self.show_current_branch_commands(from_id)

    async def cmd_auto_set_time(self, from_id, params):
        if not await self.start_user_setup(from_id):
            return
        await self.next_setup_step(200, from_id)
