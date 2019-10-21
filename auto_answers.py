from datetime import datetime

from telethon.tl.types import User, PeerUser

from bot_controller import BotController
from status_controller import StatusController


class AutoAnswers:

    def __init__(self, tg_bot_controller:BotController):
        self.tg_bot_controller = tg_bot_controller
        self.is_setup_mode = False
        self.setup_step = 0
        self.setup_user_id = None
        self.active_entity_client = None
        self.active_dialog_entity = None
        self.aa_for_users = {}
        self.aa_not_for_users = []
        self.aa_user_name = None
        self.aa_options = {
            'is_set': False,
            'from_mode': 0,
            'from_user_ids': [],
            'activate_after_minutes': 100000,
            'answer_after_minutes': 100000,
            'show_bot': True,
            'allow_bot_chat': False,
            'message': ''
        }

    async def begin_setup(self, from_id, active_entity_client, dialog_entity):
        if (self.setup_user_id is not None) and self.active_entity_client and self.active_dialog_entity:
            str_user_name = await self.tg_bot_controller.tg_client.get_entity_name(from_id, 'User')
            await self.active_entity_client.send_message(self.active_dialog_entity, 'Процесс настройки прерван ' + str_user_name)
            await active_entity_client.send_message(dialog_entity, 'Прервали настройку ' + str_user_name)
        self.active_entity_client = active_entity_client
        self.active_dialog_entity = dialog_entity
        self.aa_user_name = self.tg_bot_controller.tg_client.me_user_name
        await self.active_entity_client.send_message(dialog_entity, 'Настраиваем автоответчик для ' + self.aa_user_name)
        self.aa_options['is_set'] = False
        self.is_setup_mode = True
        self.setup_step = 0
        self.setup_user_id = from_id
        self.aa_for_users = {}
        self.aa_not_for_users = []
        await self.next_setup_step()

    async def next_setup_step(self, next_step=None):
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
            msg = msg + 'Сколько минут ' + self.aa_user_name + ' должен быть не в сети для срабатывания автоответчика?'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 4:
            msg = '**Шаг 3**\n\n'
            msg = msg + 'Через сколько минут после сообщения автоответчик сработает?'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 5:
            msg = '**Шаг 4**\n\n'
            msg = msg + 'Показывать, что отвечает бот? (да/нет/yes/no/1/0)'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 6:
            msg = '**Шаг 5**\n\n'
            msg = msg + 'После сообщения позволить боту продолжить диалог? (да/нет/yes/no/1/0)'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 7:
            msg = '**Шаг 6**\n\n'
            msg = msg + 'Ну и наконец, введите текст сообщения (тире или 0 чтобы использовать стандартное).'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
        elif self.setup_step == 8:
            msg = '**Настройка завершена!!!**\n\n'
            msg = msg + '```'
            for k, v in self.aa_options.items():
                if k != 'is_set':
                    msg = msg + k + ' = ' + str(v) + '\n'
            msg = msg + '```'
            await self.active_entity_client.send_message(self.active_dialog_entity, msg.strip())
            self.is_setup_mode = False
            self.setup_step = 0
            self.setup_user_id = None
            self.aa_options['is_set'] = True

    async def on_bot_message(self, message, from_id, dialog_entity):
        if (not self.is_setup_mode) or (self.setup_user_id != from_id) or (message == '/auto'):
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
            num_msg = int(message)
            if num_msg < 1:
                num_msg = 1
            elif num_msg > 999999999999:
                num_msg = 999999999999
            self.aa_options['activate_after_minutes'] = num_msg
            await self.next_setup_step()
        elif self.setup_step == 4:
            num_msg = int(message)
            if num_msg < 1:
                num_msg = 1
            elif num_msg > 999999999999:
                num_msg = 999999999999
            self.aa_options['answer_after_minutes'] = num_msg
            await self.next_setup_step()
        elif self.setup_step == 5:
            message = message.lower()
            self.aa_options['show_bot'] = message in ['да', 'yes', '1']
            await self.next_setup_step()
        elif self.setup_step == 6:
            message = message.lower()
            self.aa_options['allow_bot_chat'] = message in ['да', 'yes', '1']
            await self.next_setup_step()
        elif self.setup_step == 7:
            if not message or (message == '-') or (message == '0'):
                message = self.tg_bot_controller.tg_client.config['chat_bot']['bot_aa_default_message']
            self.aa_options['message'] = message

            await self.next_setup_step()
        return True

    async def send_message(self, to_id):
        message_text = self.aa_options['message']
        message = message_text.replace('[username]', self.aa_user_name)
        if self.aa_options['show_bot']:
            message = self.tg_bot_controller.text_to_bot_text(message)
        # await self.tg_bot_controller.tg_client.send_message(PeerUser(to_id), message)
        print('Sending AA message')
        print(datetime.now())
        print(PeerUser(to_id))
        print(message)

    async def on_user_message(self, from_id, message_text):
        if from_id in self.aa_not_for_users:
            print('AA not user! ' + str(from_id))
            return
        str_from_id = str(from_id)
        if (str_from_id in self.aa_for_users) and self.aa_for_users[str_from_id]:
            return
        try:
            entity = await self.tg_bot_controller.tg_client.get_entity(PeerUser(int(from_id)))
        except:
            return
        if (not entity) or (type(entity) != User):
            return
        user_level = self.tg_bot_controller.get_entity_rights_level(entity)
        if user_level < self.aa_options['from_mode']:
            if from_id not in self.aa_not_for_users:
                self.aa_not_for_users.append(from_id)
                print('AA not users:')
                print(self.aa_not_for_users)
            return
        self.aa_for_users[str_from_id] = datetime.now()
        print('AA users')
        print(self.aa_for_users)
        if self.aa_options['answer_after_minutes'] == 0:
            await self.on_timer([from_id])

    async def on_timer(self, check_ids=None):
        if not self.aa_options['is_set']:
            return
        if ((datetime.now() - self.tg_bot_controller.tg_client.me_last_activity).total_seconds() / 60) > self.aa_options['activate_after_minutes']:
            return
        if not check_ids:
            check_ids = self.aa_for_users.keys()
        for check_id in check_ids:
            str_check_id = str(check_id)
            if (str_check_id in self.aa_for_users) and self.aa_for_users[str_check_id]:
                if ((datetime.now() - self.aa_for_users[str_check_id]).total_seconds() / 60) >= self.aa_options['answer_after_minutes']:

                    t_messages = await self.tg_bot_controller.tg_client.get_messages(PeerUser(int(check_id)), limit=10)
                    for t_message in t_messages:
                        message_date = StatusController.tg_datetime_to_local_datetime(t_message.date)
                        if (t_message.from_id == self.tg_bot_controller.tg_client.me_user_id) and (message_date > self.aa_for_users[str_check_id]):
                            self.aa_for_users[str_check_id] = None
                            print('Removing AA schedule 2 ' + str_check_id)
                            continue

                    await self.send_message(int(check_id))
                    if self.aa_options['allow_bot_chat']:
                        self.tg_bot_controller.init_chat_for_user(int(check_id), self.aa_options['show_bot'])
                    self.aa_for_users[str_check_id] = None

    async def on_me_write_message_to_user(self, to_user_id):
        str_check_id = str(to_user_id)
        if (str_check_id in self.aa_for_users) and self.aa_for_users[str_check_id]:
            self.aa_for_users[str_check_id] = None
            print('Removing AA schedule 1 ' + str_check_id)
