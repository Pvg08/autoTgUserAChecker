import traceback
from datetime import datetime

from playsound import playsound
from telethon import TelegramClient, events
from telethon.events import NewMessage
from telethon.tl.types import UpdateNewMessage, PeerUser, UpdateBotCallbackQuery, Message

from bot_action_branch import BotActionBranch
from helper_functions import MainHelper
from status_controller import StatusController


class InteractiveTelegramBot(TelegramClient):

    def __init__(self, session_file, api_id, api_hash, cl_conn, proxy, parent_client):
        print('Initialization of bot')
        self.tg_client = parent_client
        self.bot_entity = None
        self.bot_entity_id = None
        super().__init__(session_file, api_id, api_hash, connection=cl_conn, proxy=proxy, sequential_updates=True)

    async def send_version_message_to_all_bot_users(self):
        sent_count = 0
        try:
            version_messages = self.get_version_messages()
            new_ver = MainHelper().get_config_float_value('main', 'actual_version')
            new_ver_title = MainHelper().get_config_value('main', 'new_version_title')
            new_ver_help = MainHelper().get_config_value('main', 'new_version_help')
            result_title = new_ver_title.replace('[actual_version]', "{0:0.2f}".format(new_ver)).replace('[bot_name]', MainHelper().get_config_value('tg_bot', 'bot_username'))
            result_title = result_title + '\n' + new_ver_help
            chats = self.tg_client.entity_controller.get_all_bot_users_chats()
            if chats and len(chats) > 0:
                for chat in chats:
                    user_last_version = self.tg_client.entity_controller.get_user_bot_last_version(chat.user_id)
                    if not user_last_version:
                        user_last_version = 0.0
                    user_last_version = float(user_last_version)
                    result_user_message=[]
                    for version_message in version_messages:
                        if version_message['version'] > user_last_version:
                            version_message_text = 'Список изменений версии {0:0.2f}:\n'.format(version_message['version'])
                            version_message_text = version_message_text + version_message['message']
                            result_user_message.append(version_message_text)
                    if len(result_user_message) > 0:
                        result_user_message = result_title + '\n--------\n' + ("\n--------\n".join(result_user_message)).strip()
                        self.tg_client.entity_controller.save_user_bot_last_version(chat.user_id, new_ver)
                        await self.send_message(chat, result_user_message)
                        sent_count = sent_count + 1
        except:
            traceback.print_exc()
        return sent_count

    @staticmethod
    def get_version_messages():
        text_file = open("changelog.txt", "r", encoding="UTF-8")
        lines = text_file.readlines()
        version_messages = []
        this_ver = 0.0
        this_ver_messages = []
        for line in lines:
            line = line.strip('\n')
            if line.startswith('===') and line.endswith('==='):
                if len(this_ver_messages) > 0:
                    message = ("\n".join(this_ver_messages)).strip()
                    message = message.replace('[version]', "{0:0.2f}".format(this_ver))
                    if message != '':
                        version_messages.append({
                            'version': this_ver,
                            'message': message
                        })
                    this_ver_messages = []
                this_ver = float(line.replace('=', '').strip())
            else:
                this_ver_messages.append(line)
        if len(this_ver_messages) > 0:
            message = ("\n".join(this_ver_messages)).strip()
            if message != '':
                message = message.replace('[version]', "{0:0.2f}".format(this_ver))
                version_messages.append({
                    'version': this_ver,
                    'message': message
                })
        return version_messages

    async def callback_handler(self, event):
        update = event.original_update
        if type(update) != UpdateBotCallbackQuery:
            return
        try:
            new_message = Message(id=None, from_id=update.user_id, to_id=update.chat_instance, message=update.data.decode('UTF-8'), out=True)
            new_event = NewMessage.Event(new_message)
            new_event.original_update = UpdateNewMessage(new_message, 0, 0)
            await self.message_handler(new_event)
        except:
            traceback.print_exc()

    async def message_handler(self, event):
        if type(event.original_update) != UpdateNewMessage:
            return
        try:
            if not self.bot_entity:
                self.bot_entity = await self.get_entity(MainHelper().get_config_value('tg_bot', 'bot_username'))
                self.bot_entity_id = self.bot_entity.id
            data = event.original_update
            if data.message.from_id == self.bot_entity_id:
                return
            MainHelper().play_notify_sound('notify_when_my_bot_message')
            if data.message.id and data.message.from_id and (data.message.from_id != self.tg_client.me_user_id):
                msg_entity_name = await self.tg_client.get_entity_name(data.message.from_id, 'User')
                if msg_entity_name:
                    print(StatusController.datetime_to_str(datetime.now()) + ' Message to my bot from "' + msg_entity_name + '"')
                    print('<<< ' + str(data.message.message))
                    t_date = StatusController.tg_datetime_to_local_datetime(data.message.date)
                    self.tg_client.add_message_to_db(self.bot_entity_id, 'Bot', data.message.from_id, self.bot_entity_id, data.message.id, data.message.message, t_date, 0)
            elif not data.message.id and data.message.from_id:
                msg_entity_name = await self.tg_client.get_entity_name(data.message.from_id, 'User')
                if msg_entity_name:
                    print(StatusController.datetime_to_str(datetime.now()) + ' Command to my bot from "' + msg_entity_name + '"')
                    print('<<< ' + str(data.message.message))

            forward_data = None

            if data.message.id:
                bot_chat = await event.get_input_chat()
                if data.message.fwd_from:
                    forward_data = {
                        'from_id': data.message.fwd_from.from_id,
                        'date_from': data.message.fwd_from.date
                    }
            else:
                try:
                    bot_chat = await self.get_input_entity(PeerUser(data.message.from_id))
                except:
                    traceback.print_exc()
                    bot_chat = None
                if not bot_chat:
                    bot_chat = self.tg_client.entity_controller.get_user_bot_chat(data.message.from_id)
            if not bot_chat:
                print("Can't get chat!")
                return
            try:
                ee_name = await self.tg_client.get_entity_name(bot_chat.user_id, 'User')
            except:
                ee_name = str(bot_chat.user_id)
            self.tg_client.entity_controller.add_entity_db_name(bot_chat.user_id, 'User', ee_name)
            self.tg_client.entity_controller.save_user_bot_chat(bot_chat)
            if data.message.message == '/start':
                self.tg_client.entity_controller.save_user_bot_last_version(bot_chat.user_id, MainHelper().get_config_float_value('main', 'actual_version'))
            if data.message.id:
                self.tg_client.bot_controller.set_message_for_user(data.message.from_id, data.message.id, False)
            await self.tg_client.bot_controller.bot_command(data.message.message, data.message.from_id, self.bot_entity_id, 'Bot', forward_data)
        except:
            traceback.print_exc()

    def do_start(self):
        print('Starting of bot')
        self.start(bot_token=MainHelper().get_config_value('tg_bot', 'token'))
        self.add_event_handler(self.message_handler, event=events.NewMessage)
        self.add_event_handler(self.callback_handler, event=events.CallbackQuery)
