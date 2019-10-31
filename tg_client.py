import asyncio
import sqlite3
import sys
import traceback
from collections import Counter

import socks
import configparser
from datetime import datetime
from getpass import getpass

from playsound import playsound
from telethon.errors import SessionPasswordNeededError, ChatIdInvalidError
from telethon.network import ConnectionTcpMTProxyRandomizedIntermediate, ConnectionTcpAbridged
from telethon import TelegramClient, events
from telethon.tl import functions
from telethon.tl.functions.messages import GetHistoryRequest, GetDialogsRequest
from telethon.tl.types import UpdateUserStatus, UserStatusOnline, UserStatusOffline, UpdateUserTyping, PeerUser, \
    UpdateNewChannelMessage, UpdateShortMessage, User, UpdateEditChannelMessage, \
    UpdateEditMessage, UpdateDeleteChannelMessages, UpdateMessagePoll, PeerChannel, PeerChat, UpdateDeleteMessages, \
    UpdateNewMessage, UpdateReadHistoryOutbox, UpdateReadHistoryInbox, UpdateDraftMessage, UpdateChannelMessageViews, \
    UpdateReadChannelInbox, UpdateWebPage, UpdateShortChatMessage, UpdateChatUserTyping, Channel, Chat, \
    UpdateNotifySettings, UpdateChannelPinnedMessage, InputPeerUser
from telethon.utils import get_display_name, is_list_like

from bot_controller import BotController
from entity_controller import EntityController
from periodic import Periodic
from status_controller import StatusController
from tg_bot import InteractiveTelegramBot


class InteractiveTelegramClient(TelegramClient):

    def __init__(self, config_file, loop):
        self.print_title('Initialization')

        self.client_loop = loop

        config = configparser.RawConfigParser(allow_no_value=True)
        config.read(config_file, encoding='utf-8')
        session_user_id = config['main']['session_fname']
        api_id = config['main']['api_id']
        api_hash = config['main']['api_hash']
        phone = config['main']['phone']

        proxy_use = int(config['main']['use_proxy'])
        proxy_type = str(config['main']['proxy_type'])
        proxy_host = str(config['main']['proxy_host'])
        proxy_port = str(config['main']['proxy_port'])
        proxy_login = str(config['main']['proxy_login'])
        proxy_password = str(config['main']['proxy_password'])
        proxy_secret = str(config['main']['proxy_secret'])
        if proxy_port:
            proxy_port = int(proxy_port)
        is_mtproxy = False

        if proxy_use > 0 and proxy_host and proxy_port > 0:
            if not proxy_type:
                proxy_type = 'SOCKS5'
            print('Using ' + proxy_type + ' proxy: ' + proxy_host + ':' + str(proxy_port))
            if proxy_type == 'MTPROTO':
                proxy_type = proxy_host
                proxy_host = proxy_port
                proxy_port = proxy_secret
                is_mtproxy = True
            elif proxy_type == 'SOCKS4':
                proxy_type = socks.SOCKS4
            elif proxy_type == 'HTTP':
                proxy_type = socks.HTTP
            else:
                proxy_type = socks.PROXY_TYPE_HTTP

            if proxy_login or proxy_password:
                proxy = (proxy_type, proxy_host, proxy_port, True, proxy_login, proxy_password)
            else:
                proxy = (proxy_type, proxy_host, proxy_port)
        else:
            proxy = None

        print('Initializing Telegram client...')

        if is_mtproxy:
            cl_conn = ConnectionTcpMTProxyRandomizedIntermediate
        else:
            cl_conn = ConnectionTcpAbridged

        super().__init__(session_user_id, api_id, api_hash, connection=cl_conn, proxy=proxy, sequential_updates=True)

        print('Connecting to Telegram servers...')
        try:
            self.client_loop.run_until_complete(self.connect())
        except IOError:
            print('Initial connection failed. Retrying...')
            self.client_loop.run_until_complete(self.connect())

        if not self.client_loop.run_until_complete(self.is_user_authorized()):
            print('First run. Sending code request...')
            if self.tmp_phone:
                user_phone = self.tmp_phone
            else:
                user_phone = input('Enter your phone: ')
            self.client_loop.run_until_complete(self.sign_in(user_phone))
            self_user = None
            while self_user is None:
                code = input('Enter the code you just received: ')
                try:
                    self_user = self.client_loop.run_until_complete(self.sign_in(code=code))
                except SessionPasswordNeededError:
                    pw = getpass('Two step verification is enabled. '
                                 'Please enter your password: ')
                    self_user = self.client_loop.run_until_complete(self.sign_in(password=pw))

        self.config = config
        self.db_conn = self.get_db('client_data.db')
        self.entity_controller = EntityController(self)

        if str(config['tg_bot']['session_fname']) and str(config['tg_bot']['token']):
            self.tg_bot = InteractiveTelegramBot(str(config['tg_bot']['session_fname']), api_id, api_hash, cl_conn, proxy, self)
            self.tg_bot.do_start()
        else:
            self.tg_bot = None

        self.tmp_phone = phone
        self.log_user_activity = False
        self.selected_user_activity = False

        self.status_controller = StatusController(self)
        self.bot_controller = BotController(self)
        self.bot_controller.register_cmd_branches()
        self.aa_controller = self.bot_controller.commands['/auto']['cmd']
        self.last_update = None
        self.dialogs_init_complete = False
        self.me_user_id = None
        self.me_user_name = None
        self.me_last_activity = datetime.now()

    def sprint(self, string, *args, **kwargs):
        try:
            print(string, *args, **kwargs)
        except UnicodeEncodeError:
            string = string.encode('utf-8', errors='ignore').decode('ascii', errors='ignore')
            print(string, *args, **kwargs)

    def print_title(self, title):
        self.sprint('\n')
        self.sprint('==={}==='.format('=' * len(title)))
        self.sprint('== {} =='.format(title))
        self.sprint('==={}==='.format('=' * len(title)))

    async def async_input(self, prompt):
        print(prompt, end='', flush=True)
        return (await self.client_loop.run_in_executor(None, sys.stdin.readline)).rstrip()

    def is_set_config_event(self, event_name):
        return (event_name in self.config['events']) and int(self.config['events'][event_name]) == 1

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def get_db(self, db_name):
        conn = sqlite3.connect(db_name, check_same_thread=False)
        conn.row_factory = self.dict_factory
        c = conn.cursor()
        StatusController.init_db(c)
        c.execute("""
            CREATE TABLE IF NOT EXISTS "messages" (
                "entity_id" INTEGER NOT NULL,
                "entity_type" TEXT NULL,
                "from_id" INTEGER NULL,
                "to_id" INTEGER NULL,
                "message_id" INTEGER NOT NULL,
                "message" TEXT NULL,
                "taken_at" DATETIME NOT NULL,
                "version" INTEGER NOT NULL,
                "removed_at" DATETIME NULL,
                "removed" INTEGER NOT NULL
            );
        """)
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS "e_id_msg_id_ver" ON "messages" (
                "entity_id" ASC,
                "message_id" ASC,
                "version" ASC
            );
        """)
        EntityController.init_db(c)
        return conn

    async def removed_messages_db_entity_get(self, message_ids):
        entities_var_groups = []
        for message_id in message_ids:
            rows = self.db_conn.execute("""
                SELECT * FROM `messages` WHERE `message_id` = ? AND `removed` = ?
                ORDER BY `taken_at` DESC, `version` DESC, `entity_id` DESC
            """, [str(message_id), '0']).fetchall()
            entities_v = []
            for row in rows:
                entity = None
                if row['entity_type'] in ['User', 'Bot']:
                    entity = PeerUser(int(row['entity_id']))
                elif row['entity_type'] == 'Chat':
                    entity = PeerChat(int(row['entity_id']))
                elif row['entity_type'] == 'Channel':
                    entity = PeerChannel(int(row['entity_id']))
                if entity:
                    try:
                        messages = await self.get_messages(entity, ids=[message_id])
                    except:
                        messages = None
                    if not messages or len(messages) == 0 or not messages[0]:
                        entities_v.append(int(row['entity_id']))
            if len(entities_v) > 0:
                entities_var_groups.append(entities_v)
        res_variants_all = []
        res_variants_1 = []
        for entities_var_group in entities_var_groups:
            for entities_var_group_i in entities_var_group:
                res_variants_all.append(entities_var_group_i)
                if len(entities_var_group) == 1:
                    res_variants_1.append(entities_var_group_i)

        if len(res_variants_all) > 0:
            most_common_a, num_most_common_a = Counter(res_variants_all).most_common(1)[0]
            if (num_most_common_a == 1) and len(res_variants_1) > 1:
                most_common_1, num_most_common_1 = Counter(res_variants_1).most_common(1)[0]
                return most_common_1
            return most_common_a

        return None

    def remove_message_db(self, message_id, entity_id=None):
        if entity_id is None:
            row = self.db_conn.execute("""
                SELECT * FROM `messages` WHERE `message_id` = ? 
                ORDER BY `taken_at` DESC, `version` DESC LIMIT 1
            """, [str(message_id)]).fetchone()
        else:
            row = self.db_conn.execute("""
                SELECT * FROM `messages` WHERE `message_id` = ? AND `entity_id` = ? 
                ORDER BY `taken_at` DESC, `version` DESC LIMIT 1
            """, [str(message_id), str(entity_id)]).fetchone()
        if row:
            c = self.db_conn.cursor()
            c.execute("""
                UPDATE `messages` SET `removed` = ?, `removed_at` = DATETIME('now', 'localtime') 
                WHERE `entity_id` = ? AND `message_id` = ? AND `version` = ?
            """, [
                '1', str(row['entity_id']), str(row['message_id']), str(row['version'])
            ])
            c.execute("""
                UPDATE `messages` SET `removed` = ?
                WHERE `entity_id` = ? AND `message_id` = ? AND `removed_at` IS NULL
            """, [
                '1', str(row['entity_id']), str(row['message_id'])
            ])
            self.db_conn.commit()
            return row
        return None

    def add_message_to_db(self, entity_id, entity_type, from_id, to_id, message_id, message, taken_at, removed=0):
        row = self.db_conn.execute("""
            SELECT * FROM `messages` WHERE `entity_id` = ? AND `message_id` = ? ORDER BY `version` DESC LIMIT 1
        """, [str(entity_id), str(message_id)]).fetchone()
        if row:
            if message == row['message']:
                return None
            version = int(row['version']) + 1
        else:
            version = 1

        c = self.db_conn.cursor()
        c.execute('INSERT INTO `messages` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', [
            str(entity_id), str(entity_type), str(from_id), str(to_id),
            str(message_id), message, taken_at, str(version), None, str(removed)
        ])
        self.db_conn.commit()
        return version

    async def get_entity_name(self, entity_id, entity_type='', allow_str_id=True, with_additional_text=False):
        return await self.entity_controller.get_entity_name(entity_id, entity_type, allow_str_id, with_additional_text)

    async def add_current_status(self, status, user_id, login=None, use_last_time=False):
        if not login:
            login = await self.get_entity_name(user_id, 'User')
        await self.status_controller.add_current_status(status, user_id, login, use_last_time)

    @staticmethod
    def get_dict_message_text(data_message):
        if 'message' in data_message:
            message_text = str(data_message['message'])
        else:
            message_text = ''
        message_parts = []

        if ('reply_markup' in data_message) and data_message['reply_markup']:
            message_parts.append('[reply_markup]')
        if ('media' in data_message) and data_message['media']:
            if (data_message['media']['_'] == 'MessageMediaWebPage') and data_message['media']['webpage'] and data_message['media']['webpage']['_'] == 'WebPage':
                message_parts.append('[url '+str(data_message['media']['webpage']['url'])+']')
            elif (data_message['media']['_'] == 'MessageMediaPhoto') and data_message['media']['photo'] and data_message['media']['photo']['_'] == 'Photo':
                message_parts.append('[photo '+str(data_message['media']['photo']['id'])+']')
            elif (data_message['media']['_'] == 'MessageMediaDocument') and data_message['media']['document'] and data_message['media']['document']['_'] == 'Document':
                fn = str(data_message['media']['document']['id'])
                fs = ''
                if ('attributes' in data_message['media']['document']) and data_message['media']['document']['attributes']:
                    for attr in data_message['media']['document']['attributes']:
                        if attr['_'] == 'DocumentAttributeFilename':
                            fn = attr['file_name']
                        elif attr['_'] == 'DocumentAttributeImageSize':
                            fs = str(attr['w']) + 'x' + str(attr['h'])
                if fs:
                    fn = fn + ' ' + fs
                message_parts.append('[document '+fn+' '+str(data_message['media']['document']['mime_type'])+']')

        if (message_text != '') and (len(message_parts) > 0):
            message_text = message_text + '\n'

        message_text = message_text + (", ".join(message_parts))

        return message_text.strip()

    async def raw_handler(self, update):
        if not self.dialogs_init_complete:
            return
        try:
            self.last_update = datetime.now()
            if type(update) in [UpdateEditChannelMessage, UpdateEditMessage, UpdateNewChannelMessage,
                                UpdateShortMessage, UpdateShortChatMessage, UpdateNewMessage]:
                data = update.to_dict()
                if not self.is_set_config_event(data['_']):
                    return
                # print(update)

                is_message_edit = (type(update) in [UpdateEditChannelMessage, UpdateEditMessage])
                is_message_new = (type(update) in [UpdateNewChannelMessage, UpdateShortMessage, UpdateNewMessage])

                reply_skip_message = False

                if ('message' in data) and data['message']:
                    from_id = None
                    to_id = None
                    entity_id = None
                    e_type = None
                    if not is_list_like(data['message']):
                        message_id = data['id']
                        message_date = data['date']
                        messages = None
                        if 'user_id' in data:
                            if self.me_user_id != data['user_id']:
                                entity_id = data['user_id']
                            e_type = 'User'
                            try:
                                entity = await self.get_entity(PeerUser(entity_id))
                                messages = await self.get_messages(PeerUser(entity_id), ids=[message_id])
                            except:
                                traceback.print_exc()
                                entity = None
                            if entity and entity.bot:
                                e_type = 'Bot'
                            if messages and (len(messages) > 0) and messages[0]:
                                data_message = messages[0].to_dict()
                            else:
                                data_message = {
                                    'to_id': {'_': 'PeerUser', 'user_id': self.me_user_id},
                                    'from_id': entity_id,
                                    'message': data['message']
                                }
                        elif 'chat_id' in data:
                            entity_id = data['chat_id']
                            e_type = 'Chat'
                            try:
                                messages = await self.get_messages(PeerChat(entity_id), ids=[message_id])
                            except:
                                traceback.print_exc()
                            if messages and (len(messages) > 0) and messages[0]:
                                data_message = messages[0].to_dict()
                            else:
                                data_message = {
                                    'to_id': {"_": 'PeerChat', 'chat_id': entity_id},
                                    'from_id': data['from_id'],
                                    'message': data['message']
                                }
                        elif 'channel_id' in data:
                            entity_id = data['channel_id']
                            e_type = 'Channel'
                            try:
                                messages = await self.get_messages(PeerChannel(entity_id), ids=[message_id])
                            except:
                                traceback.print_exc()
                            if messages and (len(messages) > 0) and messages[0]:
                                data_message = messages[0].to_dict()
                            else:
                                data_message = {
                                    'from_id': entity_id,
                                    'message': data['message']
                                }
                        else:
                            print('Unknown sender type. Ignoring message!!!')
                            return
                    else:
                        data_message = data['message']
                        message_id = data_message['id']
                        message_date = data_message['date']
                        if ('reply_markup' in data_message) and data_message['reply_markup'] and is_message_edit:
                            reply_skip_message = True

                    to_name = '*'
                    if 'to_id' in data_message:
                        if data_message['to_id']['_'] == 'PeerUser':
                            e_type = 'User'
                            if ('bot' in data_message['to_id']) and data_message['to_id']['bot']:
                                e_type = 'Bot'
                            to_id = data_message['to_id']['user_id']
                            if to_id != self.me_user_id:
                                entity_id = to_id
                            to_name = await self.get_entity_name(to_id, e_type)
                        elif data_message['to_id']['_'] == 'PeerChat':
                            e_type = 'Chat'
                            to_id = data_message['to_id']['chat_id']
                            entity_id = to_id
                            to_name = await self.get_entity_name(to_id, e_type)
                        elif data_message['to_id']['_'] == 'PeerChannel':
                            e_type = 'Channel'
                            to_id = data_message['to_id']['channel_id']
                            from_id = to_id
                            entity_id = to_id
                            to_name = await self.get_entity_name(to_id, e_type)

                    from_name = '*'
                    if ('from_id' in data_message) and data_message['from_id']:
                        from_name = await self.get_entity_name(data_message['from_id'])
                        from_id = data_message['from_id']
                        if (e_type in ['User', 'Bot']) and (from_id != self.me_user_id):
                            entity_id = data_message['from_id']

                    message = self.get_dict_message_text(data_message)

                    if ('post' in data_message) and data_message['post']:
                        message_type = 'Post'
                    else:
                        message_type = 'Message'

                    if is_message_edit:
                        a_type = 'Edit ' + message_type
                    elif is_message_new:
                        a_type = 'New ' + message_type
                    else:
                        a_type = message_type
                    a_type = a_type + ' ' + str(message_id)
                    if not e_type:
                        print('!!! NOT ETYPE')
                        if data['_'] in ['UpdateEditChannelMessage', 'UpdateNewChannelMessage']:
                            e_type = 'Channel'
                        elif data['_'] in ['UpdateShortChatMessage']:
                            e_type = 'Chat'
                        elif data['_'] in ['UpdateEditMessage', 'UpdateNewMessage', 'UpdateShortMessage']:
                            e_type = 'User'

                    a_post_type = ''
                    if ('reply_to_msg_id' in data) and (data['reply_to_msg_id']):
                        a_post_type = a_post_type + ' [reply]'
                    if ('fwd_from' in data) and (data['fwd_from']):
                        a_post_type = a_post_type + ' [forward]'

                    if is_message_edit or not message_date:
                        message_date = StatusController.now_local_datetime()
                    else:
                        message_date = StatusController.tg_datetime_to_local_datetime(message_date)

                    if entity_id and (e_type == 'Channel') and self.entity_controller.channel_is_megagroup(entity_id):
                        e_type = 'Megagroup'

                    if message and entity_id:
                        if (
                                (int(self.config['messages']['write_messages_to_database']) == 1) and
                                (
                                    ((e_type == 'User') and (int(self.config['messages']['event_include_users']) == 1)) or
                                    ((e_type == 'Bot') and (int(self.config['messages']['event_include_bots']) == 1)) or
                                    ((e_type == 'Chat') and (int(self.config['messages']['event_include_chats']) == 1)) or
                                    ((e_type == 'Megagroup') and (int(self.config['messages']['event_include_megagroups']) == 1)) or
                                    ((e_type == 'Channel') and (int(self.config['messages']['event_include_channels']) == 1))
                                )
                        ):
                            version = self.add_message_to_db(entity_id, e_type, from_id, to_id, message_id, message, message_date, 0)
                            if version and (version > 1):
                                a_post_type = a_post_type + ' [version '+str(version)+']'
                    elif is_message_edit and data_message and entity_id and ('action' in data_message) and \
                            data_message['action'] and (data_message['action']['_'] == 'MessageActionHistoryClear'):
                        msg = self.remove_message_db(message_id, entity_id)
                        if msg and msg['entity_id']:
                            a_type = 'History clear ' + message_type
                    elif not entity_id:
                        print('No entity_id')
                        print(data)
                        print(data_message)

                    if reply_skip_message:
                        a_post_type = a_post_type + ' [reply_markup]'

                    need_show_message = False
                    if message and not reply_skip_message:
                        if (
                                ((e_type == 'User') and (int(self.config['messages']['display_user_messages']) == 1)) or
                                ((e_type == 'Bot') and (int(self.config['messages']['display_bot_messages']) == 1)) or
                                ((e_type == 'Chat') and (int(self.config['messages']['display_chat_messages']) == 1)) or
                                ((e_type == 'Megagroup') and (int(self.config['messages']['display_megagroup_messages']) == 1)) or
                                ((e_type == 'Channel') and (int(self.config['messages']['display_channel_messages']) == 1))
                        ):
                            need_show_message = True

                    if not reply_skip_message and not need_show_message:
                        a_post_type = a_post_type + ' [skip]'

                    if e_type != 'Channel':
                        print(StatusController.datetime_to_str(message_date) + ' ' + a_type + ' "' + from_name + '" -> "' + to_name + '" ' + a_post_type)
                    else:
                        print(StatusController.datetime_to_str(message_date) + ' ' + a_type + ' "' + to_name + '" ' + a_post_type)

                    if is_message_new and entity_id and (int(self.config['notify_all']['notify_when_new_message']) == 1):
                        playsound(self.config['notify_sounds']['notify_when_new_message_sound'], False)
                    elif is_message_new and entity_id and (int(self.config['notify_selected']['notify_when_new_message']) == 1) and (self.selected_user_activity == entity_id):
                        playsound(self.config['notify_sounds']['notify_when_new_message_sound'], False)
                    elif is_message_edit and ((not reply_skip_message) or (e_type == 'User')) and entity_id and (int(self.config['notify_all']['notify_when_editing']) == 1):
                        playsound(self.config['notify_sounds']['notify_when_editing_sound'], False)
                    elif is_message_edit and ((not reply_skip_message) or (e_type == 'User')) and entity_id and (int(self.config['notify_selected']['notify_when_editing']) == 1) and (self.selected_user_activity == entity_id):
                        playsound(self.config['notify_sounds']['notify_when_editing_sound'], False)

                    is_bot_message = False
                    if is_message_new and (e_type in ['User']) and (
                            ((to_id == self.me_user_id) and (from_id == self.selected_user_activity)) or
                            ((from_id == self.me_user_id) and (to_id == self.selected_user_activity)) or
                            self.bot_controller.is_active_for_user(entity_id, False)
                    ) and (
                            (not self.tg_bot) or
                            (not self.tg_bot.bot_entity) or
                            (self.tg_bot.bot_entity.id != entity_id)
                    ):
                        is_bot_message = await self.bot_controller.bot_check_user_message(message, from_id, entity_id, e_type)

                    if need_show_message and is_message_edit and (int(self.config['messages']['display_edit_messages']) == 1):
                        need_show_message = False

                    if need_show_message and not is_bot_message:
                        self.sprint('<<< ' + message.replace('\n', ' \\n '))

                    if (e_type == 'User') and not is_bot_message:
                        if from_id == self.me_user_id:
                            await self.aa_controller.on_me_write_message_to_user(entity_id)
                        else:
                            await self.aa_controller.on_user_message_to_me(from_id, message)

            elif type(update) in [UpdateDeleteChannelMessages, UpdateDeleteMessages]:
                data = update.to_dict()
                if not self.is_set_config_event(data['_']):
                    return

                if ('channel_id' in data) and data['channel_id']:
                    entity_id = data['channel_id']
                else:
                    entity_id = await self.removed_messages_db_entity_get(data['messages'])
                    if not entity_id:
                        print(StatusController.datetime_to_str(datetime.now()) + ' Remove messages ' + str(data['messages']) + ' from unknown dialog (no variants)')
                        return

                if int(self.config['notify_all']['notify_when_removing']) == 1:
                    playsound(self.config['notify_sounds']['notify_when_removing_sound'], False)
                elif (int(self.config['notify_selected']['notify_when_removing']) == 1) and (self.selected_user_activity == entity_id):
                    playsound(self.config['notify_sounds']['notify_when_removing_sound'], False)

                for msg_id in data['messages']:
                    msg = self.remove_message_db(msg_id, entity_id)
                    # if (not msg) and (type(update) == UpdateDeleteChannelMessages):
                    #    msg = await self.collect_message_info(msg_id, PeerChannel(data['channel_id']))
                    if msg and msg['entity_id']:
                        msg_entity_name = await self.get_entity_name(msg['entity_id'])
                        if msg['entity_type'] != 'Channel':
                            msg_from_name = await self.get_entity_name(msg['from_id'])
                            if msg_from_name:
                                msg_from_name = ' "'+msg_from_name+'"`s'
                        else:
                            msg_from_name = ''
                        dialog_with_name = 'dialog with'
                        if msg['entity_type'] == 'Chat':
                            dialog_with_name = 'chat with'
                        elif msg['entity_type'] == 'Channel':
                            dialog_with_name = 'channel'
                        msg_version = ''
                        if ('version' in msg) and (msg['version'] > 1):
                            msg_version = ' [version '+str(msg['version'])+']'
                        print(StatusController.datetime_to_str(datetime.now()) + ' Remove'+msg_from_name+' message ' + str(msg_id) + msg_version + ' from '+dialog_with_name+' "' + msg_entity_name + '"')
                        if msg['message'] and (int(self.config['messages']['display_remove_messages']) == 1):
                            print('<<< ' + str(msg['message']).replace('\n', '\\n'))

            elif type(update) == UpdateReadHistoryOutbox:
                data = update.to_dict()
                message_info = await self.collect_message_info(data['max_id'], update.peer)
                if message_info:
                    if self.tg_bot and (self.tg_bot.bot_entity_id == message_info['to_id']):
                        if int(self.config['notify_all']['notify_when_my_bot_reads_message']) == 1:
                            playsound(self.config['notify_sounds']['notify_when_reads_message_sound'], False)
                    elif int(self.config['notify_all']['notify_when_reads_message']) == 1:
                        playsound(self.config['notify_sounds']['notify_when_reads_message_sound'], False)
                    elif (int(self.config['notify_selected']['notify_when_reads_message']) == 1) and (self.selected_user_activity == message_info['peer_id']):
                        playsound(self.config['notify_sounds']['notify_when_reads_message_sound'], False)
                    print(StatusController.datetime_to_str(datetime.now()) + ' "' + message_info['peer_name'] + '" reads message ' + str(data['max_id']) + ' from dialog with "' + message_info['not_peer_name'] + '"')

            elif type(update) in [UpdateMessagePoll, UpdateUserStatus, UpdateReadChannelInbox, UpdateReadHistoryInbox,
                                  UpdateDraftMessage, UpdateChannelMessageViews, UpdateWebPage,
                                  UpdateChannelPinnedMessage, UpdateUserTyping, UpdateChatUserTyping,
                                  UpdateNotifySettings]:
                pass
            else:
                print('Raw')
                print(update)
        except:
            traceback.print_exc()

    async def collect_message_info(self, data_message, peer_data=None):
        if peer_data and (type(peer_data) in [PeerUser, PeerChannel, PeerChat]) and not is_list_like(data_message):
            messages = None
            try:
                messages = await self.get_messages(peer_data, ids=[data_message])
            except:
                traceback.print_exc()
            if messages and (len(messages) > 0) and messages[0]:
                data_message = messages[0].to_dict()
            else:
                data_message = None

        if (not data_message) or not is_list_like(data_message):
            return None

        to_id = None
        to_name = None
        if ('to_id' in data_message) and data_message['to_id']:
            if data_message['to_id']['_'] == 'PeerUser':
                source_type = 'User'
                if ('bot' in data_message['to_id']) and data_message['to_id']['bot']:
                    source_type = 'Bot'
                to_id = data_message['to_id']['user_id']
                to_name = await self.get_entity_name(to_id, source_type)
            elif data_message['to_id']['_'] == 'PeerChannel':
                source_type = 'Channel'
                to_id = data_message['to_id']['channel_id']
                to_name = await self.get_entity_name(to_id, source_type)
            elif data_message['to_id']['_'] == 'PeerChat':
                source_type = 'Chat'
                to_id = data_message['to_id']['chat_id']
                to_name = await self.get_entity_name(to_id, source_type)

        from_id = None
        from_name = None
        if ('from_id' in data_message) and data_message['from_id']:
            from_name = await self.get_entity_name(data_message['from_id'])
            from_id = data_message['from_id']

        message = ''
        if 'message' in data_message:
            message = str(data_message['message'])

        peer_id = None
        peer_name = None
        not_peer_name = None
        if peer_data:
            peer_id, peer_name = await self.entity_controller.get_entity_id_name_by_entity(peer_data)
            if peer_name == from_name:
                not_peer_name = to_name
            elif peer_name == to_name:
                not_peer_name = from_name

        return {
            'from_id': from_id,
            'entity_id': to_id,
            'to_id': to_id,
            'from_name': from_name,
            'to_name': to_name,
            'peer_id': peer_id,
            'peer_name': peer_name,
            'not_peer_name': not_peer_name,
            'message': message,
            'version': 1,
            'data': data_message
        }

    async def update_handler(self, update):
        if not self.dialogs_init_complete:
            return
        if not update or not update.original_update:
            return
        try:
            data = update.original_update.to_dict()
            if (data['_'] == 'UpdateUserStatus') and (data['user_id'] == self.me_user_id) and (data['status']['_'] == 'UserStatusOnline'):
                self.me_last_activity = datetime.now()
            if not self.is_set_config_event(data['_']):
                return
            if data['_'] == 'UpdateUserStatus':
                if (
                    data['user_id'] and (data['status']['_'] in ['UserStatusOnline', 'UserStatusOffline']) and (
                        (self.log_user_activity is True) or
                        (self.log_user_activity == data['user_id'])
                    )
                ):
                    if (data['status']['_'] == 'UserStatusOnline') and (int(self.config['notify_all']['notify_when_online']) == 1):
                        playsound(self.config['notify_sounds']['notify_when_online_sound'], False)
                    elif (data['status']['_'] == 'UserStatusOnline') and (int(self.config['notify_selected']['notify_when_online']) == 1) and (self.selected_user_activity == data['user_id']):
                        playsound(self.config['notify_sounds']['notify_when_online_sound'], False)
                    await self.add_current_status(update.original_update.status, data['user_id'])
            elif data['_'] in ['UpdateUserTyping', 'UpdateChatUserTyping']:
                login = await self.get_entity_name(data['user_id'], 'User')
                where_typing = 'me'
                if (data['_'] == 'UpdateChatUserTyping') and data['chat_id']:
                    where_typing = await self.get_entity_name(data['chat_id'])
                if int(self.config['notify_all']['notify_when_typing']) == 1:
                    playsound(self.config['notify_sounds']['notify_when_typing_sound'], False)
                elif (int(self.config['notify_selected']['notify_when_typing']) == 1) and (self.selected_user_activity == data['user_id']):
                    playsound(self.config['notify_sounds']['notify_when_typing_sound'], False)
                print(StatusController.datetime_to_str(datetime.now()) + ' "' + login + '" typing -> ' + where_typing + "...")
            else:
                pass
        except:
            traceback.print_exc()

    async def connection_check_reconnect(self):
        if self.is_connected():
            print("Connected!")
        if await self.is_user_authorized():
            print("Authorized!")
        if self.last_update:
            print('Last activity update: ' + StatusController.datetime_to_str(self.last_update))
        await self.get_dialogs(limit=1)
        await self.get_entity("me")

    async def periodic_check(self):
        try:
            if self.dialogs_init_complete and self.last_update:
                if (datetime.now() - self.last_update).total_seconds() > 240:
                    print('Reconnection...')
                    self.is_connected()
                    await self.is_user_authorized()
                    await self.get_dialogs(limit=1)
                    me_entity = await self.get_entity("me")
                    me_entity_data = me_entity.to_dict()
                    if me_entity_data['status'] and (me_entity_data['status']['_'] == 'UserStatusOnline'):
                        self.me_last_activity = datetime.now()
                    self.last_update = datetime.now()
                await self.aa_controller.on_timer()
        except:
            traceback.print_exc()

    async def run(self):
        me_entity = await self.get_entity("me")
        self.me_user_id = me_entity.id
        self.me_user_name = get_display_name(me_entity)

        # self.add_event_handler(self.message_handler, event=events.NewMessage)
        self.add_event_handler(self.update_handler, event=events.UserUpdate)
        self.add_event_handler(self.raw_handler, event=events.Raw)

        p = Periodic(self.periodic_check, 62)
        await p.start()

        self.log_user_activity = (int(self.config['activity']['write_all_activity_to_database']) == 1)

        while True:
            entity = None
            self.bot_controller.stop_chat_with_all_users()
            self.selected_user_activity = False
            dialog_count = int(self.config['messages']['initial_dialogs_limit'])
            if dialog_count <= 0:
                dialog_count = None
            t_dialogs = await self.get_dialogs(limit=dialog_count, archived=False, folder=0)
            dialog_count = len(t_dialogs)

            t_dial_entity_ids = []
            dialogs = ["me"]
            for t_dialog in t_dialogs:

                e_id, e_type = self.entity_controller.process_entity(t_dialog.entity)

                if (not e_id) or e_id in t_dial_entity_ids:
                    continue

                t_dial_entity_ids.append(e_id)

                show_e_types = ['User']
                if int(self.config['messages']['initial_dialogs_include_bots']) == 1:
                    show_e_types.append('Bot')
                if int(self.config['messages']['initial_dialogs_include_chats']) == 1:
                    show_e_types.append('Chat')
                if int(self.config['messages']['initial_dialogs_include_channels']) == 1:
                    show_e_types.append('Channel')
                if int(self.config['messages']['initial_dialogs_include_megagroups']) == 1:
                    show_e_types.append('Megagroup')

                if e_id and (int(self.config['messages']['write_messages_to_database']) == 1):
                    initial_limit = int(self.config['messages']['initial_messages_count'])
                    if (
                            (initial_limit > 0) and
                            (
                                ((e_type == 'User') and (int(self.config['messages']['initial_include_users']) == 1)) or
                                ((e_type == 'Bot') and (int(self.config['messages']['initial_include_bots']) == 1)) or
                                ((e_type == 'Chat') and (int(self.config['messages']['initial_include_chats']) == 1)) or
                                ((e_type == 'Megagroup') and (int(self.config['messages']['initial_include_megagroups']) == 1)) or
                                ((e_type == 'Channel') and (int(self.config['messages']['initial_include_channels']) == 1))
                            )
                    ):
                        print("Getting messages for dialog with " + get_display_name(t_dialog.entity))
                        await self.entity_controller.add_entity_dialog_messages_to_db(t_dialog.entity, initial_limit)
                        await asyncio.sleep(float(self.config['messages']['initial_messages_delay']))
                if (e_type in show_e_types) and get_display_name(t_dialog.entity):
                    dialogs.append(t_dialog)

            self.dialogs_init_complete = True

            i = None
            while i is None:
                self.print_title('Users window')
                for i, dialog in enumerate(dialogs, start=1):
                    if dialog != "me":
                        self.sprint('{}. {}'.format(i, get_display_name(dialog.entity)))
                    else:
                        self.sprint('{}. {}'.format(i, get_display_name(me_entity)))

                print()
                print('> Available commands:')
                print('  !q: Quits the commands window and exits.')
                print('  !l: Logs out, terminating this session.')
                print('  !C: Check connection & reconnect.')
                print('  !P: pick user by username. param1 - username')
                if not self.log_user_activity:
                    print('  !L: Log all users activities to DB-file (until !L).')
                print()
                i = await self.async_input('Enter user ID or a command: \n')
                param1 = None
                i_words = str(i).split(" ")
                if len(i_words) > 1:
                    i = i_words[0]
                    param1 = i_words[1]

                if (i == '!L') and not self.log_user_activity:
                    self.log_user_activity = True
                    self.print_title('Print !L to exit, or !C to reconnect...')
                    print()
                    while True:
                        msg = await self.async_input('')
                        if msg == '!L':
                            self.log_user_activity = False
                            break
                        elif msg == '!C':
                            await self.connection_check_reconnect()
                elif i == '!C':
                    await self.connection_check_reconnect()
                elif i == '!P':
                    try:
                        tmp_entity = await self.get_entity(param1)
                    except:
                        tmp_entity = None
                    if tmp_entity and type(tmp_entity) == User:
                        entity = tmp_entity
                        i = None
                        break
                    else:
                        print('Wrong entity!')
                elif i == '!q':
                    return
                elif i == '!l':
                    await self.log_out()
                    return

                try:
                    i = int(i if i else 0) - 1
                    if not 0 <= i < dialog_count:
                        i = None
                except ValueError:
                    i = None

            if not entity:
                entity = dialogs[i]
            if entity == "me":
                entity = me_entity
            elif type(entity) != User:
                entity = entity.entity

            self.sprint('Selected entity: {}'.format(get_display_name(entity)))
            if type(entity) == User:
                print('Entity status: ', entity.status)
            print()

            print('Available commands:')
            print('  !q:  Deselect entity.')
            print('  !Q:  Deselect entity and exits.')
            print('  !D:  Get entity data (for debug purpose).')
            if type(entity) == User:
                print('  !M:  add all messages to database (may take a long time)')
                print('  !A:  Append to AA & run AA if not set. optional param - message for AA')
                print('  !L:  Logs only selected user online activity to file (until !L).')
                print('  !LD: Prints log activity - intervals.')
                print('  !LE: Prints log activity for CSV.')
                print('  !LP: Shows log activity as plot.')
                print('       LD, LE & LP commands accept 2 optional params:')
                print('       1) date in format like 2019-09-27    2) 1 if dates should be merged / omitted in result')
                print('       examples:')
                print('           # !LP               - plot for all days')
                print('           # !LP - 1           - plot for all days grouped by hours')
                print('           # !LP 2019-09-27    - plot for specific day')
            print()

            self.selected_user_activity = entity.id

            while True:
                msg = await self.async_input('Enter a command: ')
                param1 = None
                param2 = None
                params_text = None
                msg_words = str(msg).split(" ")
                if len(msg_words) > 1:
                    msg = msg_words[0]
                    param1 = msg_words[1]
                    params_text = msg_words[1:]
                    params_text = " ".join(params_text)
                    if len(msg_words) > 2:
                        param2 = msg_words[2]
                # Quit
                if msg == '!q':
                    break
                elif msg == '!Q':
                    return
                elif msg == '!D':
                    user_data = await self.get_entity(entity.id)
                    print(user_data)
                elif (msg in ['!LD', '!LE', '!LP']) and (type(entity) == User):
                    p_types = {'!LD': "diap", '!LE': "excel", '!LP': "plot"}
                    await self.status_controller.print_user_activity(entity.id, get_display_name(entity), p_types[msg], param1, (param2 == '1'))
                elif (msg == '!M') and (type(entity) == User):
                    print('Begin message adding...')
                    await self.entity_controller.add_entity_dialog_messages_to_db(entity, None)
                    print('Message adding complete!')
                elif (msg == '!A') and (type(entity) == User):
                    await self.aa_controller.force_add_user(entity.id, params_text)
                elif (msg == '!L') and (type(entity) == User):
                    self.log_user_activity = entity.id
                    self.print_title('Print !L to exit or !C to reconnect...')
                    print()
                    await self.add_current_status(entity.status, entity.id, None, True)
                    while True:
                        msg = await self.async_input('')
                        if msg == '!L':
                            self.log_user_activity = (int(self.config['activity']['write_all_activity_to_database']) == 1)
                            break
                        elif msg == '!C':
                            await self.connection_check_reconnect()
