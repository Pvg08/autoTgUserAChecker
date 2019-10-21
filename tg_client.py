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
from telethon.tl.types import UpdateUserStatus, UserStatusOnline, UserStatusOffline, UpdateUserTyping, PeerUser, \
    UpdateNewChannelMessage, UpdateShortMessage, User, UpdateEditChannelMessage, \
    UpdateEditMessage, UpdateDeleteChannelMessages, UpdateMessagePoll, PeerChannel, PeerChat, UpdateDeleteMessages, \
    UpdateNewMessage, UpdateReadHistoryOutbox, UpdateReadHistoryInbox, UpdateDraftMessage, UpdateChannelMessageViews, \
    UpdateReadChannelInbox, UpdateWebPage, UpdateShortChatMessage, UpdateChatUserTyping, Channel, Chat, \
    UpdateNotifySettings, UpdateChannelPinnedMessage
from telethon.utils import get_display_name, is_list_like

from bot_controller import BotController
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

        self.config = config
        self.tmp_phone = phone
        self.log_user_activity = False
        self.selected_user_activity = False
        self.user_logins = {}
        self.channel_names = {}
        self.chat_names = {}
        self.channel_megagroups = {}
        self.db_conn = self.get_db('client_data.db')
        self.status_controller = StatusController(self)
        self.bot_controller = BotController(self)
        self.last_update = None
        self.dialogs_init_complete = False
        self.me_user_id = None

        print('Initializing Telegram client...')

        if is_mtproxy:
            cl_conn = ConnectionTcpMTProxyRandomizedIntermediate
        else:
            cl_conn = ConnectionTcpAbridged

        super().__init__(session_user_id, api_id, api_hash, connection=cl_conn, proxy=proxy, sequential_updates=True)

        self.found_media = {}

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

        if str(config['tg_bot']['session_fname']) and str(config['tg_bot']['token']):
            self.tg_bot = InteractiveTelegramBot(str(config['tg_bot']['session_fname']), api_id, api_hash, cl_conn, proxy, self)
            self.tg_bot.do_start()
        else:
            self.tg_bot = None

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
            CREATE TABLE IF NOT EXISTS "entities" (
                "entity_id" INTEGER NOT NULL,
                "entity_type" TEXT NULL,
                "entity_name" TEXT NULL,
                "entity_phone" TEXT NULL,
                "taken_at" DATETIME NOT NULL,
                "version" INTEGER NOT NULL
            );
        """)
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
                if row['entity_type'] == 'User':
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

    async def get_entity_id_name_by_entity(self, entity):
        if not entity:
            return None, None
        if type(entity) == PeerUser:
            return entity.user_id, await self.get_entity_name(entity.user_id)
        elif type(entity) == PeerChannel:
            return entity.channel_id, await self.get_entity_name(entity.channel_id)
        elif type(entity) == PeerChat:
            return entity.chat_id, await self.get_entity_name(entity.chat_id)
        return None, None

    def add_entity_db_name(self, entity_id, entity_type, entity_name, entity_phone=None, entity_is_megagroup=False):
        row = self.db_conn.execute("""
            SELECT * FROM `entities` WHERE `entity_id` = ? ORDER BY `version` DESC LIMIT 1
        """, [str(entity_id)]).fetchone()
        if row:
            if (entity_name == row['entity_name']) and (entity_type == row['entity_type']):
                return None
            version = int(row['version']) + 1
        else:
            version = 1

        if entity_is_megagroup:
            entity_type = 'Megagroup'

        c = self.db_conn.cursor()
        c.execute('INSERT INTO `entities` VALUES(?, ?, ?, ?, ?, ?)', [
            str(entity_id), str(entity_type), entity_name, entity_phone,
            self.status_controller.datetime_to_str(datetime.now()),
            str(version)
        ])
        self.db_conn.commit()

    def channel_is_megagroup(self, channel_id):
        str_channel_id = str(channel_id)
        if str_channel_id in self.channel_megagroups:
            return self.channel_megagroups[str_channel_id]
        return False

    async def get_entity_name(self, entity_id, entity_type='', allow_str_id=True, with_additional_text=False):
        if not entity_id:
            return None
        str_entity_id = str(entity_id)
        if entity_type in ['User', 'Bot']:
            if str_entity_id not in self.user_logins:
                if with_additional_text:
                    print('Trying to find user login for id=' + str_entity_id)
                try:
                    entity = await self.get_entity(PeerUser(entity_id))
                    if type(entity) != User:
                        raise ValueError(str_entity_id + ' is not user')
                    self.user_logins[str_entity_id] = self.get_user_name_text(entity)
                    self.add_entity_db_name(entity_id, entity_type, self.user_logins[str_entity_id])
                    if with_additional_text:
                        if entity.is_self:
                            msg = ' - me'
                        elif entity.mutual_contact:
                            msg = ' from contacts (mutual)'
                        elif entity.contact:
                            msg = ' from contacts'
                        elif entity.bot:
                            msg = ' - bot'
                        else:
                            msg = ''
                        print('Found. It is "' + self.user_logins[str_entity_id] + '"' + msg)
                except ValueError as e:
                    res_msg = 'Not user ' + str_entity_id
                    if allow_str_id:
                        print(res_msg)
                    else:
                        raise TypeError(res_msg)
                    self.user_logins[str_entity_id] = str_entity_id
            return self.user_logins[str_entity_id]
        elif entity_type == 'Chat':
            if str_entity_id not in self.chat_names:
                try:
                    entity = await self.get_entity(PeerChat(entity_id))
                    if type(entity) != Chat:
                        raise ValueError(str_entity_id + ' is not chat')
                    self.chat_names[str_entity_id] = get_display_name(entity)
                    self.add_entity_db_name(entity_id, entity_type, self.chat_names[str_entity_id])
                except (ValueError, ChatIdInvalidError) as e:
                    res_msg = 'Not chat ' + str_entity_id
                    if allow_str_id:
                        print(res_msg)
                    else:
                        raise TypeError(res_msg)
                    self.chat_names[str_entity_id] = str_entity_id
            return self.chat_names[str_entity_id]
        elif entity_type in ['Channel', 'Megagroup']:
            if str_entity_id not in self.channel_names:
                try:
                    entity = await self.get_entity(PeerChannel(entity_id))
                    if type(entity) != Channel:
                        raise ValueError(str_entity_id + ' is not channel')
                    self.channel_names[str_entity_id] = get_display_name(entity)
                    self.channel_megagroups[str_entity_id] = entity.megagroup
                    self.add_entity_db_name(entity_id, entity_type, self.channel_names[str_entity_id], None, entity.megagroup)
                except ValueError as e:
                    res_msg = 'Not channel ' + str_entity_id
                    if allow_str_id:
                        print(res_msg)
                    else:
                        raise TypeError(res_msg)
                    self.channel_names[str_entity_id] = str_entity_id
            return self.channel_names[str_entity_id]
        elif entity_type == '':
            if str_entity_id in self.user_logins:
                return self.user_logins[str_entity_id]
            elif str_entity_id in self.chat_names:
                return self.chat_names[str_entity_id]
            elif str_entity_id in self.channel_names:
                return self.channel_names[str_entity_id]
            else:
                try:
                    entity_name = await self.get_entity_name(entity_id, 'User', False, False)
                except (TypeError, ValueError) as e:
                    try:
                        entity_name = await self.get_entity_name(entity_id, 'Chat', False, False)
                    except (TypeError, ValueError) as e:
                        try:
                            entity_name = await self.get_entity_name(entity_id, 'Channel', False, False)
                        except (TypeError, ValueError) as e:
                            res_msg = 'Unknown entity ' + str_entity_id
                            if allow_str_id:
                                print(res_msg)
                            else:
                                raise TypeError(res_msg)
                            entity_name = str_entity_id
                return entity_name
        return str_entity_id

    def get_user_name_text(self, user: User):
        if user.username:
            login = user.username
        elif user.first_name and user.last_name:
            login = user.last_name + ' ' + user.first_name
        elif user.last_name:
            login = user.last_name
        elif user.first_name:
            login = user.first_name
        elif user.phone:
            login = user.phone
        else:
            login = str(user.id)

        return login

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
                                messages = await self.get_messages(PeerUser(entity_id), ids=[message_id])
                            except:
                                traceback.print_exc()
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
                        if (e_type == 'User') and (from_id != self.me_user_id):
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

                    if entity_id and (e_type == 'Channel') and self.channel_is_megagroup(entity_id):
                        e_type = 'Megagroup'

                    print(e_type + ' ' + str(entity_id))

                    if message and entity_id:
                        if (
                                (int(self.config['messages']['write_messages_to_database']) == 1) and
                                (
                                    ((e_type == 'User') and (int(self.config['messages']['event_include_users']) == 1)) or
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
                    if is_message_new and (self.selected_user_activity == entity_id):
                        is_bot_message = await self.bot_controller.bot_check_message(message, from_id, entity_id, e_type)

                    if need_show_message and not is_bot_message:
                        self.sprint('<<< ' + message.replace('\n', ' \\n '))

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
                        if msg['message']:
                            print('<<< ' + str(msg['message']).replace('\n', '\\n'))

            elif type(update) == UpdateReadHistoryOutbox:
                data = update.to_dict()
                message_info = await self.collect_message_info(data['max_id'], update.peer)
                if message_info:
                    if int(self.config['notify_all']['notify_when_reads_message']) == 1:
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
            peer_id, peer_name = await self.get_entity_id_name_by_entity(peer_data)
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
        if self.dialogs_init_complete and self.last_update and ((datetime.now() - self.last_update).total_seconds() > 240):
            print('Reconnection...')
            self.is_connected()
            await self.is_user_authorized()
            await self.get_dialogs(limit=1)
            await self.get_entity("me")
            self.last_update = datetime.now()

    async def run(self):
        me_entity = await self.get_entity("me")
        self.me_user_id = me_entity.id

        # self.add_event_handler(self.message_handler, event=events.NewMessage)
        self.add_event_handler(self.update_handler, event=events.UserUpdate)
        self.add_event_handler(self.raw_handler, event=events.Raw)

        p = Periodic(self.periodic_check, 62)
        await p.start()

        self.log_user_activity = (int(self.config['activity']['write_all_activity_to_database']) == 1)

        while True:
            self.bot_controller.bot_reset()
            self.selected_user_activity = False
            dialog_count = int(self.config['messages']['initial_dialogs_limit'])
            t_dialogs = await self.get_dialogs(limit=dialog_count)
            t_dial_entity_ids = []
            dialogs = ["me"]
            for t_dialog in t_dialogs:
                if type(t_dialog.entity) == User:
                    e_type = 'User'
                    e_id = t_dialog.entity.id
                    self.add_entity_db_name(e_id, e_type, self.get_user_name_text(t_dialog.entity), t_dialog.entity.phone, False)
                elif type(t_dialog.entity) == Chat:
                    e_type = 'Chat'
                    e_id = t_dialog.entity.id
                    self.chat_names[str(e_id)] = get_display_name(t_dialog.entity)
                    self.add_entity_db_name(e_id, e_type, get_display_name(t_dialog.entity), None, False)
                elif type(t_dialog.entity) == Channel:
                    e_type = 'Channel'
                    if t_dialog.entity.megagroup:
                        e_type = 'Megagroup'
                    e_id = t_dialog.entity.id
                    self.channel_megagroups[str(e_id)] = t_dialog.entity.megagroup
                    self.channel_names[str(e_id)] = get_display_name(t_dialog.entity)
                    self.add_entity_db_name(e_id, e_type, get_display_name(t_dialog.entity), None, t_dialog.entity.megagroup)
                else:
                    e_type = ''
                    e_id = None

                if (not e_id) or e_id in t_dial_entity_ids:
                    continue

                t_dial_entity_ids.append(e_id)

                show_e_types = ['User']
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
                                ((e_type == 'Chat') and (int(self.config['messages']['initial_include_chats']) == 1)) or
                                ((e_type == 'Megagroup') and (int(self.config['messages']['initial_include_megagroups']) == 1)) or
                                ((e_type == 'Channel') and (int(self.config['messages']['initial_include_channels']) == 1))
                            )
                    ):
                        print("Getting messages for dialog with " + get_display_name(t_dialog.entity))
                        messages = await self.get_messages(t_dialog.entity, limit=initial_limit)
                        for message_i in messages:
                            t_date = StatusController.tg_datetime_to_local_datetime(message_i.date)
                            from_id = None
                            to_id = None
                            data_message = message_i.to_dict()
                            if 'to_id' in data_message:
                                if data_message['to_id']['_'] == 'PeerUser':
                                    to_id = data_message['to_id']['user_id']
                                elif data_message['to_id']['_'] == 'PeerChannel':
                                    to_id = data_message['to_id']['channel_id']
                                    from_id = to_id
                                elif data_message['to_id']['_'] == 'PeerChat':
                                    to_id = data_message['to_id']['chat_id']

                            if ('from_id' in data_message) and data_message['from_id']:
                                from_id = data_message['from_id']

                            message_text = self.get_dict_message_text(data_message)
                            self.add_message_to_db(e_id, e_type, from_id, to_id, message_i.id, message_text, t_date, 0)
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
                if not self.log_user_activity:
                    print('  !L: Log all users activities to DB-file (until !L).')
                print()
                i = await self.async_input('Enter user ID or a command: \n')
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

            entity = dialogs[i]
            if entity == "me":
                entity = me_entity
            else:
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
                msg_words = str(msg).split(" ")
                if len(msg_words) > 1:
                    msg = msg_words[0]
                    param1 = msg_words[1]
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
                    await self.status_controller.print_user_activity(entity.id, get_display_name(entity), p_types[msg],
                                                                     param1, (param2 == '1'))
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