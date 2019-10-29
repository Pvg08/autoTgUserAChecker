import traceback
from datetime import datetime

from telethon.errors import ChatIdInvalidError
from telethon.tl.types import User, PeerUser, PeerChat, Chat, PeerChannel, Channel
from telethon.utils import get_display_name

from status_controller import StatusController


class EntityController():
    def __init__(self, tg_client):
        self.tg_client = tg_client
        self.db_conn = tg_client.db_conn

        self.user_logins = {}
        self.channel_names = {}
        self.channel_megagroups = {}
        self.chat_names = {}

    @staticmethod
    def init_db(c):
        c.execute("""
            CREATE TABLE IF NOT EXISTS "entities" (
                "entity_id" INTEGER NOT NULL,
                "entity_type" TEXT NULL,
                "entity_name" TEXT NULL,
                "entity_phone" TEXT NULL,
                "taken_at" DATETIME NOT NULL,
                "to_answer_sec" REAL,
                "from_answer_sec" REAL,
                "instagram_username" TEXT NULL,
                "version" INTEGER NOT NULL
            );
        """)

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
        c.execute('INSERT INTO `entities` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)', [
            str(entity_id), str(entity_type), entity_name, entity_phone,
            StatusController.datetime_to_str(datetime.now()),
            None,
            None,
            None,
            str(version)
        ])
        self.db_conn.commit()

    def get_user_instagram_name(self, user_id):
        try:
            row = self.db_conn.execute("""
                SELECT * FROM `entities` WHERE `entity_id` = ? ORDER BY `version` DESC LIMIT 1
            """, [str(user_id)]).fetchone()
            if row and ('instagram_username' in row) and row['instagram_username']:
                return row['instagram_username']
        except:
            traceback.print_exc()
        return None

    def set_user_instagram_name(self, user_id, username):
        try:
            row = self.db_conn.execute("""
                SELECT * FROM `entities` WHERE `entity_id` = ? ORDER BY `version` DESC LIMIT 1
            """, [str(user_id)]).fetchone()
            if row and ('version' in row):
                version = int(row['version'])
            else:
                version = 1
            c = self.db_conn.cursor()
            c.execute("""
                UPDATE `entities` SET `instagram_username` = ?
                WHERE `entity_id` = ? AND `version` = ?
            """, [
                username, str(user_id), str(version)
            ])
            self.db_conn.commit()
        except:
            traceback.print_exc()

    def channel_is_megagroup(self, channel_id):
        str_channel_id = str(channel_id)
        if str_channel_id in self.channel_megagroups:
            return self.channel_megagroups[str_channel_id]
        return False

    @staticmethod
    def get_user_name_text(user: User):
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

    async def get_entity_name(self, entity_id, entity_type='', allow_str_id=True, with_additional_text=False):
        if not entity_id:
            return None
        str_entity_id = str(entity_id)
        if entity_type in ['User', 'Bot']:
            if str_entity_id not in self.user_logins:
                if with_additional_text:
                    print('Trying to find user login for id=' + str_entity_id)
                try:
                    entity = await self.tg_client.get_entity(PeerUser(entity_id))
                    if type(entity) != User:
                        raise ValueError(str_entity_id + ' is not user')
                    if entity.bot and (entity_type == 'User'):
                        entity_type = 'Bot'
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
                    entity = await self.tg_client.get_entity(PeerChat(entity_id))
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
                    entity = await self.tg_client.get_entity(PeerChannel(entity_id))
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

    async def add_entity_dialog_messages_to_db(self, entity, limit, e_id=None, e_type=None):
        if (not e_id) or (not e_type):
            if type(entity) == User:
                e_type = 'User'
                e_id = entity.id
                if entity.bot:
                    e_type = 'Bot'
                self.add_entity_db_name(e_id, e_type, self.get_user_name_text(entity), entity.phone, False)
            elif type(entity) == Chat:
                e_type = 'Chat'
                e_id = entity.id
                self.chat_names[str(e_id)] = get_display_name(entity)
                self.add_entity_db_name(e_id, e_type, get_display_name(entity), None, False)
            elif type(entity) == Channel:
                e_type = 'Channel'
                if entity.megagroup:
                    e_type = 'Megagroup'
                e_id = entity.id
                self.channel_megagroups[str(e_id)] = entity.megagroup
                self.channel_names[str(e_id)] = get_display_name(entity)
                self.add_entity_db_name(e_id, e_type, get_display_name(entity), None, entity.megagroup)
            else:
                return

        messages = await self.tg_client.get_messages(entity, limit=limit)
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

            message_text = self.tg_client.get_dict_message_text(data_message)
            self.tg_client.add_message_to_db(e_id, e_type, from_id, to_id, message_i.id, message_text, t_date, 0)

    def process_entity(self, entity):
        if type(entity) == User:
            e_type = 'User'
            e_id = entity.id
            if entity.bot:
                e_type = 'Bot'
            self.add_entity_db_name(e_id, e_type, self.get_user_name_text(entity), entity.phone, False)
        elif type(entity) == Chat:
            e_type = 'Chat'
            e_id = entity.id
            self.chat_names[str(e_id)] = get_display_name(entity)
            self.add_entity_db_name(e_id, e_type, get_display_name(entity), None, False)
        elif type(entity) == Channel:
            e_type = 'Channel'
            if entity.megagroup:
                e_type = 'Megagroup'
            e_id = entity.id
            self.channel_megagroups[str(e_id)] = entity.megagroup
            self.channel_names[str(e_id)] = get_display_name(entity)
            self.add_entity_db_name(e_id, e_type, get_display_name(entity), None, entity.megagroup)
        else:
            e_type = ''
            e_id = None

        return e_id, e_type
