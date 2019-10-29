import codecs
import json
import traceback
import re
from datetime import datetime, timedelta

from bot_action_branch import BotActionBranch


class InstaBranch(BotActionBranch):

    def __init__(self, tg_bot_controller):
        super().__init__(tg_bot_controller)

        self.api = None
        self.has_insta_lib = False
        self.insta_username = None
        self.insta_password = None
        try:
            from libs.instagram_private_api import Client
            print('Instagram lib was found!')
            self.has_insta_lib = True
            self.insta_username = self.get_config_value('instagram', 'username')
            self.insta_password = self.get_config_value('instagram', 'password')
            self.cache_file = self.get_config_value('instagram', 'cache_file')
        except ImportError:
            pass

        self.max_commands = 2
        self.commands = {
            '/insta_check_subscribers': {
                'cmd': self.cmd_check_subscribers,
                'condition': self.is_setup_condition,
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'сверить списки подписчиков'
            },
            '/back': {
                'cmd': self.cmd_back,
                'condition': self.is_setup_condition,
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'вернуться'
            },
        }
        self.on_init_finish()

    @staticmethod
    def to_json(python_object):
        if isinstance(python_object, bytes):
            return {
                '__class__': 'bytes',
                '__value__': codecs.encode(python_object, 'base64').decode()
            }
        raise TypeError(repr(python_object) + ' is not JSON serializable')

    @staticmethod
    def from_json(json_object):
        if '__class__' in json_object and json_object['__class__'] == 'bytes':
            return codecs.decode(json_object['__value__'].encode(), 'base64')
        return json_object

    def do_login_if_need(self):
        if not self.has_insta_lib:
            return
        if self.api:
            return

        print('Trying to login...')

        cache_prefs = None
        if self.cache_file:
            try:
                with open(self.cache_file) as json_data:
                    cache_prefs = json.load(json_data, object_hook=self.from_json)
            except FileNotFoundError:
                print(self.cache_file + " file not found in current directory. Canceling.")
                return

        cache_settings = None
        if cache_prefs and ('cache_settings' in cache_prefs):
            cache_settings = cache_prefs['cache_settings']

        from libs.instagram_private_api import Client
        self.api = Client(
            self.insta_username,
            self.insta_password,
            cookie=None,
            settings=cache_settings,
            on_login=lambda x: self.on_login(x)
        )

    def on_login(self, api):
        if self.cache_file:
            prefs = {
                'cache_settings': api.settings
            }
            with open(self.cache_file, 'w') as prefs_file:
                json.dump(prefs, prefs_file, indent=4, default=self.to_json)
        print('Instagram: Logged in!')
        cookie_expiry = api.cookie_jar.auth_expires
        print('Instagram: Cookie Expiry: {0!s}'.format(datetime.fromtimestamp(cookie_expiry).strftime('%Y-%m-%d %H:%M:%S')))

    def fetch_user_id(self, username):
        info = self.api.username_info(username)
        if info and 'user' in info:
            return info['user']['pk']
        return None

    def can_use_branch(self):
        return self.has_insta_lib and self.insta_username and self.insta_password

    async def cmd_check_subscribers(self, from_id, params):
        self.do_login_if_need()
        if self.api:
            user_id = self.fetch_user_id(self.insta_username)
            print(user_id)
            f_uuid = self.api.generate_uuid()
            followers1 = self.api.user_followers(user_id, f_uuid)
            print(followers1)
            f_uuid = self.api.generate_uuid()
            following1 = self.api.user_following(user_id, f_uuid)
            print(following1)
        await self.send_message_to_user(from_id, 'Пока не запрограммлено!')
