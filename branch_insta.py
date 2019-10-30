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

        self.max_commands = 4
        self.commands = {
            '/insta_set_username': {
                'cmd': self.cmd_set_username,
                'condition': self.can_set_username,
                'places': ['bot'],
                'rights_level': 1,
                'desc': 'указать имя пользователя (чтобы каждый раз не спрашивать)'
            },
            '/insta_reset_username': {
                'cmd': self.cmd_reset_username,
                'condition': self.can_reset_username,
                'places': ['bot'],
                'rights_level': 1,
                'desc': 'сбросить имя пользователя (чтобы каждый раз спрашивать)'
            },
            '/insta_user_info': {
                'cmd': self.cmd_user_info,
                'condition': self.is_setup_condition,
                'places': ['bot'],
                'rights_level': 1,
                'desc': 'информация о пользователе'
            },
            '/insta_check_followers': {
                'cmd': self.cmd_check_followers,
                'condition': self.is_setup_condition,
                'places': ['bot'],
                'rights_level': 1,
                'desc': 'сверить списки подписчиков и подписок'
            },
            '/back': {
                'cmd': self.cmd_back,
                'condition': self.is_setup_condition,
                'places': ['bot'],
                'rights_level': 1,
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

        print('Instagram: Trying to login...')

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
        print('Instagram: Client is ready...')

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

    def can_use_branch(self, user_id):
        return self.has_insta_lib and self.insta_username and self.insta_password

    def can_set_username(self, user_id):
        uname = self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name(user_id)
        if uname:
            return False
        return self.can_use_branch(user_id)

    def can_reset_username(self, user_id):
        uname = self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name(user_id)
        if not uname:
            return False
        return self.can_use_branch(user_id)

    @staticmethod
    def get_user_tg_text(user):
        base_str = '[' + user['username'] + '](https://www.instagram.com/'+user['username']+'/)'
        if user['full_name']:
            base_str = base_str + ' - ' + user['full_name']
        if user['is_private']:
            base_str = base_str + ' (закрытый)'
        return base_str

    def get_all_followers(self, user_id):
        followers = []
        uuid = self.api.generate_uuid()
        results = self.api.user_followers(user_id, uuid, query='')
        followers.extend(results.get('users', []))
        next_max_id = results.get('next_max_id')
        while next_max_id:
            results = self.api.user_followers(user_id, uuid, max_id=next_max_id)
            followers.extend(results.get('users', []))
            print('%d followers loaded.' % len(followers))
            next_max_id = results.get('next_max_id')
        return followers

    def get_all_following(self, user_id):
        following = []
        uuid = self.api.generate_uuid()
        results = self.api.user_following(user_id, uuid, query='')
        following.extend(results.get('users', []))
        next_max_id = results.get('next_max_id')
        while next_max_id:
            results = self.api.user_following(user_id, uuid, max_id=next_max_id)
            following.extend(results.get('users', []))
            print('%d following loaded.' % len(following))
            next_max_id = results.get('next_max_id')
        return following

    async def get_user_info_by_username(self, from_id, username):
        self.do_login_if_need()
        if not self.api:
            await self.send_message_to_user(from_id, 'API-клиент не был инициализирован!')
            return
        username = str(username).lower().strip()
        try:
            print('Instagram: Getting info for user '+username+'...')
            info = self.api.username_info(username)
            if info and ('user' in info):
                user_id = info['user']['pk']
            else:
                user_id = None
            if not user_id:
                await self.send_message_to_user(from_id, 'Пользователь с именем "'+username+'" не найден!')
                await self.show_current_branch_commands(from_id)
                return None
            print('Instagram: User ' + username + ' was found...')
            return info
        except:
            traceback.print_exc()
            await self.send_message_to_user(from_id, 'Пользователь с именем "' + username + '" не найден!')
            await self.show_current_branch_commands(from_id)
        return None

    async def cmd_user_info(self, from_id, params):
        uname = self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name(from_id)
        if uname:
            await self.on_info_read_username(uname, from_id)
        else:
            await self.read_bot_str(from_id, self.on_info_read_username, 'Введите имя пользователя Instagram:')

    async def cmd_check_followers(self, from_id, params):
        uname = self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name(from_id)
        if uname:
            await self.on_check_read_username(uname, from_id)
        else:
            await self.read_bot_str(from_id, self.on_check_read_username, 'Введите имя пользователя Instagram:')

    async def cmd_set_username(self, from_id, params):
        await self.read_bot_str(from_id, self.on_check_set_username, 'Введите имя пользователя Instagram:')

    async def cmd_reset_username(self, from_id, params):
        self.tg_bot_controller.tg_client.entity_controller.save_user_instagram_name(from_id, None)
        await self.send_message_to_user(from_id, 'Выполнено!')
        await self.show_current_branch_commands(from_id)

    async def on_check_set_username(self, message, from_id):
        info = await self.get_user_info_by_username(from_id, message)
        if info:
            self.tg_bot_controller.tg_client.entity_controller.save_user_instagram_name(from_id, info['user']['username'])
            await self.send_message_to_user(from_id, 'Выполнено!')
            await self.show_current_branch_commands(from_id)

    async def on_info_read_username(self, message, from_id):
        info = await self.get_user_info_by_username(from_id, message)
        if not info:
            return

        if info['user']['is_private']:
            profile_type = 'Закрытый'
        else:
            profile_type = 'Обычный'

        pic_name = str(info['user']['pk'])
        if ('profile_pic_id' in info['user']) and info['user']['profile_pic_id']:
            pic_name = str(info['user']['profile_pic_id'])

        results = [
            'Имя пользователя: ' + str(info['user']['username']),
            'Полное имя: ' + str(info['user']['full_name']),
            'Биография: ' + str(info['user']['biography']),
            'ID пользователя: ' + str(info['user']['pk']),
            'Тип профиля: ' + profile_type,
            'Число медиа: ' + str(info['user']['media_count']),
            'Число подписчиков: ' + str(info['user']['follower_count']),
            'Число подписок: ' + str(info['user']['following_count']),
            'Фото профиля: [' + pic_name + '](' + str(info['user']['profile_pic_url']) + ')'
        ]
        results = "\n".join(results)

        await self.send_message_to_user(from_id, results, link_preview=True)
        await self.show_current_branch_commands(from_id)

    async def on_check_read_username(self, message, from_id):
        info = await self.get_user_info_by_username(from_id, message)
        if not info:
            return

        user_id = int(info['user']['pk'])
        user_name = str(info['user']['username'])

        followers = self.get_all_followers(user_id)
        followings = self.get_all_following(user_id)

        results = []

        if (len(followers) > 0) and (len(followings) > 0):
            results.append('Число подписчиков: ' + str(len(followers)))
            results.append('Число подписок: ' + str(len(followings)))
            results.append('')

            followers_names = {}
            following_names = {}
            for follower in followers:
                followers_names[str(follower['pk'])] = self.get_user_tg_text(follower)
            for following in followings:
                following_names[str(following['pk'])] = self.get_user_tg_text(following)
            followers_ids = [x['pk'] for x in followers]
            following_ids = [x['pk'] for x in followings]

            followers_not_following = list(map(lambda x: followers_names[str(x)], filter(lambda x: x not in following_ids, followers_ids)))
            following_not_followers = list(map(lambda x: following_names[str(x)], filter(lambda x: x not in followers_ids, following_ids)))

            if len(following_not_followers) == 0:
                results.append('Все, на кого подписан ' + user_name + ', подписаны и на него')
            else:
                results.append('Не все, на кого подписан ' + user_name + ', подписаны и на него, а именно:')
                results = results + following_not_followers
            results.append('')

            if len(followers_not_following) == 0:
                results.append(user_name + ' подписан на всех своих подписчиков')
            else:
                results.append(user_name + ' подписан не на всех своих подписчиков, а именно:')
                results = results + followers_not_following
            results.append('')
        else:
            if info['user']['is_private']:
                results.append('У этого пользователя закрытый профиль! Прочитать списки подписок и подписчиков не получится!')
            else:
                results.append('У этого пользователя не нашлось ни подписок ни подписчиков!')

        results = "\n".join(results)

        await self.send_message_to_user(from_id, results)
        await self.show_current_branch_commands(from_id)
