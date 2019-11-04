import asyncio
import codecs
import json
import traceback
from datetime import datetime

from bot_action_branch import BotActionBranch
from branch_insta_stories import InstaStoriesBranch


class InstaBranch(BotActionBranch):

    def __init__(self, tg_bot_controller, branch_parent, branch_code=None):
        super().__init__(tg_bot_controller, branch_parent, branch_code)

        self.use_timer = False
        self.last_stories_check_time = None

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

        self.max_commands = 9
        self.commands.update({
            '/insta_check_followers': {
                'cmd': self.cmd_check_followers,
                'condition': self.can_use_branch,
                'bot_button': {
                    'title': 'Сверка подписчиков',
                    'position': [0, 0],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'сверить списки подписчиков и подписок чтобы найти пользователей, не подписанных на вас из тех, на которых вы подписаны'
            },
            '/insta_check_followings': {
                'cmd': self.cmd_check_followings,
                'condition': self.can_use_branch,
                'bot_button': {
                    'title': 'Сверка подписок',
                    'position': [0, 1],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'сверить списки подписчиков и подписок чтобы найти пользователей, на которых вы не подписаны из тех, что подписанных на вас'
            },
            '/insta_user_info': {
                'cmd': self.cmd_user_info,
                'condition': self.can_use_branch,
                'bot_button': {
                    'title': 'Информация',
                    'position': [1, 0],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'информация о пользователе'
            },
            '/insta_user_locations': {
                'cmd': self.cmd_user_locations,
                'condition': self.can_use_branch,
                'bot_button': {
                    'title': 'Локации',
                    'position': [1, 1],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'все локации, отмеченные пользователем'
            },
            '/insta_user_active_commenters': {
                'cmd': self.cmd_user_active_commenters,
                'condition': self.can_use_branch,
                'bot_button': {
                    'title': 'Активные комментаторы',
                    'position': [2, 0],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'активные в комментах пользователи'
            },
            '/insta_user_active_likers': {
                'cmd': self.cmd_user_active_likers,
                'condition': self.can_use_branch,
                'bot_button': {
                    'title': 'Активные лайкеры',
                    'position': [2, 1],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'активно ставящие лайки пользователи'
            },
            '/insta_stories': {
                'class': InstaStoriesBranch,
                'condition': self.can_use_branch,
                'bot_button': {
                    'title': 'Источники историй',
                    'position': [3, 0],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'управление списками пользователей - источников stories. У этих пользователей будет проводиться регулярное выкачивание stories'
            },
            '/insta_set_username': {
                'cmd': self.cmd_set_username,
                'condition': self.can_set_username,
                'bot_button': {
                    'title': 'Указать имя',
                    'position': [3, 1],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'указать имя пользователя (чтобы каждый раз не спрашивать)'
            },
            '/insta_reset_username': {
                'cmd': self.cmd_reset_username,
                'condition': self.can_reset_username,
                'bot_button': {
                    'title': 'Сбросить имя',
                    'position': [3, 2],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'сбросить имя пользователя (чтобы каждый раз спрашивать)'
            },
        })
        self.on_init_finish()

    async def show_current_branch_commands(self, from_id, post_text=None):
        if post_text is None:
            post_text = self.default_pick_action_text
        uname = self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name(from_id)
        pre_text = ''
        if uname:
            pre_text = pre_text + "\nВыбранный пользователь инстаграм: **" + uname + "**\n"

        await super().show_current_branch_commands(from_id, pre_text + "\n" + post_text)

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

    def get_all_followers(self, api_user_id):
        followers = []
        uuid = self.api.generate_uuid()
        results = self.api.user_followers(api_user_id, uuid, query='')
        followers.extend(results.get('users', []))
        next_max_id = results.get('next_max_id')
        while next_max_id:
            results = self.api.user_followers(api_user_id, uuid, max_id=next_max_id)
            followers.extend(results.get('users', []))
            print('%d followers loaded.' % len(followers))
            next_max_id = results.get('next_max_id')
        return followers

    def get_all_following(self, api_user_id):
        following = []
        uuid = self.api.generate_uuid()
        results = self.api.user_following(api_user_id, uuid, query='')
        following.extend(results.get('users', []))
        next_max_id = results.get('next_max_id')
        while next_max_id:
            results = self.api.user_following(api_user_id, uuid, max_id=next_max_id)
            following.extend(results.get('users', []))
            print('%d following loaded.' % len(following))
            next_max_id = results.get('next_max_id')
        return following

    async def get_all_feed(self, api_user_id, max_count, from_id):
        feed_items = []
        results = self.api.user_feed(api_user_id)
        feed_items.extend(results.get('items', []))
        next_max_id = results.get('next_max_id')
        while next_max_id and (len(feed_items) < max_count):
            await asyncio.sleep(0.4)
            await self.resend_typing_to_user(from_id)
            results = self.api.user_feed(api_user_id, max_id=next_max_id)
            feed_items.extend(results.get('items', []))
            print('%d feed_items loaded.' % len(feed_items))
            next_max_id = results.get('next_max_id')
        return feed_items

    async def get_user_info_by_username(self, from_id, username):
        self.do_login_if_need()
        if not self.api:
            if from_id:
                await self.send_message_to_user(from_id, 'API-клиент не был инициализирован!')
            else:
                print("Can't initialize instagram client!")
            return
        username = str(username).lower().strip()
        try:
            print('Instagram: Getting info for user '+username+'...')
            info = self.api.username_info(username)
            if info and ('user' in info):
                user_id = info['user']['pk']
            else:
                user_id = None
            if not user_id and from_id:
                await self.send_message_to_user(from_id, 'Пользователь с именем "'+username+'" не найден!')
                return None
            print('Instagram: User ' + username + ' was found...')
            return info
        except:
            # traceback.print_exc()
            if from_id:
                await self.send_message_to_user(from_id, 'Пользователь с именем "' + username + '" не найден!')
            else:
                print('Instagram user "' + username + '" was not found!')
        return None

    async def cmd_set_username(self, from_id, params):
        await self.read_bot_str(from_id, self.on_check_set_username, 'Введите имя пользователя Instagram:')

    async def cmd_reset_username(self, from_id, params):
        self.tg_bot_controller.tg_client.entity_controller.save_user_instagram_name(from_id, None)
        await self.send_message_to_user(from_id, 'Сброс имени инстаграм-аккаунта выполнен!')

    async def cmd_user_info(self, from_id, params):
        await self.read_or_run_default(from_id, self.on_info_read_username, self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name, 'Введите имя пользователя Instagram:')

    async def cmd_user_locations(self, from_id, params):
        await self.read_or_run_default(from_id, self.on_locations_read_username, self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name, 'Введите имя пользователя Instagram:')

    async def cmd_user_active_commenters(self, from_id, params):
        await self.read_or_run_default(from_id, self.on_active_commenters_read_username, self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name, 'Введите имя пользователя Instagram:')

    async def cmd_user_active_likers(self, from_id, params):
        await self.read_or_run_default(from_id, self.on_active_likers_read_username, self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name, 'Введите имя пользователя Instagram:')

    async def cmd_check_followers(self, from_id, params):
        await self.read_or_run_default(from_id, self.on_check_read_username, self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name, 'Введите имя пользователя Instagram:', "forward")

    async def cmd_check_followings(self, from_id, params):
        await self.read_or_run_default(from_id, self.on_check_read_username, self.tg_bot_controller.tg_client.entity_controller.get_user_instagram_name, 'Введите имя пользователя Instagram:', "back")

    async def on_check_set_username(self, message, from_id, params):
        info = await self.get_user_info_by_username(from_id, message)
        if info:
            self.tg_bot_controller.tg_client.entity_controller.save_user_instagram_name(from_id, info['user']['username'])
            await self.send_message_to_user(from_id, 'Установка имени инстаграм-аккаунта выполнена!')

    async def on_info_read_username(self, message, from_id, params):
        await self.send_typing_to_user(from_id, True)
        info = await self.get_user_info_by_username(from_id, message)
        if not info:
            await self.send_typing_to_user(from_id, False)
            return

        if info['user']['is_private']:
            profile_type = 'Закрытый'
        else:
            profile_type = 'Обычный'

        pic_name = str(info['user']['pk'])
        if ('profile_pic_id' in info['user']) and info['user']['profile_pic_id']:
            pic_name = str(info['user']['profile_pic_id'])

        results = [
            '**Информация о пользователе инстаграм {}:**'.format(str(info['user']['username'])),
            '',
            'Имя пользователя: ' + str(info['user']['username']),
            'Полное имя: ' + str(info['user']['full_name']),
            'Биография: ' + str(info['user']['biography']),
            'ID пользователя: ' + str(info['user']['pk']),
            'Тип профиля: ' + profile_type,
            'Число медиа: ' + str(info['user']['media_count']),
            'Число подписчиков: ' + str(info['user']['follower_count']),
            'Число подписок: ' + str(info['user']['following_count']),
            'Число тегов в подписках: ' + str(info['user']['following_tag_count']),
            'Фото профиля: [' + pic_name + '](' + str(info['user']['profile_pic_url']) + ')'
        ]
        results = "\n".join(results)

        await self.send_message_to_user(from_id, results, link_preview=True)

    async def on_check_read_username(self, message, from_id, params):
        await self.send_typing_to_user(from_id, True)
        info = await self.get_user_info_by_username(from_id, message)
        if not info:
            await self.send_typing_to_user(from_id, False)
            return

        user_id = int(info['user']['pk'])
        user_name = str(info['user']['username'])

        followers = self.get_all_followers(user_id)
        followings = self.get_all_following(user_id)

        results = [
            '**Сверка подписчиков и подписок пользователя инстаграм {}:**'.format(user_name),
            '',
        ]

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

            if params == "forward":
                if len(following_not_followers) == 0:
                    results.append('Все, на кого подписан ' + user_name + ', подписаны и на него')
                else:
                    results.append('Не все, на кого подписан ' + user_name + ', подписаны и на него, а именно:')
                    results = results + following_not_followers
                results.append('')
            else:
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

    async def on_locations_read_username(self, message, from_id, params):
        await self.send_typing_to_user(from_id, True)
        info = await self.get_user_info_by_username(from_id, message)
        if not info:
            await self.send_typing_to_user(from_id, False)
            return

        await self.send_message_to_user(from_id, 'Процесс может занять некоторое время, подождите...', do_set_next=False)
        await self.send_typing_to_user(from_id)

        user_id = int(info['user']['pk'])
        feed_items = await self.get_all_feed(user_id, 500, from_id)

        results = []
        results.append('Загружено медиа: **{}**'.format(len(feed_items)))
        results.append('')

        locations = []

        for f_item in feed_items:
            if ('location' in f_item) and f_item['location']:
                location_str = []
                if f_item['location']['name']:
                    location_str.append(f_item['location']['name'])
                if f_item['location']['city']:
                    location_str.append(f_item['location']['city'])
                if f_item['location']['address']:
                    location_str.append(f_item['location']['address'])
                location_str = (", ".join(location_str)) + '\n'
                if location_str not in locations:
                    locations.append(location_str)

        if len(locations) > 0:
            results.append('Список локаций из медиа:')
            results.append('')
            results.append('')
            results = results + list(sorted(locations))
        else:
            results.append('Локаций не обнаружено!')

        results = "\n".join(results)
        await self.send_message_to_user(from_id, results)

    async def on_active_commenters_read_username(self, message, from_id, params):
        await self.send_typing_to_user(from_id, True)
        info = await self.get_user_info_by_username(from_id, message)
        if not info:
            await self.send_typing_to_user(from_id, False)
            return

        await self.send_message_to_user(from_id, 'Процесс может занять продолжительное время, подождите...', do_set_next=False)
        await self.send_typing_to_user(from_id)

        user_id = int(info['user']['pk'])
        feed_items = await self.get_all_feed(user_id, 500, from_id)

        all_feed_comments = []

        for f_item in feed_items:
            if f_item['preview_comments'] and len(f_item['preview_comments']) > 0:
                if len(f_item['preview_comments']) == int(f_item['comment_count']):
                    print('All comments in preview for ' + str(f_item['pk']))
                    comments = f_item['preview_comments']
                else:
                    print('Getting comments for ' + str(f_item['pk']))
                    comments = self.api.media_n_comments(f_item['pk'], n=150)
                all_feed_comments = all_feed_comments + comments
                await asyncio.sleep(0.4)
                await self.resend_typing_to_user(from_id)
            else:
                print('No comments for ' + str(f_item['pk']))

        results = []
        results.append('Загружено медиа: **{}**'.format(len(feed_items)))
        results.append('')

        users = {}
        user_infos = {}

        if len(all_feed_comments) == 0:
            results.append('Комментарии к медиа отсутствуют!')
        else:
            results.append('Число загруженных комментариев: **{}**'.format(len(all_feed_comments)))
            results.append('Активные комментаторы:')
            results.append('')
            for comment in all_feed_comments:
                user = comment['user']
                if user['username'] not in users:
                    users[user['username']] = 1
                    user_infos[user['username']] = user
                else:
                    users[user['username']] = users[user['username']] + 1

        users = list(sorted(users.items(), key=lambda kv: kv[1], reverse=True))

        for user in users:
            user_text = self.get_user_tg_text(user_infos[user[0]])
            user_text = user_text + ' (комментов: {})'.format(user[1])
            results.append(user_text)

        results = "\n".join(results)
        await self.send_message_to_user(from_id, results)

    async def on_active_likers_read_username(self, message, from_id, params):
        await self.send_typing_to_user(from_id, True)
        info = await self.get_user_info_by_username(from_id, message)
        if not info:
            await self.send_typing_to_user(from_id, False)
            return

        await self.send_message_to_user(from_id, 'Процесс может занять продолжительное время, подождите...', do_set_next=False)
        await self.send_typing_to_user(from_id)

        user_id = int(info['user']['pk'])
        feed_items = await self.get_all_feed(user_id, 500, from_id)

        all_feed_likes = []

        for f_item in feed_items:
            likers = self.api.media_likers(f_item['pk'])
            all_feed_likes = all_feed_likes + likers['users']
            await asyncio.sleep(0.4)
            await self.resend_typing_to_user(from_id)

        results = []
        results.append('Загружено медиа: **{}**'.format(len(feed_items)))
        results.append('')

        users = {}
        user_infos = {}

        if len(all_feed_likes) == 0:
            results.append('Лайки к медиа отсутствуют!')
        else:
            results.append('Число загруженных лайков: **{}**'.format(len(all_feed_likes)))

            for user in all_feed_likes:
                if user['username'] not in users:
                    users[user['username']] = 1
                    user_infos[user['username']] = user
                else:
                    users[user['username']] = users[user['username']] + 1

            users = list(sorted(users.items(), key=lambda kv: kv[1], reverse=True))
            users_before = []
            filter_after = 1

            if len(users) > 200:
                results.append('Самые активные лайкатели:')
                mid_user = users[round(len(users)/2) - 1]
                filter_after = int(mid_user[1])
                users_before = list(filter(lambda kv: int(kv[1]) <= filter_after, users))
                users_after = list(filter(lambda kv: int(kv[1]) > filter_after, users))
                while len(users_after) < 10:
                    filter_after = filter_after - 1
                    users_before = list(filter(lambda kv: int(kv[1]) <= filter_after, users))
                    users_after = list(filter(lambda kv: int(kv[1]) > filter_after, users))
                users = users_after
            else:
                results.append('Активные лайкатели:')
            results.append('')

            for user in users:
                user_text = self.get_user_tg_text(user_infos[user[0]])
                user_text = user_text + ' (лайков: {})'.format(user[1])
                results.append(user_text)

            if (len(users_before) > 0) and (filter_after > 0):
                results.append('')
                results.append('Помимо вышеперечисленных, есть ещё пользователи, ставившие лайки.')
                results.append('Всего их **{}**. Каждый ставил количество лайков, меньше **{}**'.format(len(users_before), filter_after + 1))

        results = "\n".join(results)
        await self.send_message_to_user(from_id, results)
