import json
import re
import traceback
from datetime import datetime, timedelta

import apiai
import pyowm
from telethon.tl.types import PeerUser, User, UserStatusOnline, UserStatusOffline

from bot_action_branch import BotActionBranch
from auto_answers import AutoAnswers
from branch_activity import ActivityBranch
from branch_dialogue import DialogueBranch
from branch_insta import InstaBranch
from branch_tools import ToolsBranch
from status_controller import StatusController


class BotController(BotActionBranch):

    def __init__(self, tg_client):
        super().__init__(self)
        self.tg_client = tg_client
        self.bot_answer_format_text = str(self.get_config_value('chat_bot', 'bot_answer_format'))
        self.users = {}
        self.right_levels = {
            '0': 'all',
            '1': 'contacts',
            '2': 'mutual contacts',
            '3': 'pre selected',
            '4': 'only me'
        }
        self.max_commands = 5
        self.commands.update({
            '/start': {
                'cmd': self.cmd_start,
                'places': ['bot'],
                'rights_level': 0,
                'desc': None
            },
            '/exit': {
                'cmd': self.cmd_exit,
                'places': ['dialog'],
                'rights_level': 0,
                'desc': 'конец диалога'
            },
            '/insta_check': {
                'class': InstaBranch,
                'places': ['bot'],
                'bot_button': {
                    'title': 'Инстаграм',
                    'position': [1, 0],
                },
                'rights_level': 0,
                'desc': 'управление скриптом для работы с инстаграмом.'
            },
            '/auto': {
                'class': AutoAnswers,
                'places': ['bot'],
                'bot_button': {
                    'title': 'Автоответчик',
                    'position': [2, 0],
                },
                'rights_level': 3,
                'desc': 'автоответчик - начальная настройка или изменение настроек.',
            },
            '/tools': {
                'class': ToolsBranch,
                'places': ['bot'],
                'bot_button': {
                    'title': 'Утилиты',
                    'position': [3, 0],
                },
                'rights_level': 1,
                'desc': 'всякие разные утилиты.'
            },
            '/user_dialogue_info': {
                'class': DialogueBranch,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': 'Диалоги',
                    'position': [4, 0],
                },
                'rights_level': 2,
                'desc': 'статистика диалогов с пользователем [me_user].'
            },
            '/user_activity_info': {
                'class': ActivityBranch,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': 'Активность пользователей',
                    'position': [4, 1],
                },
                'rights_level': 2,
                'desc': 'статистика активности пользователей - графики и листинги.'
            },
        })
        self.on_init_finish()

    def is_active_for_user(self, entity_id, check_is_bot_dialog=None):
        if (str(entity_id) in self.users) and self.users[str(entity_id)]['active']:
            if check_is_bot_dialog is not None:
                return self.users[str(entity_id)]['is_bot_dialog'] == check_is_bot_dialog
            return True
        return False

    def get_user_place_code(self, user_id):
        if self.is_active_for_user(user_id):
            if self.users[str(user_id)]['is_bot_dialog']:
                return 'bot'
            else:
                return 'dialog'
        return 'nothing'

    def get_user_param(self, user_id, param_name, default_value=None, default_if_inactive=True):
        str_user_id = str(user_id)
        if default_if_inactive:
            if self.is_active_for_user(user_id):
                return self.users[str_user_id][param_name]
        elif str_user_id in self.users:
            return self.users[str_user_id][param_name]
        return default_value

    def get_user_show_as_bot(self, user_id):
        return self.get_user_param(user_id, 'show_as_bot')

    def get_user_branch(self, user_id):
        return self.get_user_param(user_id, 'branch')

    def get_user_message_client(self, user_id):
        return self.get_user_param(user_id, 'message_client')

    def get_user_dialog_entity(self, user_id):
        return self.get_user_param(user_id, 'dialog_entity')

    def get_user_message_id(self, user_id, get_active=True):
        return self.get_user_param(user_id, 'active_message_id' if get_active else 'last_message_id')

    def get_user_next_message_id(self, user_id):
        return self.get_user_param(user_id, 'last_next_message_id')

    async def get_user_rights_level_realtime(self, user_id):
        return self.get_user_param(user_id, 'rights', -1, False)

    def stop_chat_with_all_users(self):
        for k in self.users.keys():
            self.users[k]['active'] = False
            self.users[k]['show_as_bot'] = None

    def set_branch_for_user(self, user_id, branch):
        str_user_id = str(user_id)
        if str_user_id in self.users:
            self.users[str_user_id]['branch'] = branch

    def set_message_for_user(self, user_id, message_id, set_active=True, set_next=False):
        str_user_id = str(user_id)
        if str_user_id in self.users:
            if set_active:
                self.users[str_user_id]['active_message_id'] = message_id
            if set_next:
                self.users[str_user_id]['last_next_message_id'] = message_id
            if message_id:
                self.users[str_user_id]['last_message_id'] = message_id

    def stop_chat_with_user(self, user_id):
        str_user_id = str(user_id)
        if str_user_id in self.users:
            self.users[str_user_id]['active'] = False
            self.users[str_user_id]['show_as_bot'] = None
            if (
                (user_id == self.tg_client.me_user_id) and
                (not self.users[str_user_id]['is_bot_dialog']) and
                (type(self.users[str_user_id]['dialog_entity']) == PeerUser) and
                (self.users[str_user_id]['dialog_entity'].user_id != user_id) and
                self.is_active_for_user(self.users[str_user_id]['dialog_entity'].user_id, False)
            ):
                self.stop_chat_with_user(self.users[str_user_id]['dialog_entity'].user_id)
            elif (
                self.is_active_for_user(self.tg_client.me_user_id, False) and
                (type(self.users[str(self.tg_client.me_user_id)]['dialog_entity']) == PeerUser) and
                (self.users[str(self.tg_client.me_user_id)]['dialog_entity'].user_id == user_id)
            ):
                self.stop_chat_with_user(self.tg_client.me_user_id)

    async def init_chat_for_user(self, user_id, entity_id=None, in_bot=False, show_as_bot=None):
        str_user_id = str(user_id)

        if not entity_id:
            entity_id = user_id

        if in_bot and self.tg_client.tg_bot:
            bot_chat_dialog = self.tg_client.entity_controller.get_user_bot_chat(user_id)
            if not bot_chat_dialog and self.tg_client.tg_bot:
                bot_chat_dialog = await self.tg_client.tg_bot.get_input_entity(PeerUser(user_id))
            is_bot_dialog = True
            message_client = self.tg_client.tg_bot
        else:
            is_bot_dialog = False
            message_client = self.tg_client
            bot_chat_dialog = PeerUser(entity_id)

        if str_user_id not in self.users:
            try:
                entity = await self.tg_client.get_entity(PeerUser(int(user_id)))
                rights = self.get_entity_rights_level(entity)
            except:
                rights = 0
            self.users[str_user_id] = {
                'active': True,
                'branch': None,
                'active_message_id': None,
                'last_message_id': None,
                'last_next_message_id': None,
                'rights': rights,
                'show_as_bot': show_as_bot,
                'message_client': message_client,
                'is_bot_dialog': is_bot_dialog,
                'dialog_entity': bot_chat_dialog,
                'entity_user_id': user_id,
                'entity_user_name': await self.tg_client.get_entity_name(user_id, 'User')
            }
        else:
            self.users[str_user_id]['active'] = True
            if show_as_bot is not None:
                self.users[str_user_id]['show_as_bot'] = show_as_bot
            self.users[str_user_id]['message_client'] = message_client
            self.users[str_user_id]['is_bot_dialog'] = is_bot_dialog
            self.users[str_user_id]['dialog_entity'] = bot_chat_dialog

    def chatbot_message_response(self, message_text, user_id):
        try:
            weather_words = str(self.get_config_value('chat_bot', 'bot_weather_keywords'))
            weather_words = weather_words.split('|')
            message_text_words = re.findall(r'\w+', message_text)
            intersect_words = set(message_text_words).intersection(weather_words)
            is_weather = (len(intersect_words) > 0) and self.get_config_value('chat_bot', 'bot_weather_owm_api_key')
            if not is_weather:
                api = apiai.ApiAI(str(self.get_config_value('chat_bot', 'bot_client_token')), str(self.get_config_value('chat_bot', 'bot_session_name')) + '_' + str(user_id))
            else:
                api = apiai.ApiAI(str(self.get_config_value('chat_bot', 'bot_client_token_weather')), str(self.get_config_value('chat_bot', 'bot_session_name_weather')) + '_' + str(user_id))
            request = api.text_request()
            request.lang = 'ru'
            request.query = message_text
            response_json = json.loads(request.getresponse().read().decode('utf-8'))

            if is_weather:
                city = None
                if response_json['result']['parameters'] and response_json['result']['parameters']['address'] and response_json['result']['parameters']['address']['city']:
                    city = response_json['result']['parameters']['address']['city']
                if not city:
                    city = str(self.get_config_value('chat_bot', 'bot_weather_city_default'))
                owm = pyowm.OWM(str(self.get_config_value('chat_bot', 'bot_weather_owm_api_key')), language="ru", use_ssl=True)
                observation = owm.weather_at_place(city)
                w = observation.get_weather()
                status = w.get_detailed_status() + ' (облачность '+str(w.get_clouds())+'%)'
                pressure_res = w.get_pressure()
                pressure = str(pressure_res.get('press')) + ' мм.рт.ст.'
                visibility_distance = str(w.get_visibility_distance()) + ' м.'
                wind_res = w.get_wind()
                wind_speed = str(wind_res.get('speed')) + ' м/с'
                humidity = str(w.get_humidity()) + '%'
                celsius_result = w.get_temperature('celsius')
                temp_min_celsius = str(celsius_result.get('temp_min')) + '°C'
                temp_max_celsius = str(celsius_result.get('temp_max')) + '°C'
                response = "Текущая погода в г. " + city + ": \n" + \
                           status + "\n" + \
                           "Температура:\n    Макс: " + temp_max_celsius + "\n    Мин: " + temp_min_celsius + "\n"+\
                           "Влажность: " + humidity + "\n"+\
                           "Давление: " + pressure + "\n"+\
                           "Видимость: " + visibility_distance + "\n"+\
                           "Ветер: " + wind_speed + "\n"
            else:
                response = response_json['result']['fulfillment']['speech']
            if response:
                return response
        except:
            traceback.print_exc()
            return str(self.get_config_value('chat_bot', 'bot_error_message'))
        return str(self.get_config_value('chat_bot', 'bot_cant_understand_message'))

    async def bot_check_user_message(self, message_text, from_id, dialog_entity_id, dialog_entity_type):
        bot_check_message = message_text.replace(',', ' ')
        bot_check_message = self.adapt_command(bot_check_message)
        if self.is_active_for_user(dialog_entity_id, False) and bot_check_message.startswith(self.get_config_value('chat_bot', 'bot_ignore_prefix')):
            return False
        if self.is_active_for_user(dialog_entity_id, False) and bot_check_message.startswith(self.get_config_value('chat_bot', 'bot_end_prefix')):
            bot_message = bot_check_message.replace(self.get_config_value('chat_bot', 'bot_end_prefix'), '', 1).strip()
            if not bot_message:
                bot_message = self.get_config_value('chat_bot', 'bot_empty_goodbuy')
            print('Bot command:')
            self.tg_client.sprint('<<< ' + bot_message.replace('\n', ' \\n '))
            await self.bot_command(bot_message, from_id, dialog_entity_id, dialog_entity_type)
            self.stop_chat_with_user(dialog_entity_id)
            return True
        elif self.is_active_for_user(dialog_entity_id, False) or ((self.tg_client.selected_user_activity == dialog_entity_id) and (bot_check_message.startswith(self.get_config_value('chat_bot', 'bot_start_prefix')))):
            if not self.is_active_for_user(dialog_entity_id, False):
                bot_message = bot_check_message.replace(self.get_config_value('chat_bot', 'bot_start_prefix'), '', 1).strip()
            else:
                bot_message = message_text.strip()
            if not bot_message:
                bot_message = self.get_config_value('chat_bot', 'bot_empty_greet')
            print('Bot command:')
            self.tg_client.sprint('<<< ' + message_text.replace('\n', ' \\n '))
            await self.bot_command(bot_message, from_id, dialog_entity_id, dialog_entity_type)
            return True
        return False

    def get_entity_rights_level(self, entity, max_level_user_ids=None):
        if not max_level_user_ids:
            max_level_user_ids = [self.tg_client.me_user_id]
        if type(entity) == User:
            if entity.id in max_level_user_ids:
                return 4
            elif entity.bot:
                return -1
            elif entity.id == self.tg_client.selected_user_activity:
                return 3
            elif entity.mutual_contact:
                return 2
            elif entity.contact:
                return 1
        return 0

    async def get_user_rights_level(self, user_id):
        try:
            entity = await self.tg_client.get_entity(PeerUser(int(user_id)))
            rights = self.get_entity_rights_level(entity)
        except:
            rights = 0
        return rights

    def text_to_bot_text(self, text, user_id, place_code=None):
        if not place_code:
            place_code = self.get_user_place_code(user_id)
        if place_code != 'bot':
            text = re.sub(r"\[bot_only\].*\[/bot_only\]", '', text, flags=re.MULTILINE | re.IGNORECASE)
            text = text.replace('[dialog_only]', '')
            text = text.replace('[/dialog_only]', '')
            text = text.strip()
        if place_code != 'dialog':
            text = re.sub(r"\[dialog_only\].*\[/dialog_only\]", '', text, flags=re.MULTILINE | re.IGNORECASE)
            text = text.replace('[bot_only]', '')
            text = text.replace('[/bot_only]', '')
            text = text.strip()
        show_as_bot = self.get_user_show_as_bot(user_id)
        if (place_code != 'bot') and ((show_as_bot is None) or show_as_bot):
            return self.bot_answer_format_text.replace('[result]', text)
        else:
            return text

    #                from_id    from_entity_id    from_entity_type
    # USER -> ME     user_id    user_id           User
    # ME -> USER     me_id      user_id           User
    # USER -> BOT    user_id    bot_id            Bot
    # ME -> BOT      me_id      bot_id            Bot
    async def bot_command(self, command_text, from_id, from_entity_id, from_entity_type):

        user_branch = self.get_user_branch(from_id)

        if user_branch and (user_branch in self.branches):
            if await user_branch.on_bot_message(command_text, from_id):
                return

        if from_entity_type not in ['User', 'Bot']:
            self.stop_chat_with_user(from_entity_id)
            return

        chatbot_session_user_id = from_entity_id
        if not from_id:
            from_id = from_entity_id

        if from_entity_type=='Bot':
            await self.init_chat_for_user(from_id, from_id, True)
            chatbot_session_user_id = from_id
        else:
            await self.init_chat_for_user(from_id, from_entity_id)
            if (from_entity_id != from_id) and not self.is_active_for_user(from_entity_id, False):
                await self.init_chat_for_user(from_entity_id)

        if await self.run_command_text(command_text, from_id):
            return

        response_text = self.chatbot_message_response(command_text, chatbot_session_user_id)
        await self.send_message_to_user(from_id, response_text)

    async def cmd_start(self, from_id, params):
        await self.cmd_help(from_id, 'Start')

    async def cmd_exit(self, from_id, params):
        await self.send_message_to_user(from_id, 'Диалог прерван')
        self.stop_chat_with_user(from_id)

    async def get_from_id_param(self, from_id, params):
        if params and (len(params) > 0) and params[0]:
            try:
                entity = await self.tg_client.get_entity(str(params[0]).strip())
            except ValueError:
                try:
                    entity = await self.tg_client.get_entity(PeerUser(int(params[0])))
                except:
                    entity = None
            if type(entity) == User:
                from_id = entity.id
        return from_id
