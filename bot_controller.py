import json
import re
import traceback
from datetime import datetime, timedelta

import apiai
import pyowm
from telethon.tl.types import PeerUser, User, UserStatusOnline, UserStatusOffline

from status_controller import StatusController


class BotController:

    def __init__(self, tg_client):
        self.tg_client = tg_client
        self.bot_answer_format_text = str(self.tg_client.config['chat_bot']['bot_answer_format'])
        self.bot_active = False
        self.active_entity = None
        self.active_entity_id = None
        self.active_entity_is_bot = False
        self.active_entity_client = None
        self.active_entity_name = None
        self.users_rights = {}
        self.users = {}
        self.right_levels = {
            '0': 'all',
            '1': 'contacts',
            '2': 'mutual contacts',
            '3': 'pre selected',
            '4': 'only me'
        }
        self.max_commands = 11
        self.commands = {
            '/start': {
                'cmd': self.cmd_start,
                'places': ['bot'],
                'rights_level': 0,
                'desc': None
            },
            '/help': {
                'cmd': self.cmd_help,
                'places': ['bot', 'dialog'],
                'rights_level': 0,
                'desc': 'краткая справка, это ее ты сейчас видишь'
            },
            '/exit': {
                'cmd': self.cmd_exit,
                'places': ['dialog'],
                'rights_level': 0,
                'desc': 'конец диалога'
            },
            '/devices': {
                'cmd': self.cmd_devices,
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'состояние подключенных устройств и управление ими'
            },
            '/auto': {
                'cmd': self.cmd_auto_answer,
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'автоответчик - начальная настройка или изменение настроек'
            },
            '/auto_back': {
                'places': [],
                'rights_level': 10,
                'desc': None
            },
            '/auto_off': {
                'places': [],
                'rights_level': 10,
                'desc': None
            },
            '/auto_restart': {
                'places': [],
                'rights_level': 10,
                'desc': None
            },
            '/auto_reset_users': {
                'places': [],
                'rights_level': 10,
                'desc': None
            },
            '/user_info': {
                'cmd': self.cmd_user_info,
                'places': ['bot', 'dialog'],
                'rights_level': 2,
                'desc': 'информация о пользователе. Параметр - логин/ID пользователя. Без параметра показывает информацию о тебе'
            },
            '/activity_today': {
                'cmd': self.cmd_activity_today,
                'places': ['bot', 'dialog'],
                'rights_level': 2,
                'desc': 'сессии за сегодня. Параметр - логин/ID пользователя. Без параметра показывает твои сессии'
            },
            '/plot_today': {
                'cmd': self.cmd_plot_today,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности пользователя за сегодня. Параметр - логин/ID пользователя. Без параметра показывает твой график'
            },
            '/plot_all': {
                'cmd': self.cmd_plot_all,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности пользователя за всё время. Параметр - логин/ID пользователя. Без параметра показывает твой график'
            },
            '/plot_week': {
                'cmd': self.cmd_plot_week,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности пользователя за неделю. Параметр - логин/ID пользователя. Без параметра показывает твой график'
            },
            '/plot_hours': {
                'cmd': self.cmd_plot_hours,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности пользователя по часам за всё время. Параметр - логин/ID пользователя. Без параметра показывает твой график'
            },
            '/plot_hours_weekday': {
                'cmd': self.cmd_plot_hours_weekday,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности пользователя по часам за будние дни. Параметр - логин/ID пользователя. Без параметра показывает твой график'
            },
            '/plot_hours_weekend': {
                'cmd': self.cmd_plot_hours_weekend,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности пользователя по часам за выходные. Параметр - логин/ID пользователя. Без параметра показывает твой график'
            }
        }

    def bot_reset(self):
        self.bot_active = False
        self.active_entity = None
        self.active_entity_id = None
        self.active_entity_is_bot = False
        self.active_entity_client = None
        self.active_entity_name = None

    def init_chat_for_user(self, user_id, show_bot_str=True):
        str_user_id = str(user_id)
        self.users[str_user_id] = {
            'entity_client': self.tg_client,
            'entity_is_bot': False,
            'entity': PeerUser(user_id),
            'entity_id': user_id,
        }
        self.active_entity_client = self.tg_client
        self.active_entity_is_bot = False
        self.active_entity = PeerUser(user_id)
        self.active_entity_id = user_id

    def is_active(self):
        return self.bot_active

    def is_active_for_entity(self, entity_id):
        return self.bot_active and (entity_id == self.active_entity_id)

    @staticmethod
    def adapt_command(text):
        text = re.sub(r'[^\w\s!\\?/]', ' ', text)
        text = re.sub(' +', ' ', text)
        return text.strip().lower()

    def text_message(self, message_text):
        try:
            weather_words = str(self.tg_client.config['chat_bot']['bot_weather_keywords'])
            weather_words = weather_words.split('|')
            message_text_words = re.findall(r'\w+', message_text)
            intersect_words = set(message_text_words).intersection(weather_words)
            is_weather = (len(intersect_words) > 0) and self.tg_client.config['chat_bot']['bot_weather_owm_api_key']
            if not is_weather:
                api = apiai.ApiAI(str(self.tg_client.config['chat_bot']['bot_client_token']), str(self.tg_client.config['chat_bot']['bot_session_name']))
            else:
                api = apiai.ApiAI(str(self.tg_client.config['chat_bot']['bot_client_token_weather']), str(self.tg_client.config['chat_bot']['bot_session_name_weather']))
            request = api.text_request()
            request.lang = 'ru'
            request.query = message_text
            response_json = json.loads(request.getresponse().read().decode('utf-8'))

            if is_weather:
                city = None
                if response_json['result']['parameters'] and response_json['result']['parameters']['address'] and response_json['result']['parameters']['address']['city']:
                    city = response_json['result']['parameters']['address']['city']
                if not city:
                    city = str(self.tg_client.config['chat_bot']['bot_weather_city_default'])
                owm = pyowm.OWM(str(self.tg_client.config['chat_bot']['bot_weather_owm_api_key']), language="ru", use_ssl=True)
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
            return str(self.tg_client.config['chat_bot']['bot_error_message'])
        return str(self.tg_client.config['chat_bot']['bot_cant_understand_message'])

    async def bot_check_message(self, message_text, from_id, entity_id, e_type):
        bot_check_message = message_text.replace(',', ' ')
        bot_check_message = self.adapt_command(bot_check_message)
        if self.is_active_for_entity(entity_id) and bot_check_message.startswith(self.tg_client.config['chat_bot']['bot_ignore_prefix']):
            return False
        if self.is_active_for_entity(entity_id) and bot_check_message.startswith(self.tg_client.config['chat_bot']['bot_end_prefix']):
            bot_message = bot_check_message.replace(self.tg_client.config['chat_bot']['bot_end_prefix'], '', 1).strip()
            if not bot_message:
                bot_message = self.tg_client.config['chat_bot']['bot_empty_goodbuy']
            print('Bot command:')
            self.tg_client.sprint('<<< ' + bot_message.replace('\n', ' \\n '))
            await self.bot_command(bot_message, from_id, entity_id, e_type)
            self.bot_reset()
            return True
        elif self.is_active_for_entity(entity_id) or ((self.tg_client.selected_user_activity == entity_id) and (bot_check_message.startswith(self.tg_client.config['chat_bot']['bot_start_prefix']))):
            if not self.is_active_for_entity(entity_id):
                bot_message = bot_check_message.replace(self.tg_client.config['chat_bot']['bot_start_prefix'], '', 1).strip()
            else:
                bot_message = message_text.strip()
            if not bot_message:
                bot_message = self.tg_client.config['chat_bot']['bot_empty_greet']
            print('Bot command:')
            self.tg_client.sprint('<<< ' + message_text.replace('\n', ' \\n '))
            await self.bot_command(bot_message, from_id, entity_id, e_type)
            return True
        return False

    def text_to_bot_text(self, text, place_code=None):
        if not place_code:
            place_code = self.get_curr_place_code()
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
        if not self.active_entity_is_bot:
            return self.bot_answer_format_text.replace('[result]', text)
        else:
            return text

    def get_curr_place_code(self):
        if not self.active_entity_is_bot:
            return 'dialog'
        else:
            return 'bot'

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
        str_user_id = str(user_id)
        if str_user_id not in self.users_rights:
            try:
                entity = await self.tg_client.get_entity(PeerUser(int(user_id)))
                self.users_rights[str_user_id] = self.get_entity_rights_level(entity)
            except:
                self.users_rights[str_user_id] = 0
        return self.users_rights[str_user_id]

    async def bot_command(self, command_text, from_id, from_entity_id, from_entity_type, bot_chat=None):
        if from_entity_type not in ['User', 'Bot']:
            self.bot_reset()
            return
        if not self.is_active_for_entity(from_entity_id):
            self.bot_active = True
        if (from_entity_type=='Bot') and self.tg_client.tg_bot and bot_chat:
            self.active_entity_client = self.tg_client.tg_bot
            self.active_entity_is_bot = True
            self.active_entity = bot_chat
            self.active_entity_id = from_entity_id
        else:
            self.active_entity_client = self.tg_client
            self.active_entity_is_bot = False
            self.active_entity = PeerUser(from_entity_id)
            self.active_entity_id = from_entity_id
        self.active_entity_name = await self.tg_client.get_entity_name(from_entity_id, from_entity_type)

        if not from_id:
            from_id = from_entity_id

        command_parts = command_text.split(' ')
        if (len(command_parts) > 0) and command_parts[0] and (command_parts[0] in self.commands):
            command_code = command_parts[0]
            curr_place = self.get_curr_place_code()
            curr_rights = await self.get_user_rights_level(from_id)
            if (
                    (curr_place in self.commands[command_code]['places']) and
                    (curr_rights >= self.commands[command_code]['rights_level']) and
                    (('condition' not in self.commands[command_code]) or self.commands[command_code]['condition']()) and
                    ('cmd' in self.commands[command_code]) and
                    self.commands[command_code]['cmd']
            ):
                try:
                    await self.commands[command_code]['cmd'](from_id, command_parts[1:])
                except:
                    traceback.print_exc()
                    await self.active_entity_client.send_message(self.active_entity, self.text_to_bot_text('Какая-то ошибка!'))
            else:
                await self.active_entity_client.send_message(self.active_entity, self.text_to_bot_text('Невозможно выполнить команду!'))
            return

        response_text = self.text_message(command_text)
        result_str = self.text_to_bot_text(response_text)
        await self.active_entity_client.send_message(self.active_entity, result_str, silent=True)

    async def cmd_start(self, from_id, params):
        await self.cmd_help(from_id, 'Start')

    async def cmd_help(self, from_id, params):
        result_str = []
        curr_place = self.get_curr_place_code()
        curr_rights = await self.get_user_rights_level(from_id)
        if params == 'Start':
            result_str.append('Привет, я - чат-бот (и не только)')
        commands_results = []
        for k in self.commands.keys():
            if (
                    self.commands[k]['desc'] and
                    (curr_place in self.commands[k]['places']) and
                    (curr_rights >= self.commands[k]['rights_level']) and
                    (('condition' not in self.commands[k]) or self.commands[k]['condition']())
            ):
                commands_results.append(''+ k + ' - ' + str(self.commands[k]['desc']))
        result_str.append('\nСписок доступных для тебя моих команд ('+str(len(commands_results))+'/'+str(self.max_commands)+'):\n')
        result_str = result_str + commands_results
        result_str.append('')
        result_str = ("\n".join(result_str))
        await self.active_entity_client.send_message(self.active_entity, result_str, parse_mode="md", silent=True)

    async def cmd_exit(self, from_id, params):
        await self.active_entity_client.send_message(self.active_entity, self.text_to_bot_text('Диалог прерван'))
        self.bot_reset()

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

    async def user_link(self, user_id, user_name=None):
        if not user_name:
            user_name = await self.tg_client.get_entity_name(user_id, 'User')
        return "["+user_name+"](tg://user?id="+str(user_id)+")"

    async def send_activity_message(self, from_id, to_entity, date_activity=None, result_only_time=False, a_type="plot_img", img_caption="График активности [user]"):
        from_name = await self.tg_client.get_entity_name(from_id)
        status_results = await self.tg_client.status_controller.print_user_activity(from_id, from_name, a_type, date_activity, result_only_time)
        status_results = status_results.strip().splitlines()
        last_str = ''
        if a_type=="plot_img":
            last_str = str(status_results.pop())
        if last_str and last_str.startswith(self.tg_client.config['main']['files_folder'] + "/"):
            u_link = await self.user_link(from_id, from_name)
            await self.active_entity_client.send_file(to_entity, last_str, caption=img_caption.replace('[user]', u_link), force_document=True)
        else:
            full_results = "\n".join(status_results)
            if (len(full_results) + 8) >= 4096:
                status_results.reverse()
                buff_len = 1
                while buff_len > 0:
                    buff = []
                    buff_len = 0
                    while (len(status_results) > 0) and (buff_len + len(status_results[len(status_results) - 1]) + 8) < 4096:
                        last_s = status_results.pop()
                        buff.append(last_s)
                        buff_len = buff_len + len(last_s) + 1
                    if buff_len > 0:
                        await self.active_entity_client.send_message(to_entity, '```\n' + ("\n".join(buff)) + '\n```')
            else:
                await self.active_entity_client.send_message(to_entity, '```\n' + full_results + '\n```')

    async def cmd_user_info(self, from_id, params):
        from_id0 = from_id
        from_id = await self.get_from_id_param(from_id, params)
        entity = await self.tg_client.get_entity(from_id)
        res = []
        res.append('ID: ' + str(entity.id))
        if entity.username:
            res.append('Логин: ' + str(entity.username))
        if entity.phone:
            res.append('Телефон: ' + str(entity.phone))
        if entity.first_name:
            res.append('Имя: ' + str(entity.first_name))
        if entity.last_name:
            res.append('Фамилия: ' + str(entity.last_name))
        res.append('')
        if from_id0 != from_id:
            if entity.mutual_contact:
                res.append('У тебя в контактах, ты у него в контактах')
            elif entity.contact:
                res.append('У тебя в контактах')
        last_date = None
        if type(entity.status) == UserStatusOnline:
            status_name = 'Онлайн'
        elif type(entity.status) == UserStatusOffline:
            status_name = 'Оффлайн'
            last_date = StatusController.tg_datetime_to_local_datetime(entity.status.was_online)
            last_date = StatusController.datetime_to_str(last_date)
        elif entity.status:
            status_name = 'Не отслеживается (' + entity.status.to_dict()['_'] + ')'
        elif entity.bot:
            status_name = 'Бот'
        else:
            status_name = 'Неизвестно'
        res.append('Статус: ' + status_name)
        if last_date:
            res.append('Был в сети: ' + last_date)
        res.append('')
        sessions_cnt = self.tg_client.status_controller.get_user_activity_sessions_count(entity.id)
        if sessions_cnt > 0:
            res.append('Активность отслеживается (сохранено сессий: ' + str(sessions_cnt) + ')')
        else:
            res.append('Активность не отслеживается')
        m_types = self.tg_client.status_controller.get_user_messages_entity_types(entity.id)
        if m_types and len(m_types) > 0:
            res.append('Сообщения отслеживаются (' + (", ".join(m_types)) + ')')
        else:
            res.append('Сообщения не отслеживается')
        await self.active_entity_client.send_message(self.active_entity, self.text_to_bot_text("\n".join(res)))

    async def cmd_activity_today(self, from_id, params):
        from_id = await self.get_from_id_param(from_id, params)
        now_str = StatusController.datetime_to_str(datetime.now(),'%Y-%m-%d')
        await self.send_activity_message(from_id, self.active_entity, now_str, False, "diap")

    async def cmd_plot_today(self, from_id, params):
        from_id = await self.get_from_id_param(from_id, params)
        now_str = StatusController.datetime_to_str(datetime.now(),'%Y-%m-%d')
        await self.send_activity_message(from_id, self.active_entity, now_str, img_caption="График активности [user] за сегодня")

    async def cmd_plot_all(self, from_id, params):
        from_id = await self.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, self.active_entity, None, img_caption="График активности [user] за всё время")

    async def cmd_plot_week(self, from_id, params):
        from_id = await self.get_from_id_param(from_id, params)
        date_str1 = StatusController.datetime_to_str(datetime.now() + timedelta(days=-6), '%Y-%m-%d')
        date_str2 = StatusController.datetime_to_str(datetime.now() + timedelta(days=1), '%Y-%m-%d')
        await self.send_activity_message(from_id, self.active_entity, (date_str1, date_str2), img_caption="График активности [user] за неделю")

    async def cmd_plot_hours(self, from_id, params):
        from_id = await self.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, self.active_entity, None, True, img_caption="График активности [user] по часам")

    async def cmd_plot_hours_weekend(self, from_id, params):
        from_id = await self.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, self.active_entity, "weekend", True, img_caption="График активности [user] по часам за выходные")

    async def cmd_plot_hours_weekday(self, from_id, params):
        from_id = await self.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, self.active_entity, "weekday", True, img_caption="График активности [user] по часам за будние дни")

    async def cmd_devices(self, from_id, params):
        await self.active_entity_client.send_message(self.active_entity, 'Не хватает прав на выполнение команды!')

    async def cmd_auto_answer(self, from_id, params):
        await self.tg_client.aa_controller.begin_setup(from_id, self.active_entity_client, self.active_entity)
