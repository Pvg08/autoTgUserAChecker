import asyncio
from datetime import timedelta, datetime

from telethon.tl.types import UserStatusOnline, UserStatusOffline

from bot_action_branch import BotActionBranch
from status_controller import StatusController


class ActivityBranch(BotActionBranch):

    def __init__(self, tg_bot_controller, branch_parent, branch_code=None):
        super().__init__(tg_bot_controller, branch_parent, branch_code)

        self.me_picker_cmd = None

        self.max_commands = 9
        self.commands.update({
            '/activity_today': {
                'cmd': self.cmd_activity_today,
                'bot_button': {
                    'title': 'сессии за сегодня',
                    'position': [0, 0],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 2,
                'desc': 'вывод сессий за сегодня.'
            },
            '/plot_today': {
                'cmd': self.cmd_plot_today,
                'bot_button': {
                    'title': 'график (сегодня)',
                    'position': [1, 0],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности за сегодня.'
            },
            '/plot_week': {
                'cmd': self.cmd_plot_week,
                'bot_button': {
                    'title': 'график (неделя)',
                    'position': [1, 1],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности за неделю.'
            },
            '/plot_all': {
                'cmd': self.cmd_plot_all,
                'bot_button': {
                    'title': 'график (всё)',
                    'position': [1, 2],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности за всё время.'
            },
            '/plot_hours': {
                'cmd': self.cmd_plot_hours,
                'bot_button': {
                    'title': 'по часам (всё)',
                    'position': [2, 0],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности по часам за всё время в виде диаграммы.'
            },
            '/plot_hours_weekday': {
                'cmd': self.cmd_plot_hours_weekday,
                'bot_button': {
                    'title': 'по часам (будние)',
                    'position': [2, 1],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности по часам за будние дни в виде диаграммы.'
            },
            '/plot_hours_weekend': {
                'cmd': self.cmd_plot_hours_weekend,
                'bot_button': {
                    'title': 'по часам (выходные)',
                    'position': [2, 2],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности по часам за выходные в виде диаграммы.'
            },
            '/user_info': {
                'cmd': self.cmd_user_info,
                'bot_button': {
                    'title': 'общая информация',
                    'position': [3, 0],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 2,
                'desc': 'информация о пользователе.'
            },
        })
        self.command_groups.update({
            '/activity_today': 'Нижеперечисленные команды предназначены для получения статистики активности отслеживаемых пользователей',
            '/plot_today': None,
            '/user_info': None
        })
        self.on_init_finish()

    async def send_activity_message(self, for_id, send_to_id, date_activity=None, result_only_time=False, a_type="plot_img", img_caption="График активности [user]"):
        for_name = await self.tg_bot_controller.tg_client.get_entity_name(for_id)
        status_results = await self.tg_bot_controller.tg_client.status_controller.print_user_activity(for_id, for_name, a_type, date_activity, result_only_time)
        status_results = status_results.strip().splitlines()
        last_str = ''
        if a_type=="plot_img":
            last_str = str(status_results.pop())
        if last_str and last_str.startswith(self.get_config_value('main', 'files_folder') + "/"):
            u_link = await self.user_link(for_id, for_name)
            await self.send_file_to_user(send_to_id, last_str, img_caption.replace('[user]', u_link), True)
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
                        await self.send_message_to_user(send_to_id, '```\n' + ("\n".join(buff)) + '\n```')
                        if len(status_results) > 0:
                            await asyncio.sleep(0.75)
            else:
                await self.send_message_to_user(send_to_id, '```\n' + full_results + '\n```')

    async def cmd_activity_today(self, from_id, params):
        to_id = from_id
        from_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        now_str = StatusController.datetime_to_str(datetime.now(),'%Y-%m-%d')
        await self.send_activity_message(from_id, to_id, now_str, True, "diap")

    async def cmd_plot_today(self, from_id, params):
        to_id = from_id
        from_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        now_str = StatusController.datetime_to_str(datetime.now(),'%Y-%m-%d')
        await self.send_activity_message(from_id, to_id, now_str, img_caption="График активности [user] за сегодня")

    async def cmd_plot_all(self, from_id, params):
        to_id = from_id
        from_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, to_id, None, img_caption="График активности [user] за всё время")

    async def cmd_plot_week(self, from_id, params):
        to_id = from_id
        from_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        date_str1 = StatusController.datetime_to_str(datetime.now() + timedelta(days=-6), '%Y-%m-%d')
        date_str2 = StatusController.datetime_to_str(datetime.now() + timedelta(days=1), '%Y-%m-%d')
        await self.send_activity_message(from_id, to_id, (date_str1, date_str2), img_caption="График активности [user] за неделю")

    async def cmd_plot_hours(self, from_id, params):
        to_id = from_id
        from_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, to_id, None, True, img_caption="График активности [user] по часам")

    async def cmd_plot_hours_weekend(self, from_id, params):
        to_id = from_id
        from_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, to_id, "weekend", True, img_caption="График активности [user] по часам за выходные")

    async def cmd_plot_hours_weekday(self, from_id, params):
        to_id = from_id
        from_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        await self.send_activity_message(from_id, to_id, "weekday", True, img_caption="График активности [user] по часам за будние дни")

    async def cmd_user_info(self, to_id, params):
        from_id = to_id
        for_id = await self.tg_bot_controller.get_from_id_param(to_id, params)
        entity = await self.tg_bot_controller.tg_client.get_entity(for_id)
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
        if to_id != for_id:
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
        sessions_cnt = self.tg_bot_controller.tg_client.status_controller.get_user_activity_sessions_count(entity.id)
        if sessions_cnt > 0:
            res.append('Активность отслеживается (сохранено сессий: ' + str(sessions_cnt) + ')')
        else:
            res.append('Активность не отслеживается')
        m_types = self.tg_bot_controller.tg_client.status_controller.get_user_messages_entity_types(entity.id)
        if m_types and len(m_types) > 0:
            res.append('Сообщения отслеживаются (' + (", ".join(m_types)) + ')')
        else:
            res.append('Сообщения не отслеживается')

        stat_msg = await self.tg_bot_controller.tg_client.status_controller.get_user_aa_statistics_text(entity.id, False)
        if stat_msg:
            res.append('')
            res.append(stat_msg)

        stat_msg = await self.tg_bot_controller.tg_client.status_controller.get_stat_user_messages(entity.id, from_id)
        if stat_msg:
            res.append('')
            res.append(stat_msg)

        await self.send_message_to_user(to_id, "\n".join(res))
