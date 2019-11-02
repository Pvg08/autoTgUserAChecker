from datetime import timedelta, datetime

from telethon.utils import is_list_like

from bot_action_branch import BotActionBranch
from status_controller import StatusController


class ActivityBranch(BotActionBranch):

    def __init__(self, tg_bot_controller):
        super().__init__(tg_bot_controller)

        self.me_picker_cmd = None

        self.max_commands = 8
        self.commands.update({
            '/activity_today': {
                'cmd': self.cmd_activity_today,
                'places': ['bot', 'dialog'],
                'rights_level': 2,
                'desc': 'сессии за сегодня.'
            },
            '/plot_today': {
                'cmd': self.cmd_plot_today,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности за сегодня.'
            },
            '/plot_week': {
                'cmd': self.cmd_plot_week,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности за неделю.'
            },
            '/plot_all': {
                'cmd': self.cmd_plot_all,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'график активности за всё время.'
            },
            '/plot_hours': {
                'cmd': self.cmd_plot_hours,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности по часам за всё время.'
            },
            '/plot_hours_weekday': {
                'cmd': self.cmd_plot_hours_weekday,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности по часам за будние дни.'
            },
            '/plot_hours_weekend': {
                'cmd': self.cmd_plot_hours_weekend,
                'places': ['bot', 'dialog'],
                'rights_level': 3,
                'desc': 'статистика активности по часам за выходные.'
            }
        })
        self.command_groups.update({
            '/activity_today': 'Нижеперечисленные команды предназначены для получения статистики активности отслеживаемых пользователей.\n'
                          'После этих команд можно через пробел указать параметр - логин/ID пользователя, к которому будет применена команда. '
                          'Без параметра она применяется к тебе.',
            '/plot_today': None
        })
        self.on_init_finish()

    async def pick_user_if_need(self, from_id, params, cmd):
        if (from_id == self.tg_bot_controller.tg_client.me_user_id) and ((params is None) or is_list_like(params)):
            self.me_picker_cmd = cmd
            await self.read_bot_str(from_id, self.on_pick_username, 'Введите логин/ID пользователя Telegram:')
            return True
        return False

    async def on_pick_username(self, message, from_id):
        if not self.me_picker_cmd:
            return
        for_id = await self.tg_bot_controller.get_from_id_param(from_id, [message])
        if for_id == self.tg_bot_controller.tg_client.me_user_id:
            self.me_picker_cmd = None
            await self.send_message_to_user(from_id, 'Недопустимый выбор!')
            await self.show_current_branch_commands(from_id)
            return
        await self.me_picker_cmd(from_id, for_id)
        self.me_picker_cmd = None

    @staticmethod
    def get_send_for_id(from_id, params):
        if (params is None) or is_list_like(params):
            return from_id
        return int(params)

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
