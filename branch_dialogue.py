from datetime import timedelta, datetime

from telethon.utils import is_list_like

from bot_action_branch import BotActionBranch


class DialogueBranch(BotActionBranch):

    def __init__(self, tg_bot_controller):
        super().__init__(tg_bot_controller)

        self.me_picker_cmd = None

        self.max_commands = 5
        self.commands = {
            '/dialogue_all': {
                'cmd': self.cmd_dialogue_all,
                'places': ['bot'],
                'rights_level': 2,
                'desc': 'статистика за всё время'
            },
            '/dialogue_month': {
                'cmd': self.cmd_dialogue_month,
                'places': ['bot'],
                'rights_level': 2,
                'desc': 'статистика за месяц'
            },
            '/dialogue_week': {
                'cmd': self.cmd_dialogue_week,
                'places': ['bot'],
                'rights_level': 2,
                'desc': 'статистика за неделю'
            },
            '/dialogue_last': {
                'cmd': self.cmd_dialogue_last,
                'places': ['bot'],
                'rights_level': 2,
                'desc': 'статистика за прошлый диалог'
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

    async def pick_user_if_need(self, from_id, params, cmd):
        if (from_id == self.tg_bot_controller.tg_client.me_user_id) and ((params is None) or is_list_like(params)):
            self.me_picker_cmd = cmd
            await self.read_bot_str(from_id, self.on_pick_username, 'Введите логин/ID пользователя Telegram:')
            return True
        return False

    async def on_pick_username(self, message, from_id, dialog_entity):
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

    async def cmd_dialogue_all(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_all):
            return
        for_id = self.get_send_for_id(from_id, params)
        res = await self.tg_bot_controller.tg_client.status_controller.get_me_dialog_statistics(for_id)
        await self.send_message_to_user(from_id, "\n".join(res['results']))
        await self.show_current_branch_commands(from_id)

    async def cmd_dialogue_week(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_week):
            return
        for_id = self.get_send_for_id(from_id, params)
        res = await self.tg_bot_controller.tg_client.status_controller.get_me_dialog_statistics(for_id, datetime.now() - timedelta(days=7), 'за неделю')
        await self.send_message_to_user(from_id, "\n".join(res['results']))
        await self.show_current_branch_commands(from_id)

    async def cmd_dialogue_month(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_month):
            return
        for_id = self.get_send_for_id(from_id, params)
        res = await self.tg_bot_controller.tg_client.status_controller.get_me_dialog_statistics(for_id, datetime.now() - timedelta(days=31), 'за месяц')
        await self.send_message_to_user(from_id, "\n".join(res['results']))
        await self.show_current_branch_commands(from_id)

    async def cmd_dialogue_last(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_last):
            return
        for_id = self.get_send_for_id(from_id, params)
        res = await self.tg_bot_controller.tg_client.status_controller.get_me_dialog_statistics(for_id)
        last_date = res['last_dialogue_date']
        if not last_date:
            await self.send_message_to_user(from_id, 'Диалогов не найдено')
            return
        res = await self.tg_bot_controller.tg_client.status_controller.get_me_dialog_statistics(for_id, last_date - timedelta(hours=1), 'за последний диалог', True)
        await self.send_message_to_user(from_id, "\n".join(res['results']))
        await self.show_current_branch_commands(from_id)