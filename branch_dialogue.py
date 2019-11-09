from datetime import timedelta, datetime

from telethon.utils import is_list_like

from bot_action_branch import BotActionBranch
from dialog_stats import DialogStats
from status_controller import StatusController


class DialogueBranch(BotActionBranch):

    def __init__(self, tg_bot_controller, branch_parent, branch_code=None):
        super().__init__(tg_bot_controller, branch_parent, branch_code)

        self.me_picker_cmd = None
        self.dialog_stats = DialogStats(tg_bot_controller.tg_client)

        self.max_commands = 7
        self.commands.update({
            '/dialogue_all': {
                'cmd': self.cmd_dialogue_all,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': 'За всё время',
                    'position': [0, 0],
                },
                'rights_level': 2,
                'desc': 'статистика за всё время'
            },
            '/dialogue_1year': {
                'cmd': self.cmd_dialogue_1year,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': '1 год',
                    'position': [0, 1],
                },
                'rights_level': 2,
                'desc': 'статистика за 1 год'
            },
            '/dialogue_6month': {
                'cmd': self.cmd_dialogue_6month,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': '6 месяцев',
                    'position': [0, 2],
                },
                'rights_level': 2,
                'desc': 'статистика за 6 месяцев'
            },
            '/dialogue_month': {
                'cmd': self.cmd_dialogue_month,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': '1 месяц',
                    'position': [1, 0],
                },
                'rights_level': 2,
                'desc': 'статистика за месяц'
            },
            '/dialogue_week': {
                'cmd': self.cmd_dialogue_week,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': 'неделя',
                    'position': [1, 1],
                },
                'rights_level': 2,
                'desc': 'статистика за неделю'
            },
            '/dialogue_last': {
                'cmd': self.cmd_dialogue_last,
                'places': ['bot', 'dialog'],
                'bot_button': {
                    'title': 'прошлый диалог',
                    'position': [1, 2],
                },
                'rights_level': 2,
                'desc': 'статистика за прошлый диалог'
            },
        })
        self.on_init_finish()

    async def pick_user_if_need(self, from_id, params, cmd):
        if (from_id == self.tg_bot_controller.tg_client.me_user_id) and ((params is None) or is_list_like(params)):
            self.me_picker_cmd = cmd
            await self.read_username_str(from_id, self.on_pick_username, allow_pick_myself=False)
            return True
        return False

    async def on_pick_username(self, from_id, params):
        if not self.me_picker_cmd:
            return
        for_id = await self.tg_bot_controller.get_from_id_param(from_id, params)
        if for_id == self.tg_bot_controller.tg_client.me_user_id:
            self.me_picker_cmd = None
            await self.send_message_to_user(from_id, 'Недопустимый выбор!')
            return
        await self.me_picker_cmd(from_id, for_id)
        self.me_picker_cmd = None

    async def show_message_edits(self, from_id, entity_id, message_id):
        message_edits = self.dialog_stats.get_message_edits(entity_id, message_id)
        results = [
            'Кажется, ты переслал сообщение из отслеживаемого диалога',
            'Выведем доп. информацию по нему...',
            'Число правок: {}'.format(len(message_edits) - 1)
        ]
        if len(message_edits) > 0:
            last_version = None
            for message_edit in message_edits:
                results.append('')
                results.append('**Версия {} / {}**'.format(message_edit['version'], message_edit['max_version']))
                date = StatusController.datetime_from_str(message_edit['taken_at'], '%Y-%m-%d %H:%M:%S%z')
                results.append('Дата: {}'.format(StatusController.datetime_to_str(date)))
                results.append('Сообщение: \n[{}]\n'.format(self.dialog_stats.remove_message_tags(message_edit['message'])))
                if last_version:
                    edit_ratio = self.dialog_stats.get_str_difference_ratio(self.dialog_stats.remove_message_tags(last_version['message']), self.dialog_stats.remove_message_tags(message_edit['message']))
                    results.append('Процент правок: {0:0.2f}%'.format(100 * edit_ratio))
                    diff_counts = self.dialog_stats.get_str_difference_counts(last_version['message'], message_edit['message'])
                    results.append('Число замен   : {}'.format(diff_counts['replaces_count_edit']))
                    results.append('Число вставок : {}'.format(diff_counts['inserts_count_edit']))
                    results.append('Число удалений: {}'.format(diff_counts['deletes_count_edit']))

                last_version = message_edit
        results = "\n".join(results)
        await self.send_message_to_user(from_id, results)

    @staticmethod
    def get_send_for_id(from_id, params):
        if (params is None) or is_list_like(params):
            return from_id
        return int(params)

    async def cmd_dialogue_all(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_all):
            return
        for_id = self.get_send_for_id(from_id, params)
        await self.send_typing_to_user(from_id, True)
        res = await self.dialog_stats.get_me_dialog_statistics(for_id)
        await self.send_message_to_user(from_id, "\n".join(res['results']))

    async def cmd_dialogue_week(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_week):
            return
        for_id = self.get_send_for_id(from_id, params)
        await self.send_typing_to_user(from_id, True)
        res = await self.dialog_stats.get_me_dialog_statistics(for_id, datetime.now() - timedelta(days=7), 'за неделю')
        await self.send_message_to_user(from_id, "\n".join(res['results']))

    async def cmd_dialogue_month(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_month):
            return
        for_id = self.get_send_for_id(from_id, params)
        await self.send_typing_to_user(from_id, True)
        res = await self.dialog_stats.get_me_dialog_statistics(for_id, datetime.now() - timedelta(days=31), 'за месяц')
        await self.send_message_to_user(from_id, "\n".join(res['results']))

    async def cmd_dialogue_6month(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_6month):
            return
        for_id = self.get_send_for_id(from_id, params)
        await self.send_typing_to_user(from_id, True)
        res = await self.dialog_stats.get_me_dialog_statistics(for_id, datetime.now() - timedelta(days=183), 'за 6 месяцев')
        await self.send_message_to_user(from_id, "\n".join(res['results']))

    async def cmd_dialogue_1year(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_1year):
            return
        for_id = self.get_send_for_id(from_id, params)
        await self.send_typing_to_user(from_id, True)
        res = await self.dialog_stats.get_me_dialog_statistics(for_id, datetime.now() - timedelta(days=365), 'за 1 год')
        await self.send_message_to_user(from_id, "\n".join(res['results']))

    async def cmd_dialogue_last(self, from_id, params):
        if await self.pick_user_if_need(from_id, params, self.cmd_dialogue_last):
            return
        for_id = self.get_send_for_id(from_id, params)
        await self.send_typing_to_user(from_id, True)
        res = await self.dialog_stats.get_me_dialog_statistics(for_id, skip_vocab=True)
        last_date = res['last_dialogue_date']
        if not last_date:
            await self.send_message_to_user(from_id, 'Диалогов не найдено')
            return
        res = await self.dialog_stats.get_me_dialog_statistics(for_id, last_date - timedelta(hours=1), 'за последний диалог', True)
        await self.send_message_to_user(from_id, "\n".join(res['results']))
