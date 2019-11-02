from datetime import timedelta, datetime

from telethon.utils import is_list_like

from bot_action_branch import BotActionBranch


class ToolsBranch(BotActionBranch):

    def __init__(self, tg_bot_controller):
        super().__init__(tg_bot_controller)

        self.max_commands = 3
        self.commands.update({
            '/devices': {
                'cmd': self.cmd_devices,
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'состояние подключенных устройств и управление ими.'
            },
            '/new_version_send': {
                'cmd': self.cmd_new_version_send,
                'places': ['bot'],
                'rights_level': 4,
                'desc': 'отправить всем информацию о новой версии (кому ещё не отправляли).'
            },
        })
        self.on_init_finish()

    async def cmd_devices(self, from_id, params):
        await self.send_message_to_user(from_id, 'Не хватает прав на выполнение команды!')

    async def cmd_new_version_send(self, from_id, params):
        sent_count = await self.tg_bot_controller.tg_client.tg_bot.send_version_message_to_all_bot_users()
        if sent_count == 0:
            await self.send_message_to_user(from_id, 'Некому отправлять!')
        else:
            await self.send_message_to_user(from_id, 'Отправлено сообщений: {}'.format(sent_count))
