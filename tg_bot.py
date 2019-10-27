from datetime import datetime

from telethon import TelegramClient, events
from telethon.tl.types import UpdateNewMessage

from bot_action_branch import BotActionBranch
from status_controller import StatusController


class InteractiveTelegramBot(TelegramClient):

    def __init__(self, session_file, api_id, api_hash, cl_conn, proxy, parent_client):
        print('Initialization of bot')
        self.tg_client = parent_client
        self.bot_entity = None
        self.branches = []
        super().__init__(session_file, api_id, api_hash, connection=cl_conn, proxy=proxy, sequential_updates=True)

    def add_branch(self, branch:BotActionBranch):
        self.branches.append(branch)

    async def message_handler(self, event):
        if type(event.original_update) != UpdateNewMessage:
            return
        if not self.bot_entity:
            self.bot_entity = await self.get_entity(self.tg_client.config['tg_bot']['bot_username'])
        data = event.original_update
        if data.message.from_id == self.bot_entity.id:
            return
        if data.message.from_id and (data.message.from_id != self.tg_client.me_user_id):
            msg_entity_name = await self.tg_client.get_entity_name(data.message.from_id, 'Bot')
            if msg_entity_name:
                print(StatusController.datetime_to_str(datetime.now()) + ' Message to my bot from "' + msg_entity_name + '"')
                print('<<< ' + str(data.message.message))
                t_date = StatusController.tg_datetime_to_local_datetime(data.message.date)
                self.tg_client.add_message_to_db(self.bot_entity.id, 'Bot', data.message.from_id, self.bot_entity.id, data.message.id, data.message.message, t_date, 0)
        bot_chat = await event.get_input_chat()

        for branch in self.branches:
            if branch.is_setup_mode:
                if await branch.on_bot_message(data.message.message, data.message.from_id, bot_chat):
                    return

        await self.tg_client.bot_controller.bot_command(data.message.message, data.message.from_id, self.bot_entity.id, 'Bot', bot_chat)

    def do_start(self):
        print('Starting of bot')
        self.start(bot_token=self.tg_client.config['tg_bot']['token'])
        self.add_event_handler(self.message_handler, event=events.NewMessage)
