
from telethon import TelegramClient, events
from telethon.tl.types import UpdateNewMessage


class InteractiveTelegramBot(TelegramClient):

    def __init__(self, session_file, api_id, api_hash, cl_conn, proxy, parent_client):
        print('Initialization of bot')
        self.tg_client = parent_client
        self.me_entity = None
        super().__init__(session_file, api_id, api_hash, connection=cl_conn, proxy=proxy, sequential_updates=True)

    async def message_handler(self, event):
        if type(event.original_update) != UpdateNewMessage:
            return
        if not self.me_entity:
            self.me_entity = await self.get_entity(self.tg_client.config['tg_bot']['bot_username'])
        data = event.original_update
        if data.message.from_id == self.me_entity.id:
            return
        bot_chat = await event.get_input_chat()
        await self.tg_client.bot_controller.bot_command(data.message.message, data.message.from_id, self.me_entity.id, 'Bot', bot_chat)

    def do_start(self):
        print('Starting of bot')
        self.start(bot_token=self.tg_client.config['tg_bot']['token'])
        self.add_event_handler(self.message_handler, event=events.NewMessage)
