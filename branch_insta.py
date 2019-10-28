import traceback
import re
from datetime import datetime, timedelta

from bot_action_branch import BotActionBranch


class InstaBranch(BotActionBranch):

    def __init__(self, tg_bot_controller):
        super().__init__(tg_bot_controller)

        self.max_commands = 2
        self.commands = {
            '/insta_check_subscribers': {
                'cmd': self.cmd_check_subscribers,
                'condition': self.is_setup_condition,
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'сверить списки подписчиков'
            },
            '/back': {
                'cmd': self.cmd_back,
                'condition': self.is_setup_condition,
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'вернуться'
            },
        }
        self.on_init_finish()

    async def cmd_check_subscribers(self, from_id, params):
        print('cmd_check_subscribers')
