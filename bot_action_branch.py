import re
import traceback


class BotActionBranch:

    def __init__(self, tg_bot_controller):
        self.tg_bot_controller = tg_bot_controller
        self.max_commands = 1
        self.branches = []
        self.read_once_callbacks = {}
        self.commands = {}
        self.command_groups = {}
        self.sub_commands_forbidden = []
        self.sub_commands_forbidden_command = {
            'places': [],
            'rights_level': 10,
            'desc': None
        }

    def on_init_finish(self):
        if self != self.tg_bot_controller:
            self.tg_bot_controller.add_branch(self)
            self.tg_bot_controller.sub_commands_forbidden = self.tg_bot_controller.sub_commands_forbidden + list(self.commands.keys())

    def register_branch(self, command, cmd_class):
        obj = cmd_class(self.tg_bot_controller)
        self.commands[command]['cmd'] = obj
        self.commands[command]['condition'] = obj.can_use_branch

    def register_cmd_branches(self):
        for k in self.commands.keys():
            if ('class' in self.commands[k]) and self.commands[k]['class']:
                self.register_branch(k, self.commands[k]['class'])

    def add_branch(self, branch):
        self.branches.append(branch)

    def can_use_branch(self, user_id):
        return True

    def get_config_value(self, section, param):
        if (section in self.tg_bot_controller.tg_client.config) and (param in self.tg_bot_controller.tg_client.config[section]):
            return self.tg_bot_controller.tg_client.config[section][param]
        return ''

    async def get_commands_description_list(self, for_user_id, str_pick_text='выберите дальнейшее действие'):
        result_str = []
        curr_place = self.tg_bot_controller.get_user_place_code(for_user_id)
        curr_rights = await self.tg_bot_controller.get_user_rights_level_realtime(for_user_id)
        commands_results = []
        commands_count = 0
        for k in self.commands.keys():
            if (
                    self.commands[k]['desc'] and
                    (curr_place in self.commands[k]['places']) and
                    (curr_rights >= self.commands[k]['rights_level']) and
                    (('condition' not in self.commands[k]) or self.commands[k]['condition'](for_user_id))
            ):
                if k in self.command_groups:
                    commands_results.append('')
                    if self.command_groups[k]:
                        commands_results.append(self.command_groups[k])
                        commands_results.append('')
                commands_count = commands_count + 1
                commands_results.append('' + k + ' - ' + str(self.commands[k]['desc']).replace('[me_user]', self.tg_bot_controller.tg_client.me_user_name))
        result_str.append('\n'+str_pick_text+' (' + str(commands_count) + '/' + str(self.max_commands) + '):\n')
        result_str = result_str + commands_results
        return result_str

    @staticmethod
    def adapt_command(text):
        text = re.sub(r'[^\w\s!\\?/]', ' ', text)
        text = re.sub(' +', ' ', text)
        return text.strip().lower()

    def is_in_current_branch(self, user_id):
        return self.tg_bot_controller.get_user_branch(user_id) == self

    def activate_branch_for_user(self, user_id):
        if not self.tg_bot_controller.get_user_branch(user_id):
            self.tg_bot_controller.set_branch_for_user(user_id, self)

    def deactivate_branch_for_user(self, user_id):
        if self.is_in_current_branch(user_id):
            self.tg_bot_controller.set_branch_for_user(user_id, None)

    async def read_bot_str(self, from_id, callback, message=None):
        if message:
            await self.send_message_to_user(from_id, message)
        self.read_once_callbacks[str(from_id)] = callback

    async def run_main_setup(self, from_id, params, message_client, dialog_entity):
        if self.tg_bot_controller.is_active_for_user(from_id):
            self.activate_branch_for_user(from_id)
            await self.show_current_branch_commands(from_id)
            pass

    async def show_current_branch_commands(self, from_id):
        msg_text = "\n".join(await self.get_commands_description_list(from_id))
        await self.send_message_to_user(from_id, msg_text)

    async def on_bot_message(self, message, from_id):
        if not self.is_in_current_branch(from_id):
            return False
        str_from_id = str(from_id)
        if (str_from_id in self.read_once_callbacks) and self.read_once_callbacks[str_from_id]:
            await self.read_once_callbacks[str_from_id](message, from_id)
            self.read_once_callbacks[str_from_id] = None
            return True
        message = message.lower()
        if await self.run_command_text(message, from_id):
            return True
        return False

    async def run_command_text(self, command_text, from_id):
        command_parts = command_text.split(' ')
        if (len(command_parts) > 0) and command_parts[0] and ((command_parts[0] in self.commands) or (command_parts[0] in self.sub_commands_forbidden)):
            command_code = command_parts[0]
            if command_code in self.sub_commands_forbidden:
                command = self.sub_commands_forbidden_command
            else:
                command = self.commands[command_code]
            curr_place = self.tg_bot_controller.get_user_place_code(from_id)
            curr_rights = await self.tg_bot_controller.get_user_rights_level_realtime(from_id)
            if (
                    (curr_place in command['places']) and
                    (curr_rights >= command['rights_level']) and
                    (('condition' not in command) or command['condition'](from_id)) and
                    ('cmd' in command) and
                    command['cmd']
            ):
                try:
                    if isinstance(command['cmd'], BotActionBranch):
                        await command['cmd'].run_main_setup(from_id, command_parts[1:], self.tg_bot_controller.users[str(from_id)]['message_client'], self.tg_bot_controller.users[str(from_id)]['dialog_entity'])
                    else:
                        await command['cmd'](from_id, command_parts[1:])
                except:
                    traceback.print_exc()
                    await self.send_message_to_user(from_id, 'Какая-то ошибка!')
            else:
                await self.send_message_to_user(from_id, 'Невозможно выполнить команду!')
            return True
        return False

    async def send_message_to_user(self, user_id, message, link_preview=False):
        if not self.tg_bot_controller.is_active_for_user(user_id):
            return
        str_user_id = str(user_id)
        message_client = self.tg_bot_controller.users[str_user_id]['message_client']
        dialog_entity = self.tg_bot_controller.users[str_user_id]['dialog_entity']
        message = self.tg_bot_controller.text_to_bot_text(message, user_id)
        await message_client.send_message(dialog_entity, message, link_preview=link_preview)

    async def send_file_to_user(self, user_id, file_name, caption, force_document=False):
        if not self.tg_bot_controller.is_active_for_user(user_id):
            return
        str_user_id = str(user_id)
        message_client = self.tg_bot_controller.users[str_user_id]['message_client']
        dialog_entity = self.tg_bot_controller.users[str_user_id]['dialog_entity']
        caption = self.tg_bot_controller.text_to_bot_text(caption, user_id)
        await message_client.send_file(dialog_entity, file_name, caption=caption, force_document=force_document)

    async def return_to_main_branch(self, from_id):
        self.deactivate_branch_for_user(from_id)
        await self.tg_bot_controller.cmd_help(from_id, [])

    async def cmd_back(self, from_id, params):
        await self.return_to_main_branch(from_id)
