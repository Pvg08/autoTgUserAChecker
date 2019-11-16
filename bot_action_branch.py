import asyncio
import re
import traceback
from datetime import datetime

from telethon.errors import MessageNotModifiedError
from telethon.tl import functions
from telethon.tl.types import SendMessageTypingAction, SendMessageCancelAction, KeyboardButtonCallback, Message, \
    MessageService, PeerUser, User


class BotActionBranch:

    def __init__(self, tg_bot_controller, branch_parent, branch_code=None):
        if branch_code is None:
            branch_code = ''
        self.tg_bot_controller = tg_bot_controller
        self.max_commands = 1
        self.branches = []
        self.read_once_callbacks = {}
        self.default_pick_action_text = 'Выберите дальнейшее действие'
        self.default_not_avail_command_text = 'Невозможно выполнить команду!\n\nДля получения списка доступных команд нажмите **[Продолжить]**'
        self.default_error_text = 'Какая-то ошибка!\n\n[error_name]'
        self.yes_variants = ['1', 'да', 'ок', 'yes', 'ok', 'y', 'хорошо', '+']
        self.no_variants = ['0', 'нет', 'не', 'no', 'not', 'n', '-']
        self.branch_parent = branch_parent
        self.branch_code = branch_code
        self.last_typing_sent_date = None
        self.use_timer = False
        self.commands = {
            '/help': {
                'cmd': self.cmd_help,
                'display_condition': self.do_not_display,
                'bot_button': {
                    'title': 'Справка',
                    'position': [1000, 1],
                },
                'places': ['bot', 'dialog'],
                'rights_level': 0,
                'desc': 'краткая справка, это ее ты сейчас видишь.'
            },
            '/main': {
                'cmd': self.cmd_to_top,
                'display_condition': self.is_in_second_branch,
                'bot_button': {
                    'title': 'Главное меню',
                    'position': [1000, 0],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'вернуться в основное меню'
            },
            '/back': {
                'cmd': self.cmd_back,
                'display_condition': self.is_in_third_branches,
                'bot_button': {
                    'title': 'Назад',
                    'position': [1000, 0],
                },
                'places': ['bot'],
                'rights_level': 0,
                'desc': 'подняться на уровень выше'
            },
            '/enter_str': {
                'cmd': self.cmd_enter_str,
                'display_condition': self.do_not_display,
                'places': ['bot'],
                'rights_level': 0,
                'desc': None
            },
        }
        self.command_groups = {}
        self.sub_commands_forbidden = []
        self.sub_commands_forbidden_command = {
            'places': [],
            'rights_level': 10,
            'desc': None
        }

    async def on_timer(self):
        pass

    def on_init_finish(self):
        if self != self.tg_bot_controller:
            if self.branch_parent:
                self.branch_parent.branches.append(self)
            self.tg_bot_controller.sub_commands_forbidden = self.tg_bot_controller.sub_commands_forbidden + list(self.commands.keys())

    def register_cmd_branches(self):
        for k in self.commands.keys():
            if ('class' in self.commands[k]) and self.commands[k]['class']:
                cmd_class = self.commands[k]['class']
                if issubclass(cmd_class, BotActionBranch):
                    obj = cmd_class(self.tg_bot_controller, self, str(k).strip('/'))
                    self.commands[k]['cmd'] = obj
                    self.commands[k]['condition'] = obj.can_use_branch
                    obj.register_cmd_branches()
        if self.use_timer:
            self.tg_bot_controller.tg_client.register_on_timer(self.on_timer)

    def can_use_branch(self, user_id):
        return True

    async def user_link(self, user_id, user_name=None):
        if not user_name:
            user_name = await self.tg_bot_controller.tg_client.get_entity_name(user_id, 'User')
        return "["+user_name+"](tg://user?id="+str(user_id)+")"

    async def get_commands_buttons_list(self, for_user_id):
        button_rows = {}

        def place_button(title, cmd, position):
            row = position[0]
            col = position[1]
            if row not in button_rows:
                button_rows[row] = {}
            while col in button_rows[row]:
                col = col + 1
            button_rows[row][col] = KeyboardButtonCallback(title, str(cmd).encode())

        curr_place = self.tg_bot_controller.get_user_place_code(for_user_id)
        curr_rights = await self.tg_bot_controller.get_user_rights_level_realtime(for_user_id)
        for k in self.commands.keys():
            if (
                self.commands[k]['desc'] and
                (curr_place in self.commands[k]['places']) and
                (curr_rights >= self.commands[k]['rights_level']) and
                (('condition' not in self.commands[k]) or self.commands[k]['condition'](for_user_id)) and
                (('display_condition' not in self.commands[k]) or self.commands[k]['display_condition'](for_user_id)) and
                (curr_place == 'bot') and ('bot_button' in self.commands[k])
            ):
                place_button(self.commands[k]['bot_button']['title'], k, self.commands[k]['bot_button']['position'])

        button_rows = list(map(
            lambda x: list(map(
                lambda xs: xs[1],
                sorted(x[1].items(), key=lambda kx: kx[0])
            )),
            sorted(button_rows.items(), key=lambda kv: kv[0])
        ))

        return button_rows

    async def get_commands_description_list(self, for_user_id, str_pick_text=None):
        if str_pick_text is None:
            str_pick_text = self.default_pick_action_text
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
                (('condition' not in self.commands[k]) or self.commands[k]['condition'](for_user_id)) and
                (('display_condition' not in self.commands[k]) or self.commands[k]['display_condition'](for_user_id))
            ):
                if k in self.command_groups:
                    commands_results.append('')
                    if self.command_groups[k]:
                        commands_results.append(self.command_groups[k])
                        commands_results.append('')
                commands_count = commands_count + 1
                if (curr_place != 'bot') or ('bot_button' not in self.commands[k]):
                    str_k = str(k)
                else:
                    str_k = '**[' + self.commands[k]['bot_button']['title'] + ']**'
                commands_results.append( str_k + ' - ' + str(self.commands[k]['desc']).replace('[me_user]', self.tg_bot_controller.tg_client.me_user_entity_name))
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

    def is_in_main_branch(self, user_id):
        return self.tg_bot_controller.get_user_branch(user_id) is None

    def is_in_second_branch(self, user_id):
        return (self.tg_bot_controller.get_user_branch(user_id) is not None) and (self.branch_parent == self.tg_bot_controller)

    def is_in_third_branches(self, user_id):
        return (self.tg_bot_controller.get_user_branch(user_id) is not None) and (self.branch_parent != self.tg_bot_controller)

    def is_not_active_message(self, user_id):
        msg_id_active = self.tg_bot_controller.get_user_message_id(user_id, True)
        msg_id_last = self.tg_bot_controller.get_user_message_id(user_id, False)
        return (not msg_id_active) or (msg_id_active != msg_id_last)

    def do_not_display(self, user_id):
        return False

    def get_branch_path(self):
        self_path = self.branch_code
        if self.branch_parent:
            pre_path = self.branch_parent.get_branch_path()
            self_path = (pre_path + '/' + self_path).strip('/')
        return '/' + self_path

    def activate_branch_for_user(self, user_id):
        curr_branch = self.tg_bot_controller.get_user_branch(user_id)
        if (not curr_branch) or (curr_branch == self.branch_parent):
            self.tg_bot_controller.set_branch_for_user(user_id, self)

    def deactivate_branch_for_user(self, user_id):
        self.tg_bot_controller.set_branch_for_user(user_id, None)

    async def read_bot_str(self, from_id, callback, message=None, params=None):
        if message:
            await self.send_message_to_user(from_id, message, do_set_next=False)
        self.read_once_callbacks[from_id] = {
            'callback': callback,
            'params': params
        }

    @staticmethod
    def cmd_to_btns(cmd_dict, cols=1):
        buttons = []
        curr_row = []
        for b_cmd, b_text in cmd_dict.items():
            curr_row.append(KeyboardButtonCallback(str(b_text), str(b_cmd).encode()))
            if len(curr_row) >= cols:
                buttons.append(curr_row)
                curr_row = []
        if len(curr_row) > 0:
            buttons.append(curr_row)
        return buttons

    async def read_username_str(self, from_id, callback, message=None, params=None, allow_pick_myself=True):
        if not message:
            message = 'Выберите вариант из списка:'
        username_variants = await self.tg_bot_controller.entity_controller.get_username_variants(from_id, allow_pick_myself)
        buttons = [[KeyboardButtonCallback(str(variant[1]), str(variant[0]).encode())] for variant in username_variants.items()]
        buttons.append([KeyboardButtonCallback('Ввести логин/ID', b"/enter_str")])
        await self.send_message_to_user(from_id, message, buttons=buttons, do_set_next=False)
        self.read_once_callbacks[from_id] = {
            'enter_str_text': 'Введите логин/ID пользователя Telegram:',
            'need_username_add': True,
            'username_add_text': 'Выбран пользователь: **{}**',
            'allow_pick_myself': allow_pick_myself,
            'callback': callback,
            'params': params
        }

    async def get_from_id_param(self, from_id, params):
        if params and (len(params) > 0) and params[0]:
            try:
                entity = await self.tg_bot_controller.tg_client.get_entity(str(params[0]).strip())
            except ValueError:
                try:
                    entity = await self.tg_bot_controller.tg_client.get_entity(PeerUser(int(params[0])))
                except:
                    entity = None
            if type(entity) == User:
                from_id = entity.id
        return from_id

    async def run_user_callback(self, message, from_id):
        if message == '/enter_str':
            await self.send_message_to_user(from_id, self.read_once_callbacks[from_id]['enter_str_text'], do_set_next=False)
            return
        if ('need_username_add' in self.read_once_callbacks[from_id]) and self.read_once_callbacks[from_id]['need_username_add']:
            message_user_id = await self.get_from_id_param(None, [message])
            if message_user_id:
                if ('allow_pick_myself' not in self.read_once_callbacks[from_id]) or not self.read_once_callbacks[from_id]['allow_pick_myself']:
                    if (int(message_user_id) == self.tg_bot_controller.tg_client.me_user_id) and (from_id == self.tg_bot_controller.tg_client.me_user_id):
                        await self.send_message_to_user(from_id, 'Недопустимый выбор! Попробуй что-нибудь другое!', do_set_next=False)
                        return
                    if (int(message_user_id) == self.tg_bot_controller.tg_client.me_user_id) and (from_id != self.tg_bot_controller.tg_client.me_user_id):
                        message_user_id = from_id
                if ('username_add_text' in self.read_once_callbacks[from_id]) and self.read_once_callbacks[from_id]['username_add_text']:
                    user_name = await self.tg_bot_controller.tg_client.get_entity_name(message_user_id, 'User')
                    await self.send_message_to_user(from_id, self.read_once_callbacks[from_id]['username_add_text'].format(user_name), do_set_next=False)
                await self.tg_bot_controller.entity_controller.add_username_variant(from_id, message_user_id)
        n_params = self.read_once_callbacks[from_id]['params']
        if n_params is None:
            n_params = [message]
        elif type(n_params) == list:
            n_params.append(message)
        else:
            n_params = [n_params, message]
        r_callback = self.read_once_callbacks[from_id]['callback']
        self.read_once_callbacks[from_id] = None
        await r_callback(from_id, n_params)

    async def read_or_run_default(self, from_id, callback_cmd, callback_param, read_message, cmd_params=None):
        # async def callback_cmd(from_id, params)
        # def callback_param(user_id)
        u_param = False
        if callback_param:
            u_param = callback_param(from_id)
        if u_param:
            n_params = cmd_params
            if n_params is None:
                n_params = [u_param]
            elif type(n_params) == list:
                n_params.append(u_param)
            else:
                n_params = [n_params, u_param]
            await callback_cmd(from_id, n_params)
        else:
            await self.read_bot_str(from_id, callback_cmd, read_message, cmd_params)

    async def run_main_setup(self, from_id, params):
        if self.tg_bot_controller.is_active_for_user(from_id):
            self.activate_branch_for_user(from_id)
            await self.show_current_branch_commands(from_id)
            pass

    async def show_current_branch_commands(self, from_id, pre_text=None):
        msg_text = "\n".join(await self.get_commands_description_list(from_id, pre_text))
        buttons = await self.get_commands_buttons_list(from_id)
        await self.send_message_to_user(from_id, msg_text, buttons=buttons, set_active=True)

    async def on_bot_message(self, message, from_id):
        if not self.is_in_current_branch(from_id):
            return False
        if (from_id in self.read_once_callbacks) and self.read_once_callbacks[from_id]:
            await self.run_user_callback(message, from_id)
            return True
        message = message.lower()
        if await self.run_command_text(message, from_id):
            return True
        return False

    async def run_command_text(self, command_text, from_id):
        command_parts = command_text.split(' ')
        command_code = None
        if (len(command_parts) > 0) and command_parts[0]:
            command_code = command_parts[0].lower()
        if command_code and (command_parts[0] in self.commands):
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
                    command_obj = command['cmd']
                    if isinstance(command_obj, BotActionBranch):
                        command_f = command_obj.run_main_setup
                    else:
                        command_f = command_obj
                    if ('cmd_params_reader' in command) and command['cmd_params_reader']:
                        await command['cmd_params_reader'](from_id, command_f)
                    else:
                        await command_f(from_id, command_parts[1:])
                except Exception as exception:
                    traceback.print_exc()
                    await self.send_message_to_user(from_id, self.default_error_text.replace('[error_name]', str(exception)).strip())
            else:
                await self.send_message_to_user(from_id, self.default_not_avail_command_text)
            return True
        elif command_code in self.sub_commands_forbidden:
            await self.send_message_to_user(from_id, self.default_not_avail_command_text)
            return True
        return False

    async def remove_message_buttons(self, user_id, message_id):
        if not self.tg_bot_controller.is_active_for_user(user_id):
            return
        str_user_id = str(user_id)
        message_client = self.tg_bot_controller.users[str_user_id]['message_client']
        dialog_entity = self.tg_bot_controller.users[str_user_id]['dialog_entity']
        message = await message_client.get_messages(dialog_entity, ids=message_id)
        if not message:
            return
        if (type(message) != Message) and (type(message) != MessageService):
            if len(message) == 0:
                return
            message = message[0]
        if type(message) != Message:
            return
        try:
            msg_text = message.text
            if message.entities and len(message.entities) > 0 and str(msg_text).find('Фото профиля:') >= 0:
                link_preview = True
            else:
                link_preview = False
            await message_client.edit_message(dialog_entity, message_id, text=msg_text, link_preview=link_preview, buttons=None)
        except MessageNotModifiedError:
            return

    async def send_message_to_user(self, user_id, message, link_preview=False, buttons=None, set_active=False, do_set_next=True):
        if not self.tg_bot_controller.is_active_for_user(user_id):
            return
        if (len(message) + 8) >= 4096:
            status_results = message.strip().splitlines()
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
                    if len(status_results) > 0:
                        await self.send_one_message_to_user(user_id, "\n".join(buff), link_preview, None, False, False)
                        await asyncio.sleep(0.75)
                    else:
                        await self.send_one_message_to_user(user_id, "\n".join(buff), link_preview, buttons, set_active, do_set_next)
        else:
            await self.send_one_message_to_user(user_id, message, link_preview, buttons, set_active, do_set_next)

    async def send_one_message_to_user(self, user_id, message, link_preview=False, buttons=None, set_active=False, do_set_next=True):
        if not self.tg_bot_controller.is_active_for_user(user_id):
            return
        try:
            str_user_id = str(user_id)
            message_client = self.tg_bot_controller.users[str_user_id]['message_client']
            dialog_entity = self.tg_bot_controller.users[str_user_id]['dialog_entity']
            message = self.tg_bot_controller.text_to_bot_text(message, user_id)
            last_active = None
            last_next = self.tg_bot_controller.get_user_next_message_id(user_id)
            set_next = False
            if set_active:
                last_active = self.tg_bot_controller.get_user_message_id(user_id, True)
                last_msg_id = self.tg_bot_controller.get_user_message_id(user_id, False)
                if last_active and (last_active != last_msg_id):
                    await message_client.delete_messages(dialog_entity, [last_active])
                    last_active = None
            if last_next:
                await self.remove_message_buttons(user_id, last_next)
                self.tg_bot_controller.set_message_for_user(user_id, None, False, True)
            if not last_active:
                if (not buttons) and (not set_active) and do_set_next:
                    buttons = [KeyboardButtonCallback('Продолжить', '/help'.encode())]
                    set_next = True
                elif buttons and (not set_active):
                    set_next = True
                message = await message_client.send_message(dialog_entity, message, link_preview=link_preview, buttons=buttons)
                if message and (type(message) == Message):
                    self.tg_bot_controller.set_message_for_user(user_id, message.id, set_active, set_next)
            else:
                try:
                    message = await message_client.edit_message(dialog_entity, last_active, message, link_preview=link_preview, buttons=buttons)
                except MessageNotModifiedError:
                    return True
                    pass
        except:
            traceback.print_exc()
        return True if message else False

    async def send_file_to_user(self, user_id, file_name, caption, force_document=False):
        if not self.tg_bot_controller.is_active_for_user(user_id):
            return
        str_user_id = str(user_id)
        message_client = self.tg_bot_controller.users[str_user_id]['message_client']
        dialog_entity = self.tg_bot_controller.users[str_user_id]['dialog_entity']
        caption = self.tg_bot_controller.text_to_bot_text(caption, user_id)
        message = await message_client.send_file(dialog_entity, file_name, caption=caption, force_document=force_document)
        if message and (type(message) == Message):
            self.tg_bot_controller.set_message_for_user(user_id, message.id, False, False)
        return True if message else False

    async def send_typing_to_user(self, user_id, typing_begin=True):
        str_user_id = str(user_id)
        message_client = self.tg_bot_controller.users[str_user_id]['message_client']
        dialog_entity = self.tg_bot_controller.users[str_user_id]['dialog_entity']
        self.last_typing_sent_date = datetime.now()
        return await message_client(functions.messages.SetTypingRequest(
            peer=dialog_entity,
            action=SendMessageTypingAction() if typing_begin else SendMessageCancelAction()
        ))

    async def resend_typing_to_user(self, user_id):
        if self.last_typing_sent_date and ((datetime.now() - self.last_typing_sent_date).total_seconds() > 5):
            await self.send_typing_to_user(user_id)

    async def return_to_main_branch(self, from_id):
        self.deactivate_branch_for_user(from_id)
        await self.tg_bot_controller.cmd_help(from_id, [])

    async def return_to_back_branch(self, from_id):
        if self == self.tg_bot_controller:
            self.deactivate_branch_for_user(from_id)
        else:
            if self.branch_parent != self.tg_bot_controller:
                self.tg_bot_controller.set_branch_for_user(from_id, self.branch_parent)
                await self.branch_parent.cmd_help(from_id, [])
            else:
                self.deactivate_branch_for_user(from_id)

    async def cmd_help(self, from_id, params):
        text = []
        if params == 'Start':
            text.append('Привет, я - чат-бот (и не только)')
        text.append('Список доступных для тебя моих команд')
        await self.show_current_branch_commands(from_id, "\n".join(text))

    async def cmd_to_top(self, from_id, params):
        await self.return_to_main_branch(from_id)

    async def cmd_back(self, from_id, params):
        await self.return_to_back_branch(from_id)

    async def cmd_enter_str(self, from_id, params):
        await self.return_to_back_branch(from_id)
