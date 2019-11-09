import json
import traceback
import re
import os
from datetime import datetime

import requests

from bot_action_branch import BotActionBranch
from helper_functions import MainHelper
from status_controller import StatusController


class InstaStoriesBranch(BotActionBranch):

    def __init__(self, tg_bot_controller, branch_parent, branch_code=None):
        super().__init__(tg_bot_controller, branch_parent, branch_code)

        self.use_timer = True
        self.last_stories_check_time = None

        self.max_commands = 4
        self.commands.update({
            '/insta_set_stories_users': {
                'cmd': self.cmd_set_stories_users,
                'condition': self.branch_parent.can_use_branch,
                'bot_button': {
                    'title': 'Задать источники',
                    'position': [0, 0],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'указать список пользователей - источников stories. У этих пользователей будет проводиться регулярное выкачивание stories'
            },
            '/insta_reset_stories_users': {
                'cmd': self.cmd_reset_stories_users,
                'condition': self.branch_parent.can_use_branch,
                'bot_button': {
                    'title': 'Очистить для всех',
                    'position': [0, 1],
                },
                'places': ['bot'],
                'rights_level': 4,
                'desc': 'очистить список пользователей историй для всех'
            },
            '/insta_stories_log': {
                'cmd': self.cmd_show_stories_log,
                'condition': self.branch_parent.can_use_branch,
                'bot_button': {
                    'title': 'Последние истории',
                    'position': [1, 0],
                },
                'places': ['bot'],
                'rights_level': 3,
                'desc': 'показать список последних скачанных историй'
            },
        })
        self.on_init_finish()

    async def show_current_branch_commands(self, from_id, post_text=None):
        if post_text is None:
            post_text = self.default_pick_action_text
        pre_text = ''

        curr_rights = await self.tg_bot_controller.get_user_rights_level_realtime(from_id)
        if curr_rights >= 3:
            ustories = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option(from_id, 'stories_accounts')
            if ustories:
                ustories = str(ustories).splitlines()
                pre_text = pre_text + "\nВыбранные вами пользователи для выгрузки историй: **" + (", ".join(ustories)) + "**\n"

            ustories2 = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option_list('stories_accounts', from_id)
            if ustories2 and (len(ustories2) > 0):
                ustories2 = str("\n".join(ustories2)).splitlines()
                ustories2 = list(dict.fromkeys(ustories2))
                if len(ustories2) > 0:
                    if curr_rights >= 4:
                        pre_text = pre_text + "\nВыбранные другими пользователи для выгрузки историй: **" + (", ".join(ustories2)) + "**\n\n"
                    else:
                        pre_text = pre_text + "\nЧисло пользователей, выбранное другими для выгрузки историй: **{}**\n\n".format(len(ustories2))

        await super().show_current_branch_commands(from_id, pre_text + "\n" + post_text)

    def save_user_stories(self, user_id, username):

        try:
            response = self.branch_parent.api.user_reel_media(user_id)
        except:
            print(username + ': Stories read error!')
            traceback.print_exc()
            return

        story_records = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option(0, 'insta_story_entries')
        if story_records:
            story_records = json.loads(story_records)
        else:
            story_records = {}

        for st_id in list(story_records.keys()):
            try:
                n_time = StatusController.now_local_datetime()
                d_time = StatusController.timestamp_to_local_datetime(int(story_records[st_id]['taken_at']))
                if (not d_time) or (n_time - d_time).total_seconds() > (2 * 24 * 60 * 60):
                    del story_records[st_id]
            except:
                del story_records[st_id]

        dups = 0
        for item in response['items']:
            id = item['id']
            try:
                url = item['video_versions'][0]['url']
            except KeyError:
                url = item['image_versions2']['candidates'][0]['url']
            taken_at = item['taken_at']
            location = ''
            if 'story_locations' in item:
                location_items = item['story_locations']
                if location_items and len(location_items) > 0:
                    location_item = location_items[0]
                    if location_item and ('location' in location_item) and location_item['location']:
                        location_item = location_item['location']
                        location = location_item['name']
                        if ('address' in location_item) and location_item['address']:
                            location = location + ', ' + str(location_item['address'])
                        if ('city' in location_item) and location_item['city']:
                            location = location + ', ' + str(location_item['city'])
                        if ('lat' in location_item) and ('lng' in location_item):
                            location = location + ', ' + str(location_item['lat']) + ',' + str(location_item['lng'])
            story_fname = self.get_story_file_name(taken_at, location)
            viewers_info = {}
            viewers_ids = []
            if 'viewers' in item:
                viewers = item['viewers']
                for viewer in viewers:
                    viewers_info[str(viewer['pk'])] = {
                        'date': datetime.now(),
                        'username': viewer['username'],
                        'full_name': viewer['full_name'],
                        'pk': viewer['pk']
                    }
                    viewers_ids.append(viewer['pk'])
                viewers_ids.sort()
                viewers = json.dumps(viewers_ids)
            else:
                viewers = '[]'

            if id not in story_records:
                entry = {
                    "id": id,
                    "url": url,
                    "userid": user_id,
                    "username": username,
                    "taken_at": taken_at,
                    "filename": "",
                    "viewers": viewers,
                    "location": location
                }
                story_records[id] = entry
                old_viewers = '[]'
                print('New story for user ' + str(username) + ' was found: ' + story_fname)
                MainHelper().play_notify_sound('notify_when_new_insta_story')
            else:
                dups = dups + 1
                old_viewers = story_records[id]['viewers']

            if old_viewers != viewers:
                if viewers != '[]':
                    MainHelper().play_notify_sound('notify_when_new_insta_view')
                old_viewers = json.loads(old_viewers)
                append_strs = []

                for new_viewer in viewers_ids:
                    if new_viewer not in old_viewers:
                        new_data = viewers_info[str(new_viewer)]
                        new_str = '"' + str(new_data['pk']) + '";"' + \
                                new_data['full_name'] + '";"' + str(new_data['date'].strftime('%Y-%m-%d %H:%M:%S')) + \
                                '";"https://www.instagram.com/' + new_data['username'] + '"'
                        append_strs.append(new_str)
                        print('!!! User ' + new_data['full_name'] + ' watched your story ' + story_fname)

                if len(append_strs) > 0:
                    stories_folder = MainHelper().get_config_root_folder_value('instagram', 'stories_folder')
                    if not os.path.exists(stories_folder):
                        os.makedirs(stories_folder)
                    file_path = stories_folder + "/" + str(username)
                    if not os.path.exists(file_path):
                        os.makedirs(file_path)
                    file_name = story_fname + ".csv"
                    with open(file_path + "/" + file_name, 'a') as file:
                        if len(old_viewers) == 0:
                            file.write('"ID";"Full name";"Date";"User link"' + "\n")
                        for line in append_strs:
                            file.write(line + "\n")
                        file.close()

                if id in story_records:
                    story_records[id]['viewers'] = viewers

        if dups > 0:
            if dups > 1:
                stories_str = "stories"
            else:
                stories_str = "story"
            print(username + ": " + str(dups) + " old " + stories_str + " was skipped.")
        if (not response['items']) or (len(response['items']) == 0):
            print(username + ": Stories was not found.")

        story_records = json.dumps(story_records)
        self.tg_bot_controller.tg_client.entity_controller.set_entity_db_option(0, 'insta_story_entries', story_records)

    @staticmethod
    def get_story_file_name(taken_at, location):
        story_fname = StatusController.timestamp_to_utc_str(taken_at, '%Y-%m-%d--%H%M%S')
        if location:
            story_fname = story_fname + ' ' + location
        story_fname = re.sub(r'[^\w\s\d.,-]', '', story_fname)
        story_fname = story_fname.strip()
        story_fname = re.sub(r'\s+', ' ', story_fname)
        story_fname = re.sub(r'-+', '-', story_fname)
        return story_fname

    def get_session(self):
        session = requests.Session()
        session.headers['user-agent'] = self.branch_parent.api.user_agent
        session.headers['x-ig-capabilities'] = '36oD'
        session.headers['cache-control'] = 'no-cache'
        return session

    def download_stories(self):

        stories_folder = MainHelper().get_config_root_folder_value('instagram', 'stories_folder')
        to_delete, to_update = [], []
        if not os.path.exists(stories_folder):
            os.makedirs(stories_folder)

        try:
            c_session = self.get_session()
        except:
            c_session = None
            print('!!! Error: Cant open session')
            traceback.print_exc()

        story_records = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option(0, 'insta_story_entries')
        if story_records:
            story_records = json.loads(story_records)
        else:
            story_records = {}

        for row in story_records.values():
            if row['filename'] and len(row['filename']) > 0:
                continue
            r = c_session.get(row['url'])
            if r.status_code == 404:
                print("Deleting old story DB record: " + str(row['id']))
                to_delete.append(row['id'])
            elif r.status_code % 200 < 100:
                if len(to_update) == 0:
                    print()
                    print("Downloading new story files:")
                file_name = self.get_story_file_name(row['taken_at'], row['location'])
                if r.headers["Content-Type"] == "video/mp4" or r.headers["Content-Type"] == "text/plain":
                    filename = str(row['username']) + "/" + file_name + ".mp4"
                elif r.headers["Content-Type"] == "image/jpeg":
                    filename = str(row['username']) + "/" + file_name + ".jpg"
                else:
                    filename = str(row['username']) + "/" + file_name + ".unknown"
                    print("WARNING: couldn't identify MIME type for URL " + row['url'])
                if not os.path.exists(stories_folder + "/" + str(row['username'])):
                    os.makedirs(stories_folder + "/" + str(row['username']))
                with open(stories_folder + "/" + filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                to_update.append([row['id'], filename])
                print(filename)

        for item in to_delete:
            if item in story_records:
                del story_records[item]

        for item in to_update:
            if item[0] in story_records:
                story_records[item[0]]['filename'] = item[1]

        story_records = json.dumps(story_records)
        self.tg_bot_controller.tg_client.entity_controller.set_entity_db_option(0, 'insta_story_entries', story_records)

    async def on_timer(self):
        if not self.branch_parent.has_insta_lib:
            return
        if self.last_stories_check_time and (datetime.now() - self.last_stories_check_time).total_seconds() < MainHelper().get_config_int_value('instagram', 'check_story_delay_seconds'):
            return
        self.last_stories_check_time = datetime.now()

        ustories = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option_list('stories_accounts')
        if ustories and (len(ustories) > 0):
            ustories = str("\n".join(ustories)).splitlines()
            ustories = list(dict.fromkeys(ustories))
        else:
            return
        self.branch_parent.do_login_if_need()
        if not self.branch_parent.api:
            print('API-клиент не был инициализирован!')
            return

        insta_id_cache = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option(0, 'insta_user_ids')
        if insta_id_cache:
            insta_id_cache = json.loads(insta_id_cache)
        else:
            insta_id_cache = {}

        if not insta_id_cache:
            insta_id_cache = {}
        for username in ustories:
            if username in insta_id_cache:
                user_id = insta_id_cache[username]
            else:
                info = await self.branch_parent.get_user_info_by_username(None, username)
                if not info:
                    continue
                user_id = int(info['user']['pk'])
                insta_id_cache[username] = user_id
            self.save_user_stories(user_id, username)
        self.download_stories()

        insta_id_cache = json.dumps(insta_id_cache)
        self.tg_bot_controller.tg_client.entity_controller.set_entity_db_option(0, 'insta_user_ids', insta_id_cache)

    async def cmd_set_stories_users(self, from_id, params):
        text = ''
        ustories = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option(from_id, 'stories_accounts')
        if ustories:
            ustories = str(ustories).splitlines()
            text = "\nВыбранные вами пользователи для выгрузки историй: **" + (", ".join(ustories)) + "**\n\n"
        text = text + 'Введите новый список пользователей через запятую, либо введите 0 чтобы очистить список'
        await self.read_bot_str(from_id, self.on_set_user_stories, text)

    async def cmd_reset_stories_users(self, from_id, params):
        self.tg_bot_controller.tg_client.entity_controller.set_entity_db_option(None, 'stories_accounts', None)
        await self.send_message_to_user(from_id, 'Сброс аккаунтов для получения историй выполнен!')

    async def cmd_show_stories_log(self, from_id, params):
        story_records = self.tg_bot_controller.tg_client.entity_controller.get_entity_db_option(0, 'insta_story_entries')
        if story_records:
            story_records = json.loads(story_records)
        else:
            story_records = {}
        story_records = list(filter(lambda xs: xs[1]['filename'] != '', sorted(story_records.items(), key=lambda x: x[1]['taken_at'], reverse=True)))
        if not story_records or len(story_records) == 0:
            await self.send_message_to_user(from_id, 'Истории ещё не скачивались!')
            return
        story_records = story_records[:10]
        story_records = [StatusController.timestamp_to_str(x[1]['taken_at']) + ': файл "' + x[1]['filename'] + '"' for x in story_records]
        results = ['**Лог скачиваний**:\n```']
        results = results + story_records
        results = "\n\n".join(results) + '```'
        await self.send_message_to_user(from_id, results)

    async def on_set_user_stories(self, message, from_id, params):
        message = str(message).strip().lower()
        if message in self.no_variants:
            self.tg_bot_controller.tg_client.entity_controller.set_entity_db_option(from_id, 'stories_accounts', None)
        else:
            message = message.split(',')
            message = [x.strip() for x in message]
            message = list(filter(lambda x: len(x) > 0, message))
            if len(message) > 0:
                message = "\n".join(message)
                self.tg_bot_controller.tg_client.entity_controller.set_entity_db_option(from_id, 'stories_accounts', message)
            else:
                self.tg_bot_controller.tg_client.entity_controller.set_entity_db_option(from_id, 'stories_accounts', None)
        await self.send_message_to_user(from_id, 'Установка аккаунтов для получения историй выполнена!')
