import configparser
import json
import os
import stat
import sys

import time
from playsound import playsound


class MetaSingleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class MainHelper(metaclass=MetaSingleton):

    config = None
    root_path = ''

    def init_config(self, config_file):
        config = configparser.RawConfigParser(allow_no_value=True)
        config.read(config_file, encoding='utf-8')
        self.config = config
        pathname = os.path.dirname(sys.argv[0])
        self.root_path = pathname + '/'

    def get_config_value(self, section, param):
        if self.config and (section in self.config) and (param in self.config[section]):
            return self.config[section][param]
        return ''

    def get_config_root_folder_value(self, section, param):
        if self.config and (section in self.config) and (param in self.config[section]):
            return self.root_path + str(self.config[section][param])
        return self.root_path.rstrip('/')

    def get_config_root_file_value(self, section, param):
        if self.config and (section in self.config) and (param in self.config[section]):
            return self.root_path + str(self.config[section][param])
        return self.root_path + 'default.dat'

    def get_config_int_value(self, section, param):
        if self.config and (section in self.config) and (param in self.config[section]):
            return int(self.config[section][param])
        return 0

    def get_config_float_value(self, section, param):
        if self.config and (section in self.config) and (param in self.config[section]):
            return float(self.config[section][param])
        return 0.0

    def is_set_config_event(self, event_name):
        return self.get_config_int_value('events', event_name) == 1

    def play_sound(self, file_name):
        playsound(file_name, False)

    def play_notify_sound(self, notify_name, is_selected=False):
        notify_section = 'notify_selected' if is_selected else 'notify_all'
        if self.get_config_int_value(notify_section, notify_name) == 1:
            notify_sound = self.get_config_value('notify_sounds', notify_name + '_sound')
            if notify_sound:
                playsound(self.root_path + notify_sound, False)
                return True
        return False


class CacheHelper(metaclass=MetaSingleton):

    @staticmethod
    def cache_key_to_str(key):
        if isinstance(key, dict):
            key = "_".join([str(x[0])+'-'+str(x[1]) for x in key.items()])
        elif isinstance(key, list):
            key = "_".join([str(x) for x in key])
        else:
            key = str(key)
        return key

    @staticmethod
    def file_age_in_seconds(pathname):
        return time.time() - os.stat(pathname)[stat.ST_MTIME]

    def get_from_cache(self, data_name, key, max_age_seconds=86400, default=None, sub_folder_key=None):
        file_name = MainHelper().get_config_root_folder_value('main', 'cache_folder') + '/' + data_name
        if sub_folder_key:
            sub_folder_key = self.cache_key_to_str(sub_folder_key)
            file_name = file_name + '/' + sub_folder_key
        file_name = file_name + '/' + self.cache_key_to_str(key) + '.json'
        try:
            file_age = self.file_age_in_seconds(file_name)
            print('Age of cache file ' + file_name + ' is ' + str(file_age))
            if file_age <= max_age_seconds:
                with open(file_name) as json_data:
                    return json.load(json_data)
        except OSError:
            pass
        return default

    def save_to_cache(self, data_name, key, data_object, sub_folder_key=None):
        folder = MainHelper().get_config_root_folder_value('main', 'cache_folder') + '/' + data_name + '/'
        if not os.path.exists(folder):
            os.makedirs(folder)
        if sub_folder_key:
            sub_folder_key = self.cache_key_to_str(sub_folder_key)
            folder = folder + sub_folder_key + '/'
            if not os.path.exists(folder):
                os.makedirs(folder)
        file_name = folder + self.cache_key_to_str(key) + '.json'
        with open(file_name, 'w') as json_file:
            json.dump(data_object, json_file)
