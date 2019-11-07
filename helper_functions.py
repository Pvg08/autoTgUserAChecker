import configparser
import os
import sys

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

    def get_from_cache(self, data_name, key, default=None):
        pass

    def save_to_cache(self, data_name, key, data):
        pass
