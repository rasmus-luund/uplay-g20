import os
import time
import logging as log
import math
import re

from threading import Thread

import psutil as psutil
import yaml

from definitions import UbisoftGame, GameType, GameStatus, ProcessType, WatchedProcess, SYSTEM, System

from consts import UBISOFT_REGISTRY_LAUNCHER, UBISOFT_REGISTRY_LAUNCHER_INSTALLS, \
    UBISOFT_CONFIGURATIONS_BLACKLISTED_NAMES

from steam import get_steam_game_status

if SYSTEM == System.WINDOWS:
    import winreg


def _get_registry_value_from_path(top_key, registry_path, key):
    with winreg.OpenKey(top_key, registry_path, 0, winreg.KEY_READ) as winkey:
        return winreg.QueryValueEx(winkey, key)[0]


def _return_local_game_path_from_special_registry(special_registry_path):
    if not special_registry_path:
        return GameStatus.NotInstalled
    try:
        install_location = _get_registry_value_from_path(winreg.HKEY_LOCAL_MACHINE, special_registry_path,
                                                         "InstallLocation")
        return install_location
    except WindowsError:
        # Entry doesn't exist, game is not installed.
        return ""
    except Exception as e:
        log.warning(f"Unable to read special registry status for {special_registry_path}: {repr(e)}")
        return ""


def _return_local_game_path(launch_id):
    installs_path = UBISOFT_REGISTRY_LAUNCHER_INSTALLS
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, installs_path):
            try:
                with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, installs_path + f'\\{launch_id}') as lkey:
                    game_path, _ = winreg.QueryValueEx(lkey, 'InstallDir')
                    return os.path.normcase(os.path.normpath(game_path))
            except OSError:
                return ""  # end of iteration
    except WindowsError:
        return ""  # Game not installed / during installation


def _smart_return_local_game_path(special_registry_path, launch_id):
    local_game_path = _return_local_game_path(launch_id)
    if not local_game_path and special_registry_path:
        local_game_path = _return_local_game_path_from_special_registry(special_registry_path)
    return local_game_path


def _is_file_at_path(path, file):
    if os.path.isdir(path):
        file_location = os.path.join(path, file)
        if os.path.isfile(file_location):
            return True
        return False
    else:
        return False


def _read_status_from_state_file(game_path):
    try:
        if os.path.exists(os.path.join(game_path, 'uplay_install.state')):
            with open(os.path.join(game_path, 'uplay_install.state'), 'rb') as f:
                if f.read()[0] == 0x0A:
                    return GameStatus.Installed
                else:
                    return GameStatus.NotInstalled
        # State file doesn't exit
        else:
            return GameStatus.NotInstalled
    except Exception as e:
        log.warning(f"Issue reading install state file for {game_path}: {repr(e)}")
        return GameStatus.NotInstalled


def _return_game_installed_status(path, exe=None, special_registry_path=None):
    status = GameStatus.NotInstalled
    try:
        if path and os.access(path, os.F_OK):
            status = _read_status_from_state_file(path)
            # Fallback for old games
            if status == GameStatus.NotInstalled and exe and special_registry_path:
                if _is_file_at_path(path, exe):
                    status = GameStatus.Installed
    except Exception as e:
        log.error(f"Error reading game installed status at {path}: {repr(e)}")
    finally:
        return status


class LocalClient(object):
    def __init__(self):
        self.last_modification_times = None
        self.configurations_path = None
        self.ownership_path = None
        self.launcher_log_path = None
        self.user_id = None
        self._is_installed = None
        self.refresh()

    def initialize(self, user_id):
        log.info('Setting user id:' + user_id)
        self.user_id = user_id
        self.refresh()
        # Start tracking ownership file if exists
        self.ownership_changed()

    def ownership_accesible(self):
        if self.ownership_path is None:
            return False
        else:
            return os.access(self.ownership_path, os.R_OK)

    def configurations_accessible(self):
        if self.configurations_path is None:
            return False
        else:
            return os.access(self.configurations_path, os.R_OK)

    def __read_file(self, filepath):
        try:
            with open(filepath, 'rb') as f:
                return f.read()
        except FileExistsError:
            return None

    def read_config(self):
        return self.__read_file(self.configurations_path)

    def read_ownership(self):
        return self.__read_file(self.ownership_path)

    @property
    def is_installed(self):
        return self._is_installed

    @property
    def was_user_logged_in(self):
        if not self.ownership_path:
            return False
        return os.path.exists(self.ownership_path)

    def _find_windows_client(self):
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, UBISOFT_REGISTRY_LAUNCHER, 0,
                                winreg.KEY_READ) as key:
                directory, _ = winreg.QueryValueEx(key, "InstallDir")
                return os.access(directory, os.F_OK), directory
        except OSError:
            return False, ''

    def refresh(self):
        if SYSTEM == System.MACOS:
            return

        exists, path = self._find_windows_client()
        if exists:
            if not self._is_installed:
                log.info('Local client installed')
                self._is_installed = True
            self.configurations_path = os.path.join(path, "cache", "configuration", "configurations")
            self.launcher_log_path = os.path.join(path, "logs", "launcher_log.txt")
            if self.user_id is not None:
                self.ownership_path = os.path.join(path, "cache", "ownership", self.user_id)
        else:
            if self._is_installed:
                log.info('Local client uninstalled')
                self._is_installed = False
            self.configurations_path = None
            self.ownership_path = None
            self.launcher_log_path = None

    def ownership_changed(self):
        path = self.ownership_path
        try:
            stat = os.stat(path)
        except TypeError:
            log.warning(f'Undecided Ownership file path, uplay client might not be installed')
            self.refresh()
        except FileNotFoundError:
            log.warning(f'Ownership file at {path} path not present, user never logged in to uplay client.')
            self.refresh()
        except Exception as e:
            log.exception(f'Stating {path} has failed: {str(e)}')
            self.refresh()
        else:
            if stat.st_mtime != self.last_modification_times:
                self.last_modification_times = stat.st_mtime
                return True
        return False


class ProcessWatcher(object):
    def __init__(self):
        self.watched_processes = []

    def watch_process(self, proces, game=None):
        try:
            process = WatchedProcess(
                process=proces,
                timeout=time.time() + 30,
                type=ProcessType.Game if game else ProcessType.Launcher,
                game=game if game else None,
            )
            self.watched_processes.append(process)
            return process
        except:
            return None

    def update_watched_processes_list(self):
        try:
            for proc in self.watched_processes:
                if not proc.process.is_running():
                    log.info(f"Removing {proc}")
                    self.watched_processes.remove(proc)
        except Exception as e:
            log.error(f"Error removing process from watched processes list {repr(e)}")


class GameStatusNotifier(object):
    def __init__(self, process_watcher):
        self.process_watcher = process_watcher
        self.games = {}
        self.watchers = {}
        self.statuses = {}
        self.launcher_log_path = None
        if SYSTEM == System.WINDOWS:
            Thread(target=self._process_data, daemon=True).start()

    def update_game(self, game: UbisoftGame):

        if game.launch_id in self.watchers:
            if game.path == self.watchers[game.launch_id].path:
                return

        self.games[game.launch_id] = game

    def _is_process_alive(self, game):
        try:
            self.process_watcher.update_watched_processes_list()
            for process in self.process_watcher.watched_processes:
                if process.type == ProcessType.Game:
                    if process.game.launch_id == game.launch_id:
                        return True
            return False
        except Exception as e:
            log.error(f"Error checking if process is alive {repr(e)}")
            return False

    def _parse_log(self, game, line_list):
        try:
            line = len(line_list) - 1
            while line > 0:
                if "disconnected" in line_list[line]:
                    return False
                if "has been started with product id" in line_list[line] and game.launch_id in line_list[line]:
                    pid = int(
                        re.search('Game with process id ([-+]?[0-9]+) has been started', line_list[line]).group(1))
                    if pid:
                        self.process_watcher.watch_process(psutil.Process(pid), game)
                        return True
                line = line - 1
            return False

        except Exception as e:
            log.error(f"Error parsing launcher log file is game running {repr(e)}")
            return False

    def _is_game_running(self, game, line_list):
        try:
            if self.statuses[game.launch_id] == GameStatus.Running:
                return self._is_process_alive(game)
            else:
                return self._parse_log(game, line_list)
        except Exception as e:
            log.error(f"Error in checking is game running {repr(e)}")

    def _get_launcher_log_lines(self, number_of_lines):
        line_list = []
        if self.launcher_log_path:
            try:
                with open(self.launcher_log_path, "r") as fh:
                    line_list = fh.readlines()
                    line_list = line_list[-number_of_lines:]
            except FileNotFoundError:
                pass
            except Exception as e:
                log.warning(
                    f"Can't read launcher log at {self.launcher_log_path}, unable to read running games statuses: {repr(e)}")
        return line_list

    def _process_data(self):
        statuses = {}
        while True:
            line_list = self._get_launcher_log_lines(50)
            try:
                for launch_id, game in self.games.items():

                    if game.type == GameType.Steam:
                        statuses[launch_id] = get_steam_game_status(game.path)
                        continue
                    else:
                        if not game.path:
                            game.path = _smart_return_local_game_path(game.special_registry_path, game.launch_id)

                        statuses[launch_id] = _return_game_installed_status(game.path, game.exe, game.special_registry_path)

                    if statuses[launch_id] == GameStatus.Installed:
                        if self._is_game_running(game, line_list):
                            statuses[launch_id] = GameStatus.Running

            except Exception as e:
                log.error(f"Process data error {repr(e)}")
            finally:
                time.sleep(1)
            self.statuses = statuses


class LocalParser(object):
    def __init__(self):
        self.configuration_raw = None
        self.ownership_raw = None
        self.parsed_games = None

    def _convert_data(self, data):
        # calculate object size (konrad's formula)
        if data > 256 * 256:
            data = data - (128 * 256 * math.ceil(data / (256 * 256)))
            data = data - (128 * math.ceil(data / 256))
        else:
            if data > 256:
                data = data - (128 * math.ceil(data / 256))
        return data

    def _parse_configuration_header(self, header, second_eight=False):
        offset = 1
        multiplier = 1
        record_size = 0
        tmp_size = 0

        if second_eight:
            while header[offset] != 0x08 or (header[offset] == 0x08 and header[offset + 1] == 0x08):
                record_size += header[offset] * multiplier
                multiplier *= 256
                offset += 1
                tmp_size += 1
        else:
            while header[offset] != 0x08 or record_size == 0:
                record_size += header[offset] * multiplier
                multiplier *= 256
                offset += 1
                tmp_size += 1

        record_size = self._convert_data(record_size)

        offset += 1  # skip 0x08

        # look for launch_id
        multiplier = 1
        launch_id = 0

        while header[offset] != 0x10 or header[offset + 1] == 0x10:
            launch_id += header[offset] * multiplier
            multiplier *= 256
            offset += 1

        launch_id = self._convert_data(launch_id)

        offset += 1  # skip 0x10

        while header[offset] != 0x1A or (header[offset] == 0x1A and header[offset + 1] == 0x1A):
            offset += 1

        # if object size is smaller than 128b, there might be a chance that secondary size will not occupy 2b
        if record_size - offset < 128 <= record_size:
            tmp_size -= 1
            record_size += 1

        # we end up in the middle of header, return values normalized
        # to end of record as well real yaml size and game launch_id
        return record_size - offset, launch_id, offset + tmp_size + 1

    def _parse_ownership_header(self, header):
        offset = 1
        multiplier = 1
        record_size = 0
        tmp_size = 0
        if header[offset - 1] == 0x0a:
            while header[offset] != 0x08 or record_size == 0:
                record_size += header[offset] * multiplier
                multiplier *= 256
                offset += 1
                tmp_size += 1

            record_size = self._convert_data(record_size)

            offset += 1  # skip 0x08

            # look for launch_id
            multiplier = 1
            launch_id = 0

            while header[offset] != 0x10 or header[offset + 1] == 0x10:
                launch_id += header[offset] * multiplier
                multiplier *= 256
                offset += 1

            launch_id = self._convert_data(launch_id)

            offset += 1  # skip 0x10

            multiplier = 1
            launch_id_2 = 0
            while header[offset] != 0x22:
                launch_id_2 += header[offset] * multiplier
                multiplier *= 256
                offset += 1

            launch_id_2 = self._convert_data(launch_id_2)
            return launch_id, launch_id_2, record_size + tmp_size + 1
        else:
            return None, None, None

    def _parse_configuration(self):
        configuration_content = self.configuration_raw
        global_offset = 0
        records = {}
        try:
            while global_offset < len(configuration_content):
                data = configuration_content[global_offset:]
                object_size, launch_id, header_size = self._parse_configuration_header(data)

                record = {'size': object_size, 'offset': global_offset + header_size}
                records[launch_id] = record

                global_offset_tmp = global_offset
                global_offset += object_size + header_size

                if global_offset < len(configuration_content) and configuration_content[global_offset] != 0x0A:
                    object_size, launch_id, header_size = self._parse_configuration_header(data, True)
                    global_offset = global_offset_tmp + object_size + header_size
        except:
            log.exception("parse_configuration failed with exception. Possibly 'configuration' file corrupted")
            return {}
        return records

    def _parse_ownership(self):
        ownership_content = self.ownership_raw
        global_offset = 0x108
        records = []
        try:
            while global_offset < len(ownership_content):
                data = ownership_content[global_offset:]
                launch_id, launch_id2, record_size = self._parse_ownership_header(data)
                if launch_id:
                    records.append(launch_id)
                    if launch_id2 != launch_id:
                        records.append(launch_id2)
                    global_offset += record_size
                else:
                    break
        except:
            log.exception("parse_ownership failed with exception. Possibly 'ownership' file corrupted")
            return []
        return records

    def _parse_game(self, game_yaml, launch_id):
        path = ''
        space_id = ''
        third_party_id = ''
        special_registry_path = ''
        status = GameStatus.NotInstalled
        game_type = GameType.New
        game_name = ''
        exe = ''
        launch_id = str(launch_id)

        if 'space_id' in game_yaml['root']:
            space_id = game_yaml['root']['space_id']
        else:
            game_type = GameType.Legacy

        if 'third_party_platform' in game_yaml['root']:
            if game_yaml['root']['third_party_platform']['name'].lower() == 'steam':
                game_type = GameType.Steam
                path = game_yaml['root']['start_game']['steam']['game_installation_status_register']
                status = get_steam_game_status(path)
                if 'start_game' in game_yaml['root']:
                    if 'steam' in game_yaml['root']['start_game']:
                        third_party_id = game_yaml['root']['start_game']['steam']['steam_app_id']
            elif game_yaml['root']['third_party_platform']['name'].lower() == 'origin':
                game_type = GameType.Origin
                path = game_yaml['root']['third_party_platform']['platform_installation_status']['register']
                # todo status = _return_origin_game_status(path)
        else:
            try:
                registry_path = game_yaml['root']['start_game']['online']['executables'][0]['working_directory']['register']
                if "Uninstall" in registry_path:
                    registry_path = registry_path.split("HKEY_LOCAL_MACHINE\\")[1]
                    registry_path = registry_path.split("\\InstallLocation")[0]
                    special_registry_path = registry_path
                    exe = game_yaml['root']['start_game']['online']['executables'][0]['path']['relative']
            except Exception as e:
                log.info(f"Unable to read registry path for game {launch_id}: {repr(e)}")

            path = _smart_return_local_game_path(special_registry_path, launch_id)
            if path:
                status = _return_game_installed_status(path, exe, special_registry_path)

        if 'name' in game_yaml['root']:
            game_name = game_yaml['root']['name']
        # Fallback 1
        if game_name.lower() in UBISOFT_CONFIGURATIONS_BLACKLISTED_NAMES:
            if 'installer' in game_yaml['root'] and 'game_identifier' in game_yaml['root']['installer']:
                game_name = game_yaml['root']['installer']['game_identifier']
        # Fallback 2
        if game_name.lower() in UBISOFT_CONFIGURATIONS_BLACKLISTED_NAMES:
            if 'localizations' in game_yaml and 'default' in game_yaml['localizations'] and 'GAMENAME' in \
                    game_yaml['localizations']['default']:
                game_name = game_yaml['localizations']['default']['GAMENAME']

        log.info(f"Parsed game from configuration {space_id}, {launch_id}, {game_name}")
        return UbisoftGame(
            space_id=space_id,
            launch_id=launch_id,
            third_party_id=third_party_id,
            name=game_name,
            path=path,
            type=game_type,
            special_registry_path=special_registry_path,
            exe=exe,
            status=status
        )

    def parse_games(self, configuration_data):
        self.configuration_raw = configuration_data

        configuration_records = self._parse_configuration()
        for launch_id, game in configuration_records.items():
            if game['size']:
                stream = self.configuration_raw[game['offset']: game['offset'] + game['size']].decode("utf8",
                                                                                                      errors='ignore')
                if stream and 'start_game' in stream:
                    yaml_object = yaml.load(stream)
                    yield self._parse_game(yaml_object, launch_id)

    def get_owned_local_games(self, ownership_data):
        self.ownership_raw = ownership_data
        return self._parse_ownership()
