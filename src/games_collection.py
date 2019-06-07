import logging as log
from definitions import GameStatus


class GamesCollection(list):

    def get_local_games(self):
        local_games = []
        for game in self:
            if game.status in [GameStatus.Installed, GameStatus.Running]:
                local_games.append(game)
        return local_games

    def append(self, games):
        spaces = set([game.space_id for game in self if game.space_id])
        launches = set([game.launch_id for game in self if game.launch_id])

        for game in games:
            if game.space_id not in spaces and game.launch_id not in launches:
                super().append(game)
                continue
            if game.space_id in spaces or game.launch_id in launches:
                for game_in_list in self:
                    if game.space_id == game_in_list.space_id or game.launch_id == game_in_list.launch_id:
                        if game.launch_id and not game_in_list.launch_id:
                            log.debug(f"Extending existing game entry {game_in_list} with launch id: {game.launch_id}")
                            game_in_list.launch_id = game.launch_id
                        if game.space_id and not game_in_list.space_id:
                            log.debug(f"Extending existing game entry {game_in_list} with space id: {game.space_id}")
                            game_in_list.space_id = game.space_id
                        if game.status is not GameStatus.Unknown and game_in_list.status is GameStatus.Unknown:
                            game_in_list.status = game.status
                        if game.owned:
                            game_in_list.owned = game.owned




