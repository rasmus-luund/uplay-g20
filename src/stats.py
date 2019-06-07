import dateutil.parser
import logging


def _normalize_last_played(card):
    iso_datetime = card.get('lastModified', None)
    if iso_datetime:
        dt = dateutil.parser.parse(iso_datetime)
        return round(dt.timestamp())


def _normalize_playtime(card):
    """ All known games has 'unit': 'Seconds' in for time fields
    Champions of Anteria is exception, it has 'Hours' for playtime, but uses seconds in fact
    That is why we assume Seconds everywhere
    :param card     statistic card with 'format': 'LongTimespan'
    :return         playtime in minutes
    """
    value = card.get('value', None)
    unit = card.get('unit', None)
    if unit != 'Seconds':
        logging.warning(f'Unexpected unit [{unit}] with value: {value}. Seconds assumed anyway')
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None
    else:
        return value / 60


def find_playtime(statscards, default_total_time=0, default_last_played=0):
    """ Returns played time in minutes or None if not found.
    Default values are returned if card(s) was found 
    """
    cards = []
    time_stats = [card for card in statscards if card['format'] == 'LongTimespan']
    if len(time_stats) == 0:
        pass
    elif len(time_stats) == 1:
        cards = [time_stats[0]]
    else:
        for st in time_stats:
            if st['displayName'].lower() in ['playtime', 'time played', 'play time']:
                cards = [st]
                break
        else:
            if len(time_stats) == 2:
                # for games with separate time tracking for two game modes
                n1 = time_stats[0]['statName'].lower()
                n2 = time_stats[1]['statName'].lower()
                if (('pvp' in n1 and 'pve' in n2) or ('pve' in n1 and 'pvp' in n2)) or \
                    (('solo' in n1 and 'coop' in n2) or ('coop' in n1 and 'solo' in n2)) or \
                    (('single' in n1 and 'multi' in n2) or ('multi' in n1 and 'solo' in n2)):
                    cards = time_stats
            else:
                # guessing with indexing based on keywords
                for st in time_stats:
                    st['_weight'] = 0
                    for sup in ['all', 'total', 'absolute']:
                        if sup in st['displayName'].lower() or sup in st['statName'].lower():
                            st['_weight'] += 1
                time_stats_sorted = sorted(time_stats, key=lambda x: x['_weight'], reverse=True)
                max_weight = time_stats_sorted[0]['_weight']
                for st in time_stats_sorted:
                    if st['_weight'] == max_weight:
                        cards.append(st)
                    else:  # only less probable cards left
                        break

    # no candidate cards, no stats
    if len(cards) == 0:
        return (None, None)

    time_sum = default_total_time
    last_played = default_last_played
    for card in cards:  # in most cases there is one card
        card_time = _normalize_playtime(card)
        if card_time is not None:
            time_sum += card_time
        card_last_modified = _normalize_last_played(card)
        if card_last_modified and card_last_modified > last_played:
            last_played = card_last_modified
    if type(time_sum) == float:
        time_sum = round(time_sum)
    return (time_sum, last_played)
