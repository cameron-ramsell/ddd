import os
import json
import collections
import itertools
import statistics
from datetime import datetime
from os import path
from typing import Iterator, Dict, Any

import matplotlib.pyplot as plt

class DumpMessageIterator:
    _channel_iterator: Iterator[os.DirEntry]
    
    _channel_data = Dict[str, Any]
    _message_iterator: Iterator[Dict[str, Any]]

    def __init__(self, root_dir: str):
        self._channel_iterator = os.scandir(root_dir)
        self._fetch_next_channel()
    
    def __iter__(self):
        return self

    def _fetch_next_channel(self):
        found = False
        while not found:
            dir = next(self._channel_iterator)
            if not dir.is_dir():
                continue

            with (
                open(path.join(dir.path, "channel.json"), "r") as channel_f,
                open(path.join(dir.path, "messages.json"), "r") as messages_f
            ):
                message_json = json.load(messages_f)
                if len(message_json) == 0:
                    continue

                found = True
                self._message_iterator = iter(message_json)
                self._channel_data = json.load(channel_f) 

    def __next__(self):
        try:
            cur_msg = next(self._message_iterator)
        except StopIteration:
            self._fetch_next_channel()
            cur_msg = next(self._message_iterator)
        return cur_msg, self._channel_data


class GroupTracker:
    _id: str
    _name: str

    _ct: int
    _time_bins: Dict[datetime, int]

    def __init__(self, id: str, name: str):
        self._id = id
        self._name = name

        self._ct = 0
        self._time_bins = collections.defaultdict(int)

    @staticmethod
    def from_channel(channel_data: Dict[str, Any]):
        return GroupTracker(
            GroupTracker._generate_id(channel_data),
            GroupTracker._generate_name(channel_data)
        )
    
    @staticmethod
    def _generate_id(channel_data: Dict[str, Any]):
        if "guild" in channel_data:
            return channel_data["guild"]["id"]
        return channel_data["id"]

    @staticmethod
    def _generate_name(channel_data: Dict[str, Any]):
        if "guild" in channel_data:
            return channel_data["guild"]["name"] # guilds
        if "name" in channel_data:
            return channel_data["name"] # group chats with a name you are still a member of
        return f"{channel_data["type"]} ({channel_data["id"]})" 

    def register_message(self, message: Dict[str, Any]):
        time = datetime.strptime(
            message["Timestamp"],
            "%Y-%m-%d %H:%M:%S"
        ).date()
        self._time_bins[time] += 1
        self._ct += 1

    def plot_cum(self, axis):
        sorted_bins = list(self._time_bins.items())
        sorted_bins.sort()

        axis.plot(
            [time for (time, _) in sorted_bins],
            list(
                itertools.accumulate([ct for (_, ct) in sorted_bins])
            ),
            label=self._name
        )

    def plot_ravg(self, axis, n=30):
        sorted_bins = list(self._time_bins.items())
        sorted_bins.sort()

        counts = [ct for (_, ct) in sorted_bins]

        ravg_bins = [
            statistics.mean(counts[max(0,i-n):i+1])
            for i in range(len(counts))
        ]
        axis.plot(
            [time for (time, _) in sorted_bins],
            ravg_bins,
            label=self._name
        )

    def calculate_streaks(self):
        sorted_bins = list(self._time_bins.items())
        sorted_bins.sort()
        
        dates = [time for (time,ct) in sorted_bins if ct > 0]

        prev_it = iter(dates)
        cur_it = iter(dates)

        on_streak_arr = []
        off_streak_arr = []

        cur_on_streak_start = next(cur_it)
        for prev, cur in zip(prev_it, cur_it):
            if (cur-prev).days <= 1:
                continue
            on_streak_length = (prev-cur_on_streak_start).days
            on_streak_arr.append((on_streak_length, cur_on_streak_start, prev))
            off_streak_arr.append(((cur - prev).days, cur, prev))

def grab_id(channel_data: Dict[str, Any]):
    if "guild" in channel_data:
        return channel_data["guild"]["id"]
    return channel_data["id"]

def run_analysis(root_message_dir: str):
    N = 5

    message_iterator = DumpMessageIterator(root_message_dir)
    group_tracker_map = {}
    global_tracker = GroupTracker(0, "All Targets") 
    for msg, channel in message_iterator:
        id = grab_id(channel)
        if id not in group_tracker_map:
            group_tracker_map[id] = GroupTracker.from_channel(channel)
        group_tracker_map[id].register_message(msg)
        global_tracker.register_message(msg)

    groups = list(group_tracker_map.values())
    groups.sort(key=lambda group: group._ct)

    fig, ax = plt.subplots()
    for tracker in groups[-N:]:
        tracker.plot_ravg(ax)
    global_tracker.calculate_streaks()
    global_tracker.plot_ravg(ax)
    ax.legend(bbox_to_anchor=(1.04, 0), loc="lower left")
    ax.set_title("Daily messages sent (30-day rolling average)")
    fig.savefig("out/message_groups.svg", bbox_inches = "tight")

if __name__ == "__main__":
    run_analysis(os.sys.argv[1])
