import sys
import json
from datetime import datetime
import matplotlib as mpl
import matplotlib.pyplot as plt

AGE_EVENTS = [
    "prob_13_17",
    "prob_18_24",
    "prob_25_34",
    "prob_35_over"
]
GENDER_EVENTS = [
    "prob_male",
    "prob_female",
    "prob_non_binary_gender_expansive"
]

def get_event_date(event):
    return datetime.strptime(
        event["day_pt"],
        "%Y-%m-%d %H:%M:%S %Z"
    ) 

def ingest_events(file_stream):
    age_events = []
    gender_events = []
    for line in file_stream:
        event = json.loads(line)
        if "predicted_age" in event:
            age_events.append(event)
        elif "predicted_gender" in event:
            gender_events.append(event)
    return age_events, gender_events

def transform_events(event_list, keys):
    times = []
    keys_out = {k:[] for k in keys}
    for event in event_list:
        times.append(get_event_date(event))
        for k in keys:
            keys_out[k].append(event[k])
    return times, keys_out

with open(sys.argv[1], "r") as file_stream:
    print("Ingesting")
    age_events, gender_events = ingest_events(file_stream)
    
    print("Sorting")
    age_events.sort(key=get_event_date)
    gender_events.sort(key=get_event_date)

    print("transforming")
    age_xs, age_keys_out = transform_events(age_events, AGE_EVENTS)
    gender_xs, gender_keys_out = transform_events(gender_events, GENDER_EVENTS)

    print("Plotting age")
    age_fig, age_ax = plt.subplots()
    for n, ys in age_keys_out.items():
        age_ax.plot(age_xs, ys, label=n)
    age_ax.legend()
    age_fig.savefig("out/age.svg")

    print("Plotting gender")
    gender_fig, gender_ax = plt.subplots()
    for n, ys in gender_keys_out.items():
        gender_ax.plot(gender_xs, ys, label=n)
    gender_ax.legend()
    gender_fig.savefig("out/gender.svg")

