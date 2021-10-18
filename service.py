# !/usr/bin/env python3

from sqlalchemy import create_engine
import pandas
import os.path

active = pandas.read_pickle("active.pkl")

active_interactive = active[active['activeType'] == '2']

engine = create_engine('sqlite://', echo=False)

active_interactive.to_sql('active_interactive', con=engine)

print(active_interactive)

execute_interactive = engine.execute(
    "SELECT title,Image,accountId,activeType,dateFilter,countryCode,Location,rangeEffective,rangeInteractive FROM active_interactive order by dateFilter,activeType,rangeInteractive desc ")

active_interactive = pandas.DataFrame(execute_interactive)

active_effective = active[active['activeType'] == '1']

active_effective.to_sql('active_effective', con=engine)

execute_effective = engine.execute(
    "SELECT title,Image,accountId,activeType,dateFilter,countryCode,Location,rangeEffective,rangeInteractive FROM active_effective order by dateFilter,activeType,rangeEffective desc ")

active_effective = pandas.DataFrame(execute_effective)

active_result = active_effective.append(active_interactive)

active_result.columns = ['title', 'Image', 'accountId', 'activeType', 'dateFilter', 'countryCode', 'Location',
                         'rangeEffective', 'rangeInteractive']


active_result.to_json('sna.json', orient='records')

import requests

import json


def getTicket():
    # put the ip address or dns of your apic-em controller in this url
    url = "https://...
    with open('sna.json') as json_file:
        mydata = json.load(json_file)
    # the username and password to access the APIC-EM Controller
    payload = mydata
    # Content type must be included in the header
    header = {"content-type": "application/json"}
    # Performs a POST on the specified url to get the service ticket
    response = requests.post(url, data=json.dumps(payload), headers=header, verify=False)
    r_json = response.json()
    print(r_json)
    # parse the json to get the service ticket
    ticket = r_json["response"]["serviceTicket"]
    return ticket


print(getTicket())
