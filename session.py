#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from urllib.parse import urljoin

import dataset
import requests
import sqlalchemy
from bs4 import BeautifulSoup


# config
base_url = 'http://darmstadt.more-rubin1.de/'
db_file = 'darmstadt.db'

# setup DB
db = dataset.connect('sqlite:///' + db_file)
t_lastaccess = db['lastscrape']
t_lastaccess.create_column('scraped_at', sqlalchemy.DateTime)


############################################
############################################

class Form(object):

    def __init__(self, action, values=None):
        self.action = action
        self.values = values or []

    def toURL(self):
        parameters = []
        for key, value in self.values:
            parameters.append('{}={}'.format(key, value))

        return "{}?{}".format(self.action, '&'.join(parameters))


def getSIDsOfMeetings():
    scrape_from = '01.01.2006'
    scrape_to = '31.01.2006'
    search_parameters = ("recherche.php?suchbegriffe=&select_gremium=&"
                         "datum_von={start}&datum_bis={end}&"
                         "startsuche=Suche+starten")
    search_parameters = search_parameters.format(start=scrape_from,
                                                 end=scrape_to)

    starturl = urljoin(base_url, search_parameters)
    entry = 0  # set for first run
    SIDs = set()
    notempty = 1
    while notempty > 0:
        # prevent infinite loop
        notempty = 0
        site_content = requests.get(starturl + "&entry=" + str(entry - 1)).text
        table = BeautifulSoup(site_content).find('table', {"width": "100%"})

        for inputs in table.find_all('input', {"name": "sid"}):
            sid = inputs["value"]
            if sid not in SIDs:
                SIDs.add(inputs["value"])
                entry = entry + 1
                notempty = notempty + 1
                yield sid


def getSession(sid):
    if not sid:
        raise RuntimeError("Missing session ID.")
    print(sid)

    target_url = urljoin(base_url, "sitzungen_top.php")
    site_content = requests.get(target_url, params={"sid": sid}).text
    soup = BeautifulSoup(site_content)

    session = {'sid': sid}
    session['title'] = soup.find('b', {'class': 'Suchueberschrift'}).get_text(
    )
    # METADATEN
    table = soup.find('div', {'class': 'InfoBlock'}).find('table')
    values = parseTable(table)

    for row in values:
        if row[0] == "Termin: ":
            datum = row[1]
            if len(datum) == len("29.11.2006, 15:00 Uhr - 15:45 Uhr"):
                beginn = datetime.datetime.strptime(datum[12:17], "%H:%M")
                ende = datetime.datetime.strptime(datum[24:29], "%H:%M")
                delta = ende - beginn
                session['start'] = str(beginn)
                session['end'] = str(ende)
                session['date'] = datum[0:10]
                session['duration'] = str(delta.seconds / 60)
        elif row[0] == "Raum: ":
            session['location'] = str(row[1])
        elif row[0] == "Gremien: ":
            session['body'] = str(row[1])
        t_sessions = db['sessions']
        print(session)
        t_sessions.insert(session)

    for tab in soup.find_all('table'):
        tr = int(len(tab.find_all('tr')))
        td = int(len(tab.find_all('td')))
        if td > 9 * tr:
            tops = parseTable(tab)
            parseTOPs(sid, tops)


def parseTable(table):
    values = list()
    for TRs in table.find_all('tr'):
        row = list()
        for TDs in TRs.find_all('td'):
            if TDs.form != None:
                url = extractHiddenFormURL(TDs)
                row.append(url)
            else:
                row.append(TDs.get_text())
        values.append(row)
    return values


def extractHiddenFormURL(td):
    form = Form(td.form['action'])

    for tag in td.form.find_all('input', {'type': 'hidden'}):
        form.values.append((tag['name'], tag['value']))

    return urljoin(base_url, form.toURL())


def parseTOPs(sid, tops):
    count = 0
    for top in tops:
        count = count + 1
        vorlnr = ""
        gesamtID = ""
        jahr = ""
        if '[Vorlage: ' in top[4]:
            if '[Vorlage: SV-' in top[4]:
                jahr = top[4][len("Vorlage: SV-") + 1:len("Vorlage: SV-") + 5]
                vorlnr = top[4][
                    len("Vorlage: SV-") + 6:len("Vorlage: SV-") + 10]
            else:
                jahr = top[4][len("Vorlage: ") + 1:len("Vorlage: ") + 5]
                vorlnr = top[4][len("Vorlage: ") + 6:len("Vorlage: ") + 10]
            gesamtID = top[4][10:top[4].index(',')]

        tab = db['agenda']
        tab.insert(dict(sid=sid, status=top[0], topnumber=top[1], column3=top[2], details_link=top[3], title_full=top[4], document_link=top[
                   5], attachment_link=top[6], decision_link=top[7], column9=top[8], column10=top[9], year=jahr, billnumber=vorlnr, billid=gesamtID, position=count))


#######################


if __name__ == '__main__':
    for sid in getSIDsOfMeetings():
        getSession(sid)

    # getSession("ni_2006-HFA-7")
    rest = db['sessions'].all()
    dataset.freeze(rest, format='json', filename='da-sessions.json')
    rest = db['sessions'].all()
    dataset.freeze(rest, format='csv', filename='da-sessions.csv')

    rest = db['agenda'].all()
    dataset.freeze(rest, format='json', filename='da-agenda.json')
    rest = db['agenda'].all()
    dataset.freeze(rest, format='csv', filename='da-agenda.csv')
