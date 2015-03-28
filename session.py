#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime

import dataset
import requests
import sqlalchemy
from bs4 import BeautifulSoup
from requests.compat import urljoin


class Form(object):
    def __init__(self, action, values=None):
        self.action = action
        self.values = values or []

    def toURL(self):
        parameters = []
        for key, value in self.values:
            parameters.append('{}={}'.format(key, value))

        return "{}?{}".format(self.action, '&'.join(parameters))


class RubinScraper(object):
    def __init__(self, db_connection_string, domain='darmstadt'):
        self.base_url = 'http://' + domain + '.more-rubin1.de/'

        self.db = dataset.connect(db_connection_string)
        t_lastaccess = self.db['updates']
        t_lastaccess.create_column('scraped_at', sqlalchemy.DateTime)

    def scrape(self):
        for sid in self.getSIDsOfMeetings():
            self.getSession(sid)

    def hasWebsiteChanged(self):

        html = requests.get(self.base_url).text
        psoup = BeautifulSoup(html)
        text = psoup.find('div', {'class': 'aktualisierung'}).get_text()
        last_website_update = text[len('Letzte Aktualisierung am:'):]
        websitedatetime = datetime.datetime.strptime(last_website_update, "%d.%m.%Y, %H:%M")
        db_datetime = ""
        query = self.db.query("SELECT max(scraped_at) as lastaccess from updates")
        for row in query:
            db_datetime = row['lastaccess']
        self.db['updates'].insert(dict(scraped_at=datetime.datetime.now()))
        if db_datetime is None:
            return True
        if datetime.datetime.strptime(db_datetime[:16], "%Y-%m-%d %H:%M") < websitedatetime:
            return True
        return False


    def scrapeAttachmentsPage(self, sessionID, agenda_item_id, attachmentsPageURL):
        print("scrape Attachment " + attachmentsPageURL)
        html = requests.get(attachmentsPageURL).text
        soup = BeautifulSoup(html)
        txt = soup.get_text()

        if "Auf die Anlage konnte nicht zugegriffen werden oder Sie existiert nicht mehr." in txt:
            print("Zu TOP " + agenda_item_id + " fehlt mindestens eine Anlage")
            errortable = self.db['404attachments']
            errortable.insert(dict(agenda_item_id=agenda_item_id, attachmentsPageURL=attachmentsPageURL))

        for forms in soup.find_all('form'):
            title = forms.get_text()
            values = list()
            for val in forms.find_all('input', {'type': 'hidden'}):
                values.append([val['name'], val['value']])

            form = Form(forms['action'], values)
            url = self.base_url + form.toURL()

            tab = self.db['attachments']
            tab.insert(
                dict(sid=sessionID, agenda_item_id=agenda_item_id, attachment_title=title, attachment_file_url=url))


    def getSIDsOfMeetings(self):
        # TODO: the dates should be automatically generated
        scrape_from = '01.01.2006'
        scrape_to = '31.01.2006'

        url = urljoin(self.base_url, 'recherche.php')
        params = {'suchbegriffe': '', 'select_gremium': '',
                  'datum_von': scrape_from, 'datum_bis': scrape_to,
                  'startsuche': 'Suche+starten'}

        entry = 0  # set for first run
        SIDs = set()
        notempty = 1
        while notempty > 0:
            # prevent infinite loop
            notempty = 0
            params['entry'] = entry - 1
            site_content = requests.get(url, params=params).text
            table = BeautifulSoup(site_content).find('table', {"width": "100%"})

            for inputs in table.find_all('input', {"name": "sid"}):
                sid = inputs["value"]
                if not sid:
                    continue

                if sid not in SIDs:
                    SIDs.add(inputs["value"])
                    entry = entry + 1
                    notempty = notempty + 1
                    yield sid


    def getSession(self, sid):
        if not sid:
            raise RuntimeError("Missing session ID.")
        print(sid)

        url = urljoin(self.base_url, "sitzungen_top.php")
        site_content = requests.get(url, params={"sid": sid}).text
        soup = BeautifulSoup(site_content)

        session = {'sid': sid}
        session['title'] = soup.find('b', {'class': 'Suchueberschrift'}).get_text()
        # METADATEN
        table = soup.find('div', {'class': 'InfoBlock'}).find('table')
        values = self.parseTable(table)

        for row in values:
            if row[0] == "Termin: ":
                datum = row[1]
                if len(datum) == len("29.11.2006, 15:00 Uhr - 15:45 Uhr"):
                    beginn = datetime.datetime.strptime(datum[:10]+" "+datum[12:17], "%d.%m.%Y %H:%M")
                    ende = datetime.datetime.strptime(datum[:10]+" "+datum[24:29], "%d.%m.%Y %H:%M")
                    delta = ende - beginn
                    session['start'] = str(beginn)
                    session['end'] = str(ende)
                    session['date'] = datum[0:10]
                    session['duration'] = str(delta.seconds / 60)
            elif row[0] == "Raum: ":
                session['location'] = str(row[1])
            elif row[0] == "Gremien: ":
                session['body'] = str(row[1])
            t_sessions = self.db['sessions']
            print(session)
            t_sessions.insert(session)

        for tab in soup.find_all('table'):
            tr = int(len(tab.find_all('tr')))
            td = int(len(tab.find_all('td')))
            if td > 9 * tr:
                tops = self.parseTable(tab)
                self.parseTOPs(sid, tops)

    def parseTable(self, table):
        values = list()
        for TRs in table.find_all('tr'):
            row = list()
            for TDs in TRs.find_all('td'):
                if TDs.form is not None:
                    url = self.extractHiddenFormURL(TDs)
                    row.append(url)
                else:
                    row.append(TDs.get_text())
            values.append(row)
        return values

    def extractHiddenFormURL(self, td):
        form = Form(td.form['action'])

        for tag in td.form.find_all('input', {'type': 'hidden'}):
            form.values.append((tag['name'], tag['value']))

        return urljoin(self.base_url, form.toURL())

    def parseTOPs(self, sid, tops):
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

            tab = self.db['agenda']
            attachment_link = top[6]
            tab.insert(
                dict(sid=sid, status=top[0], topnumber=top[1], column3=top[2], details_link=top[3], title_full=top[4],
                     document_link=top[
                         5], attachment_link=attachment_link, decision_link=top[7], column9=top[8], column10=top[9],
                     year=jahr,
                     billnumber=vorlnr, billid=gesamtID, position=count))
            if "http://" in attachment_link:
                self.scrapeAttachmentsPage(sid, gesamtID, attachment_link)


if __name__ == '__main__':
    scraper = RubinScraper('sqlite:///darmstadt.db')
    scraper.scrape()

    rest = scraper.db['sessions'].all()
    dataset.freeze(rest, format='json', filename='da-sessions.json')
    rest = scraper.db['sessions'].all()
    dataset.freeze(rest, format='csv', filename='da-sessions.csv')

    rest = scraper.db['agenda'].all()
    dataset.freeze(rest, format='json', filename='da-agenda.json')
    rest = scraper.db['agenda'].all()
    dataset.freeze(rest, format='csv', filename='da-agenda.csv')
