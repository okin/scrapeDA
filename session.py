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


class SessionFinder(object):
    """Find possible sessions."""

    def __init__(self, year, domain):
        self.year = year
        self.base_url = 'http://{}.more-rubin1.de/'.format(domain)

    def __iter__(self):
        def iterator():
            for session in self.get_meetings():
                yield session

        return iterator()

    def get_meetings(self):
        scrape_from = '01.01.{}'.format(self.year)
        scrape_to = '31.01.{}'.format(self.year)

        url = urljoin(self.base_url, 'recherche.php')
        params = {'suchbegriffe': '', 'select_gremium': '',
                  'datum_von': scrape_from, 'datum_bis': scrape_to,
                  'startsuche': 'Suche+starten'}

        i = 0
        meeting_ids = set()
        exhausted = False
        while not exhausted:
            # prevent infinite loop
            params['entry'] = len(meeting_ids)
            content = requests.get(url, params=params).text
            table = BeautifulSoup(content).find('table', {"width": "100%"})

            tags = table.find_all('input', {'name': 'sid'})
            if tags:
                for tag in tags:
                    meeting_id = tag['value']
                    if not meeting_id:
                        continue
                    if meeting_id not in meeting_ids:
                        meeting_ids.add(meeting_id)
                        i += 1
                        yield meeting_id
            else:
                exhausted = True


class RubinScraper(object):
    def __init__(self, domain='darmstadt'):
        self.base_url = 'http://{}.more-rubin1.de/'.format(domain)

    def hasWebsiteChanged(self, since):
        html = requests.get(self.base_url).text
        psoup = BeautifulSoup(html)
        text = psoup.find('div', {'class': 'aktualisierung'}).get_text()
        last_website_update = text[len('Letzte Aktualisierung am:'):]
        websitedatetime = datetime.datetime.strptime(last_website_update, "%d.%m.%Y, %H:%M")
        if since is None:
            return True
        if datetime.datetime.strptime(since[:16], "%Y-%m-%d %H:%M") < websitedatetime:
            return True
        return False

    def scrapeAttachmentsPage(self, sessionID, agenda_item_id, attachmentsPageURL):
        print("scrape Attachment " + attachmentsPageURL)
        html = requests.get(attachmentsPageURL).text
        soup = BeautifulSoup(html)
        txt = soup.get_text()

        if "Auf die Anlage konnte nicht zugegriffen werden oder Sie existiert nicht mehr." in txt:
            print("Zu TOP " + agenda_item_id + " fehlt mindestens eine Anlage")
            yield ('404', {'agenda_item_id': agenda_item_id,
                           'attachmentsPageURL': attachmentsPageURL})
        else:
            for forms in soup.find_all('form'):
                title = forms.get_text()
                values = []
                for val in forms.find_all('input', {'type': 'hidden'}):
                    values.append([val['name'], val['value']])

                form = Form(forms['action'], values)
                url = self.base_url + form.toURL()

                yield ('OK', {'sid': sessionID,
                              'agenda_item_id': agenda_item_id,
                              'attachment_title': title,
                              'attachment_file_url': url})

    def getMetadata(self, sid):
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

        return session

    def getTOPs(self, session_id):
        url = urljoin(self.base_url, "sitzungen_top.php")
        site_content = requests.get(url, params={"sid": session_id}).text
        soup = BeautifulSoup(site_content)

        for tab in soup.find_all('table'):
            tr = int(len(tab.find_all('tr')))
            td = int(len(tab.find_all('td')))
            if td > 9 * tr:
                tops = self.parseTable(tab)
                for top in self.parseTOPs(session_id, tops):
                    yield top

    def parseTable(self, table):
        values = []
        for TRs in table.find_all('tr'):
            row = []
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
        vorlagen_template = "Vorlage: SV-"
        first_length = len(vorlagen_template)
        second_length = len("Vorlage: ")

        for top in tops:
            count = count + 1
            vorlnr = ""
            gesamtID = ""
            jahr = ""
            if '[Vorlage: ' in top[4]:
                if '[Vorlage: SV-' in top[4]:
                    jahr = top[4][first_length + 1:first_length + 5]
                    vorlnr = top[4][first_length + 6:first_length + 10]
                else:
                    jahr = top[4][second_length + 1:second_length + 5]
                    vorlnr = top[4][second_length + 6:second_length + 10]
                gesamtID = top[4][10:top[4].index(',')]

            attachment_link = top[6]
            yield {'sid': sid, 'status': top[0], 'topnumber': top[1],
                   'column3': top[2], 'details_link': top[3],
                   'title_full': top[4], 'document_link': top[5],
                   'attachment_link': attachment_link,
                   'decision_link': top[7], 'column9': top[8],
                   'column10': top[9], 'year': jahr, 'billnumber': vorlnr,
                   'billid': gesamtID, 'position': count}


def exportFromDatabase(database):
    rest = database['sessions'].all()
    dataset.freeze(rest, format='json', filename='da-sessions.json')
    rest = database['sessions'].all()
    dataset.freeze(rest, format='csv', filename='da-sessions.csv')

    rest = database['agenda'].all()
    dataset.freeze(rest, format='json', filename='da-agenda.json')
    rest = database['agenda'].all()
    dataset.freeze(rest, format='csv', filename='da-agenda.csv')


def getScrapingTime(database):
    db_datetime = ""
    query = database.query("SELECT max(scraped_at) as lastaccess from updates")
    for row in query:
        db_datetime = row['lastaccess']
    return db_datetime


if __name__ == '__main__':
    database = dataset.connect('sqlite:///darmstadt.db')
    t_lastaccess = database['updates']
    t_lastaccess.create_column('scraped_at', sqlalchemy.DateTime)
    database['updates'].insert({'scraped_at': datetime.datetime.now()})

    scraper = RubinScraper()
    for sessionID in SessionFinder(2006, 'darmstadt'):
        if not sessionID:
            continue
        print(sessionID)

        metadata = scraper.getMetadata(sessionID)

        t_sessions = database['sessions']
        t_sessions.insert(metadata)

        tab = database['agenda']
        errortable = database['404attachments']
        attachments = database['attachments']

        for top in scraper.getTOPs(sessionID):
            tab.insert(top)

            if "http://" in top['attachment_link']:
                for (code, attachment) in scraper.scrapeAttachmentsPage(sessionID, top['billid'], top['attachment_link']):
                    if code == '404':
                        errortable.insert(attachment)
                    elif code == 'OK':
                        attachments.insert(attachment)

    exportFromDatabase(database)
