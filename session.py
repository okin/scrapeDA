#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# scrapeDA - Scraping the city council information system.
# Copyright (C) 2015 Markus Drenger, Niko Wenselowski, Martin Weinelt

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import locale
import re
import sys

import dataset
import requests
import sqlalchemy
from bs4 import BeautifulSoup
from datetime import datetime
from requests.compat import urljoin


try:
    locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')
except locale.Error:
    print("Setting the required german locale failed. Please install!")
    raise

COMMITTEES = set([
    'AEA',  # Akteneinsichtsausschuss
    'AER',  # Ältestenrat
    'Bau',  # Ausschuss für Bauen, Stadtplanung, Verkehr und Liegenschaften
    'Schul',  # Ausschuss für Bildung und Schule
    'SportA',  # Ausschuss für Familie, Kinderbetreuung und Sport bis 30.09.2011
    'GleichA',  # Ausschuss für Gleichstellung und interkulturelle Fragen bis 30.09.2011
    'LiegenA',  # Ausschuss für Liegenschaften und Wirtschaftsförderung bis 30.09.2011
    'SozialA',  # Ausschuss für Soziales (einschl. Gleichstellung, Interkulturelles, Familie und Kinderbetreuung)
    'Sport',  # Ausschuss für Sport und Gesundheit (einschl. öffentliche Einrichtungen und Ordnungswesen)
    'UmweltA',  # Ausschuss für Umweltschutz und Nachhaltigkeit
    'Wifoe',  # Ausschuss für Wirtschaftsförderung und Wissenschaft
    'HFA',  # Haupt- und Finanzausschuss (einschl. Recht, Stellenplan und Beteiligungen)
    'JHA',  # Jugendhilfeausschuss
    'KulturA',  # Kulturausschuss
    'Mag',  # Magistrat der Stadt Darmstadt
    'OBW',  # Ortsbeirat Darmstadt-Wixhausen
    'BauA',  # Planungs-, Bau- und Verkehrsausschuss bis 30.09.2011
    'Stavo',  # Stadtverordnetenversammlung
    'WahlA',  # Wahlvorbereitungsausschuss
])


class Form(object):
    def __init__(self, action, values=None):
        self.action = action
        self.values = values or []

    def toURL(self):
        parameters = []
        for key, value in self.values:
            parameters.append('{}={}'.format(key, value))

        return "{}?{}".format(self.action, '&'.join(parameters))


class MeetingFinder(object):
    """Find possible sessions."""

    def __init__(self, year, domain):
        self.year = year
        self.base_url = 'http://{}.more-rubin1.de/'.format(domain)

        self.startdate = datetime(year, 1, 1)
        self.enddate = datetime(year, 12, 31)

    def __iter__(self):
        def iterator():
            for session in self.get_meetings():
                yield session

        return iterator()

    def get_meetings(self, committee=''):
        scrape_from = self.startdate.strftime('%d.%m.%Y')
        scrape_to = self.enddate.strftime('%d.%m.%Y')

        url = urljoin(self.base_url, 'recherche.php')
        params = {'suchbegriffe': '', 'select_gremium': '',
                  'datum_von': scrape_from, 'datum_bis': scrape_to,
                  'startsuche': 'Suche+starten'}

        i = 0
        meeting_ids = set()
        exhausted = False
        while not exhausted:
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

    def has_website_changed(self, since):
        html = requests.get(self.base_url).text
        soup = BeautifulSoup(html)

        text = soup.find('div', {'class': 'aktualisierung'}).get_text()
        last_website_update = text[len('Letzte Aktualisierung am:'):]
        websitedatetime = datetime.strptime(last_website_update, "%d.%m.%Y, %H:%M")
        if since is None:
            return True
        if datetime.strptime(since[:16], "%Y-%m-%d %H:%M") < websitedatetime:
            return True
        return False

    def get_metadata(self, meeting_id):
        url = urljoin(self.base_url, 'sitzungen_top.php')
        html = requests.get(url, params={"sid": meeting_id}).text
        soup = BeautifulSoup(html)

        meeting = {'sid': meeting_id, 'title': soup.find('b', {'class': 'Suchueberschrift'}).get_text()}

        table = soup.find('div', {'class': 'InfoBlock'}).find('table')

        pattern_dt = '(?P<day>\d{2})\.(?P<month>\d{2})\.(?P<year>\d{4}),\s(?P<from_h>\d{2}):(?P<from_m>\d{2})\sUhr\s-\s(?P<until_h>\d{2}):(?P<until_m>\d{2})\sUhr'

        for tr in table.find_all('tr'):
            td = tr.find_all('td')
            if not td:
                continue
            try:
                key = td[0].get_text().strip()
                value = td[1].get_text().strip()
            except IndexError:
                continue

            if key == 'Termin:':
                result = re.search(pattern_dt, value)
                if result:
                    begin = datetime(int(result.group('year')), int(result.group('month')), int(result.group('day')),
                                     int(result.group('from_h')), int(result.group('from_m')))
                    end = datetime(int(result.group('year')), int(result.group('month')), int(result.group('day')),
                                   int(result.group('until_h')), int(result.group('until_m')))
                    duration = end - begin

                    meeting['begin'] = str(begin)
                    meeting['end'] = str(end)
                    meeting['duration'] = str(int(duration.seconds / 60))
                else:
                    print('Unparseable date/time info in meeting {}: "{}"'.format(meeting_id, value),
                          file=sys.stderr)
            elif key == 'Raum:':
                meeting['location'] = value
            elif key == 'Gremien:':
                meeting['body'] = value

        return meeting

    def get_toc(self, session_id):
        url = urljoin(self.base_url, 'sitzungen_top.php')
        html = requests.get(url, params={"sid": session_id}).text
        soup = BeautifulSoup(html)

        table = soup.find('div', {'id': 'ajax_sitzungsmappe'}).table
        toc = self.parse_table(table)
        for entry in self.parse_toc(session_id, toc):
            yield entry

    def parse_table(self, table):
        values = []
        for TRs in table.find_all('tr'):
            row = []
            for TDs in TRs.find_all('td'):
                if TDs.form is not None:
                    url = self.get_url_from_form(TDs)
                    row.append(url)
                else:
                    row.append(TDs.get_text())
            values.append(row)
        return values

    def get_url_from_form(self, td):
        form = Form(td.form['action'])

        for tag in td.form.find_all('input', {'type': 'hidden'}):
            form.values.append((tag['name'], tag['value']))

        return urljoin(self.base_url, form.toURL())

    def parse_toc(self, meeting_id, toc):
        count = 0
        vorlagen_template = "Vorlage: SV-"
        first_length = len(vorlagen_template)
        second_length = len("Vorlage: ")

        for entry in toc:
            count += 1
            vorlnr = ""
            gesamt_id = ""
            jahr = ""
            if '[Vorlage: ' in entry[4]:
                if '[Vorlage: SV-' in entry[4]:
                    jahr = entry[4][first_length + 1:first_length + 5]
                    vorlnr = entry[4][first_length + 6:first_length + 10]
                else:
                    jahr = entry[4][second_length + 1:second_length + 5]
                    vorlnr = entry[4][second_length + 6:second_length + 10]
                gesamt_id = entry[4][10:entry[4].index(',')]

            attachment_link = entry[6]
            yield {'sid': meeting_id, 'status': entry[0], 'topnumber': entry[1],
                   'column3': entry[2], 'details_link': entry[3],
                   'title_full': entry[4], 'document_link': entry[5],
                   'attachment_link': attachment_link,
                   'decision_link': entry[7], 'column9': entry[8],
                   'column10': entry[9], 'year': jahr, 'billnumber': vorlnr,
                   'billid': gesamt_id, 'position': count}

    def scrape_attachments_page(self, session_id, agenda_item_id, attachments_page_url):
        print("scrape Attachment " + attachments_page_url)
        html = requests.get(attachments_page_url).text
        soup = BeautifulSoup(html)
        txt = soup.get_text()

        if "Auf die Anlage konnte nicht zugegriffen werden oder Sie existiert nicht mehr." in txt:
            print("Zu TOP " + agenda_item_id + " fehlt mindestens eine Anlage")
            yield ('404', {'agenda_item_id': agenda_item_id,
                           'attachmentsPageURL': attachments_page_url})
        else:
            for forms in soup.find_all('form'):
                title = forms.get_text()
                values = []
                for val in forms.find_all('input', {'type': 'hidden'}):
                    values.append([val['name'], val['value']])

                form = Form(forms['action'], values)
                url = self.base_url + form.toURL()

                yield ('OK', {'sid': session_id,
                              'agenda_item_id': agenda_item_id,
                              'attachment_title': title,
                              'attachment_file_url': url})


def export_from_db(database):
    rest = database['sessions'].all()
    dataset.freeze(rest, format='json', filename='da-sessions.json')
    rest = database['sessions'].all()
    dataset.freeze(rest, format='csv', filename='da-sessions.csv')

    rest = database['agenda'].all()
    dataset.freeze(rest, format='json', filename='da-agenda.json')
    rest = database['agenda'].all()
    dataset.freeze(rest, format='csv', filename='da-agenda.csv')


def get_scraping_time(database):
    db_datetime = ""
    query = database.query("SELECT max(scraped_at) as lastaccess from updates")
    for row in query:
        db_datetime = row['lastaccess']
    return db_datetime


if __name__ == '__main__':
    database = dataset.connect('sqlite:///darmstadt.db')
    t_lastaccess = database['updates']
    t_lastaccess.create_column('scraped_at', sqlalchemy.DateTime)
    database['updates'].insert({'scraped_at': datetime.now()})

    scraper = RubinScraper()
    for meeting_id in MeetingFinder(2006, 'darmstadt'):
        if not meeting_id:
            continue
        print(meeting_id)

        metadata = scraper.get_metadata(meeting_id)

        t_sessions = database['sessions']
        t_sessions.insert(metadata)

        tab = database['agenda']
        errortable = database['404attachments']
        attachments = database['attachments']

        for entry in scraper.get_toc(meeting_id):
            tab.insert(entry)

            if "http://" in entry['attachment_link']:
                for (code, attachment) in scraper.scrape_attachments_page(meeting_id, entry['billid'],
                                                                          entry['attachment_link']):
                    if code == '404':
                        errortable.insert(attachment)
                    elif code == 'OK':
                        attachments.insert(attachment)

    export_from_db(database)
