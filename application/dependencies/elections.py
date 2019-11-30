from io import StringIO
import re
import requests
from lxml import etree
from nameko.dependency_providers import DependencyProvider
import xmltodict

BASE_URL = 'https://www.interieur.gouv.fr/avotreservice/elections/telechargements/'


class ElectionRequestError(Exception):
    pass


def election_request(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        raise ElectionRequestError(
            f'Non 200 HTTP response: {resp.status_code}: {resp.test}')
    return xmltodict.parse(resp.text)

class ElectionRequest(object):

    def __init__(self, url, election_id):
        self.url = url
        self.election_id = election_id

    def call(self):
        return election_request(self.url)

    @property
    def feed_id(self):
        return '_'.join([
            self.election_id,
            self.url.rsplit('/', 1)[-1].replace('.xml', '')
        ])

class ElectionURLBundle(object):

    def _scan(self, election_id):
        parser = etree.HTMLParser()

        def is_candidate(url):
            try:
                _, last_seg, filename = url.rsplit('/', 2)
            except ValueError:
                return False

            # if filename == 'FE.xml':
            #     return True
            # if last_seg == filename[-4:] or filename[-4:] == ''.join([last_seg, 'com']):
            #     return True
            if filename == ''.join([last_seg, 'com.xml']):
                return True
                
            return False

        def urls(url):
            if url[-4:] == '.xml':
                if is_candidate(url):
                    yield url
            else:
                html = etree.parse(
                    StringIO(requests.get(url).text), parser)
                for n in html.xpath('//tr/td/a'):
                    next_url = ''.join([url, n.text])
                    if n.attrib['href'] not in url and n.text[-4:] != '.zip':
                        if 'resultats' in next_url:
                            for l in urls(next_url):
                                yield l

        return urls(''.join([BASE_URL, election_id, '/']))

    def results(self, election_id):
        for url in self._scan(election_id):
            yield ElectionRequest(url, election_id)


class Election(DependencyProvider):
    def get_dependency(self, worker_ctx):
        return ElectionURLBundle()