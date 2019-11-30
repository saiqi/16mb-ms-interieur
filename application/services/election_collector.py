import logging
import itertools
from nameko.dependency_providers import DependencyProvider
from nameko.rpc import rpc
from nameko.events import event_handler, BROADCAST
from nameko.messaging import Publisher
from nameko.constants import PERSISTENT
from kombu.messaging import Exchange
from nameko_mongodb import MongoDatabase
import bson.json_util
import pymongo
from application.dependencies.elections import Election
from application.services.meta import META


_log = logging.getLogger(__name__)


class ErrorHandler(DependencyProvider):

    def worker_result(self, worker_ctx, res, exc_info):
        if exc_info is None:
            return

        exc_type, exc, tb = exc_info
        _log.error(str(exc))


class ElectionCollectorError(Exception):
    pass


class ElectionCollectorService(object):
    name = 'election_collector'
    database = MongoDatabase(result_backend=False)
    election = Election()
    error = ErrorHandler()
    pub_input = Publisher(exchange=Exchange(
        name='all_inputs', type='topic', durable=True, auto_delete=True, delivery_mode=PERSISTENT))
    pub_notif = Publisher(exchange=Exchange(
        name='all_notifications', type='topic', durable=True, auto_delete=True, delivery_mode=PERSISTENT))

    def add_election(self, election_id):
        self.database['elections'].update_one(
            {'id': election_id}, {'$set':{'id': election_id}}, upsert=True)

    @staticmethod
    def handle_missing_number(doc, key):
        if key not in doc:
            return None
        d = doc[key]
        if 'Nombre' in doc[key]:
            d = doc[key]['Nombre']
        try:
            return int(d)
        except ValueError:
            return None

    @staticmethod
    def to_boolean(doc, key):
        if key not in doc:
            return None
        if doc[key] == 'O':
            return True
        return False

    @staticmethod
    def extract_scrutin(doc):
        _log.info('Handling scrutin informations ...')
        return {
            'scrutin_type': doc['Type'],
            'scrutin_annee': int(doc['Annee'])
        }

    @staticmethod
    def extract_commune(doc):
        _log.info('Handling commune informations ...')
        return {
            'commune_code': doc['CodSubCom'],
            'commune_lib': doc['LibSubCom'],
            'circonscription_code': doc.get('CodCirLg', None),
            'circonscription_lib': doc.get('LibFraSubCom', None),
            'mode_scrutin': doc.get('ModeScrutin', None)
        }

    @staticmethod
    def extract_tour(doc):
        _log.info('Handling tour informations ...')
        return {
            'num_tour': int(doc['NumTour'])
        }

    @staticmethod
    def extract_mention(doc):
        _log.info('Handling mention informations ...')
        return {
            'inscrits': int(doc['Inscrits']['Nombre']),
            'abstentions': int(doc['Abstentions']['Nombre']),
            'votants': int(doc['Votants']['Nombre']),
            'blancs': ElectionCollectorService.handle_missing_number(
                doc, 'Blancs'),
            'nuls': ElectionCollectorService.handle_missing_number(
                doc, 'Nuls'),
            'blancs_nuls': ElectionCollectorService.handle_missing_number(
                doc, 'BlancsOuNuls'),
            'exprimes': int(doc['Votants']['Nombre'])
        }

    @staticmethod
    def extract_resultats(doc):
        _log.info('Handling resultats informations ...')
        return {
            'nb_sap': ElectionCollectorService.handle_missing_number(doc, 'NbSap'),
            'nb_sp': ElectionCollectorService.handle_missing_number(doc, 'NbSiePourvus')
        }

    @staticmethod
    def extract_departement(doc):
        _log.info('Handling departement information ...')
        return {
            'departement_code': doc['CodDpt'],
            'departement_lib': doc['LibDpt']
        }

    @staticmethod
    def extract_nuance(doc):
        _log.info('Handling nuance information ...')
        return {
            'nuance_code': doc['CodNua'],
            'nuance_lib': doc['LibNua'],
            'nb_voix': int(doc['NbVoix']),
            'nuance_nb_siege': ElectionCollectorService.handle_missing_number(
                doc, 'NbSieges')
        }

    @staticmethod
    def extract_candidat(doc):
        _log.info('Handling candidat information ...')
        return {
            'candidat_numero': ElectionCollectorService.handle_missing_number(
                doc, 'NumPanneauCand'),
            'candidat_nom': doc['NomPsn'],
            'candidat_prenom': doc['PrenomPsn'],
            'candidat_civilite': doc['CivilitePsn'],
            'candidat_ordre': ElectionCollectorService.handle_missing_number(
                doc, 'NumeroOrdCand'),
            'candidat_elu': ElectionCollectorService.to_boolean(doc, 'Elu'),
            'nuance_code': doc.get('CodNua'),
            'nuance_lib': doc.get('LibNua'),
            'nb_voix': int(doc['NbVoix'])
        }
    
    @staticmethod
    def extract_liste(doc):
        _log.info('Handling liste information ...')
        return {
            'liste_code': doc['CodSeqLisCand'],
            'liste_lib': doc['NomListe'],
            'liste_tete_nom': doc.get('NomTeteListe', None),
            'liste_tete_prenom': doc.get('NomTeteListe', None),
            'liste_tete_civilite': doc.get('CiviliteTeteListe', None),
            'liste_nb_elus': ElectionCollectorService.handle_missing_number(doc, 'NbSieges'),
            'nb_voix': int(doc['NbVoix'])
        }

    @staticmethod
    def complete_records(records, extract_func, rec_type, prev):
        def create_record(r):
            c = extract_func(r)
            c.update(prev)
            c['type'] = rec_type
            for m in [meta[0] for meta in META]:
                if m not in c:
                    c[m] = None
            return c
        return [create_record(r) for r in records]
    
    @staticmethod
    def build_records(doc, er):
        _log.info(f'Building data from {er.url}')
        election = doc['Election']
        root = ElectionCollectorService.extract_scrutin(election['Scrutin'])
        root['election_id'] = er.election_id
        root['feed_id'] = er.feed_id

        def ensure_list(d):
            if isinstance(d, list):
                return d
            return [d] 

        def handle_tour(t, prev):
            t_lvl = ElectionCollectorService.extract_tour(t)
            t_lvl.update(prev)
            t_lvl.update(
                ElectionCollectorService.extract_mention(t['Mentions']))

            res = t['Resultats']
            t_lvl.update(
                ElectionCollectorService.extract_resultats(res))
            if 'Nuances' in res:
                return ElectionCollectorService.complete_records(
                    res['Nuances']['Nuance'], ElectionCollectorService.extract_nuance, 'N', t_lvl)
            elif 'Listes' in res:
                return ElectionCollectorService.complete_records(
                    res['Listes']['Liste'], ElectionCollectorService.extract_liste, 'L', t_lvl)
            elif 'Candidats' in res:
                return ElectionCollectorService.complete_records(
                    res['Candidats']['Candidat'], ElectionCollectorService.extract_candidat, 'C', t_lvl)
            else:
                raise ElectionCollectorError(
                    'Cannot find neither Nuances, Listes nor Candidats under Resultats')

        def handle_commune(c, prev):
            c_lvl = ElectionCollectorService.extract_commune(c)
            c_lvl.update(prev)
            return list(itertools.chain.from_iterable([
                handle_tour(t, c_lvl) for t in ensure_list(c['Tours']['Tour'])]))

        if 'Departement' in election:
            root.update(ElectionCollectorService.extract_departement(election['Departement']))
            return list(itertools.chain.from_iterable(
                [handle_commune(c, root) for c in ensure_list(
                    election['Departement']['Communes']['Commune'])]))
        return list(itertools.chain.from_iterable(
            [handle_tour(t, root) for t in ensure_list(
                election['Tours']['Tour'])]))

    def update_checksum(self, id_, checksum):
        self.database['elections'].update_one(
            {'id': id_}, {'$set':{'checksum': checksum}})

    @rpc
    def publish(self, election_id):
        _log.info(f'Publishing election {election_id} ...')
        for r in self.election.results(election_id):
            _log.info(f'Getting {r.url} ...')
            doc = r.call()
            try:
                records = ElectionCollectorService.build_records(
                    doc, r)
            except ElectionCollectorError as e:
                _log.error(f'Error on {r.url}: {str(e)}')
                continue
            data = {
                'referential': {},
                'datastore': [
                    {
                        'write_policy': 'delete_bulk_insert',
                        'meta': META,
                        'target_table': 'french_election',
                        'delete_keys': {'feed_id': r.feed_id},
                        'records': records,
                        'chunk_size': 100
                    }
                ],
                'id': r.feed_id,
                'status': 'CREATED',
                'checksum': None,
                'meta': {'type': 'election', 'source': 'interieur', 'content_id': r.feed_id}
            }
            self.pub_input(bson.json_util.dumps(data))

    @event_handler(
        'loader', 'input_loaded', handler_type=BROADCAST, reliable_delivery=False)
    def ack(self, payload):
        msg = bson.json_util.loads(payload)
        meta = msg.get('meta', None)
        if not meta:
            return
        checksum = msg.get('checksum', None)
        if not checksum:
            return
        if 'type' not in meta or 'source' not in meta or meta['source'] != 'interieur':
            return
        
        self.pub_notif(bson.json_util.dumps({
            'id': msg['id'],
            'source': msg['source'],
            'type': msg['type'],
            'content': 'French election loaded!'
        }))

    @staticmethod
    def is_meta_valid(msg):
        if 'meta' not in msg:
            return False
        if 'type' not in msg['meta'] or 'source' not in msg['meta']:
            return False
        if msg['meta']['type'] != 'election' or msg['meta']['source'] != 'interieur':
            return False
        if 'config' not in msg:
            _log.warning('Missing config within the message. Ignoring ...')
            return False
        if 'election' not in msg['config']:
            _log.error('Missing mandatory field: election')
            return False
        return True

    @event_handler(
        'api_service', 'input_config', handler_type=BROADCAST, reliable_delivery=False)
    def handle_input_config(self, payload):
        msg = bson.json_util.loads(payload)

        if not ElectionCollectorService.is_meta_valid(msg):
            return

        election_id = msg['config']['election']
        _log.info('Received a related input config ...')
        self.add_election(election_id)
        self.publish(election_id)