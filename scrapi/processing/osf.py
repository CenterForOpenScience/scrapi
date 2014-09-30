import json
import requests

from scrapi import settings
from scrapi.processing.base import BaseProcessor


POST_HEADERS = {
    'Content-Type': 'application/json'
}


class OSFProcessor(BaseProcessor):
    NAME = 'osf'
    HASH_FUNCTIONS = []
    REPORT_HASH_FUNCTIONS = []

    def process_normalized(self, raw_doc, normalized):
        if self.is_event(normalized):
            self.create_event(normalized)
            return

        report_hash = self.generate_hash_list(normalized, self.REPORT_HASH_FUNCTIONS)
        resource_hash = self.generate_hash_list(normalized, self.RESOURCE_HASH_FUNCTIONS)

        report = self.detect_collisions(report_hash)
        resource = self.detect_collisions(resource_hash)

        if not resource:
            resource = self.create_resource(normalized, resource_hash)
        elif not self.is_claimed(resource):
            self.update_resource(normalized, resource)

        if not report:
            self.create_report(normalized, report_hash, resource)
        else:
            self.update_report(normalized, report)

    def generate_hash_list(self, normalized, hashes):
        hashlist = []

        for hashfunc, normalizefunc in hashes:
            normalized_dict = normalizefunc(normalized.attributes)
            hashlist.append(hashfunc(normalized_dict))

        return hashlist

    def detect_collisions(self, normalized, hashlist):
        uuids = 'uuid:{}'.format(','.join(hashlist))
        ret = requests.get()  # TODO
        if ret['count'] > 0:
            return ret['doc']
        return None

    def create_resource(self, normalized, hashlist):
        bundle = {
            'systemData': {
                'uuid': hashlist
            },
            'permissions': ['read']
        }

        self._create_node(normalized, bundle)

    def create_report(self, normalized, parent, hashlist):
        bundle = {
            'parent': parent['id'],
            'category': 'report',
            'systemData': {
                'uuid': hashlist
            },
        }

        self._create_node(normalized, bundle)

    def _create_node(self, normalized, additional):
        contributors = [
            {'name': x['full_name'], 'email': x.get('email')}
            for x in normalized['contributors']
        ]

        bundle = {
            'title': normalized['title'],
            'description': normalized.get('description'),
            'contributors': contributors,
            'tags': normalized.get('tags'),
            'metadata': normalized.attributes,
        }

        kwargs = {
            'auth': settings.OSF_AUTH,
            'data': json.dumps(bundle),
            'headers': POST_HEADERS
        }

        requests.post(settings.OSF_NEW_PROJECT, **kwargs)

    def create_event(self, normalized):
        pass

    def is_event(self, normalized):
        pass

    def update_resource(self, normalized, resource):
        pass

    def update_report(self, normalized, report):
        pass
