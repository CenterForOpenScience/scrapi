from scrapi.processing.osf import crud
from scrapi.processing.osf import collision
from scrapi.processing.base import BaseProcessor


class OSFProcessor(BaseProcessor):
    NAME = 'osf'

    def process_normalized(self, raw_doc, normalized):
        if crud.is_event(normalized):
            crud.create_event(normalized)
            return

        report_hash = collision.generate_report_hash_list(normalized)
        resource_hash = collision.generate_resource_hash_list(normalized)

        report = collision.detect_collisions(report_hash)
        resource = collision.detect_collisions(resource_hash)

        if not resource:
            resource = crud.create_resource(normalized, resource_hash)
        elif not crud.is_claimed(resource):
            crud.update_resource(normalized, resource)

        if not report:
            crud.create_report(normalized, resource, report_hash)
        else:
            crud.update_report(normalized, report)
