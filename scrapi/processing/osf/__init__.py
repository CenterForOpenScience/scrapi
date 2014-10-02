from scrapi.processing.osf import crud
from scrapi.processing.osf import collision
from scrapi.processing.base import BaseProcessor


class OSFProcessor(BaseProcessor):
    NAME = 'osf'

    def process_normalized(self, raw_doc, normalized):
        if crud.is_event(normalized):
            crud.create_event(normalized)
            return

        normalized['collisionCategory'] = crud.get_collision_cat(normalized['source'])

        report_norm = normalized
        resource_norm = crud.clean_report(normalized)

        report_hash = collision.generate_report_hash_list(report_norm)
        resource_hash = collision.generate_resource_hash_list(resource_norm)

        report = collision.detect_collisions(report_hash)
        resource = collision.detect_collisions(resource_hash)

        if not resource:
            resource = crud.create_resource(resource_norm, resource_hash)
        elif not crud.is_claimed(resource):
            crud.update_resource(resource_norm, resource)

        if not report:
            crud.create_report(report_norm, resource, report_hash)
        else:
            crud.update_report(report_norm, report)
