from scrapi.processing.osf import crud
from scrapi.processing.osf import collision
from scrapi.processing.base import BaseProcessor


class OSFProcessor(BaseProcessor):
    NAME = 'osf'

    def process_normalized(self, raw_doc, normalized):
        if crud.is_event(normalized):
            crud.dump_metdata(normalized, {})
            return

        normalized['collisionCategory'] = crud.get_collision_cat(normalized['source'])

        report_norm = normalized
        resource_norm = crud.clean_report(normalized)

        report_hash = collision.generate_report_hash_list(report_norm)
        resource_hash = collision.generate_resource_hash_list(resource_norm)

        report = collision.detect_collisions(report_hash)
        resource = collision.detect_collisions(resource_hash, is_resource=True)

        if not resource:
            resource = crud.create_resource(resource_norm, resource_hash)
        else:
            crud.dump_metadata(resource_norm, {'nid': resource})

        if not report:
            report = crud.create_report(report_norm, resource, report_hash)
        else:
            crud.dump_metadata(report_norm, {'nid': report, 'pid': resource})

        crud.update_report(report, report_norm)
        if not crud.is_claimed(resource):
            crud.update_resource(resource, resource_norm)
