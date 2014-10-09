from scrapi.processing.osf import crud
from scrapi.processing.osf import collision
from scrapi.processing.base import BaseProcessor


class OSFProcessor(BaseProcessor):
    NAME = 'osf'

    def process_normalized(self, raw_doc, normalized):
        found, _hash = collision.already_processed(raw_doc)

        if found:
            return

        normalized['meta'] = {
            'docHash': _hash
        }

        # if crud.is_event(normalized):
        #     crud.dump_metadata(normalized, {})
        #     return

        normalized['collisionCategory'] = crud.get_collision_cat(normalized['source'])

        report_norm = normalized
        resource_norm = crud.clean_report(normalized)

        report_hash = collision.generate_report_hash_list(report_norm)
        resource_hash = collision.generate_resource_hash_list(resource_norm)

        # report = collision.detect_collisions(report_hash)
        # resource = collision.detect_collisions(resource_hash, is_resource=True)

        report_norm['meta']['uids'] = report_hash
        resource_norm['meta']['uids'] = resource_hash
        resource_norm['meta']['isResource'] = True

        crud.dump_metadata(report_norm, {})
        crud.dump_metadata(resource_norm, {})

        # if not resource:
        #     resource = crud.create_resource(resource_norm)
        # else:
        #     crud.dump_metadata(resource_norm, {'nid': resource})
        #     crud.update_node(report, report_norm)

        # if not report:
        #     report = crud.create_report(report_norm, resource)
        # else:
        #     crud.dump_metadata(report_norm, {'nid': report, 'pid': resource})
        #     if not crud.is_claimed(resource):
        #         crud.update_node(resource, resource_norm)
