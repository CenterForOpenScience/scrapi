class BaseProcessor(object):
    NAME = None

    def process_raw(self, raw_doc, **kwargs):
        pass  # pragma: no cover

    def process_normalized(self, raw_doc, normalized, **kwargs):
        pass  # pragma: no cover
