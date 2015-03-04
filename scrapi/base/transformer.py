import logging

from lxml import etree

XML_to_JSON = 'xml_to_json'

supported_transformations = [XML_to_JSON]

logger = logging.getLogger(__name__)


class Transformer(object):

    def __init__(self, _type):
        self.transformations = {}
        if _type in supported_transformations:
            self._type = _type
        else:
            raise TransformationUnsupportedError("Transformation {} unsupported".format(_type))

    def transform(self, doc):
        if self._type == XML_to_JSON:
            doc = etree.XML(doc)
            return {
                key: transformation(doc) for key, transformation in self.transformations.items()
            }

    def register_transformation(self, source, target, namespaces={}, fun=lambda x: x):
        if self._type == XML_to_JSON:
            self.transformations[target] = lambda doc: fun(doc.xpath(source, namespaces=namespaces))
        else:
            raise TransformationUnsupportedError

    def register_transformations(self, transformations):
        for transformation in transformations:
            self.register_transformation(
                transformation[0],
                transformation[1],
                namespaces=transformation[2],
                fun=transformation[3] if len(transformations) == 4 else lambda x: x
            )


class TransformationUnsupportedError(Exception):
    pass

# register_transformation(
#     '<dc:title>',
#     'title',
#     namespaces={'dc': 'htt[ asfdf'}
#     fun=to_lowercase
# )
