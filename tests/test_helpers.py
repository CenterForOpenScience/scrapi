from scrapi.base import helpers


class TestHelpers(object):

    def test_format_one_tag(self):
        single_tag = ' A single tag '
        single_output = helpers.format_tags(single_tag)
        assert single_output == ['a single tag']
        assert isinstance(single_output, list)

    def test_format_many_tags(self):
        many_tags = [' A', 'Bunch', ' oftags ']
        many_output = helpers.format_tags(many_tags)
        assert set(many_output) == set(['a', 'bunch', 'oftags'])

    def test_format_sep_tags(self):
        sep_tags = ['These, we know', 'should be many']
        sep_output = helpers.format_tags(sep_tags, sep=',')
        assert set(sep_output) == set(['these', 'we know', 'should be many'])

    def test_extract_dois(self):
        identifiers = 'doi: THIS_IS_A_DOI!'
        valid_doi = helpers.oai_extract_dois(identifiers)
        assert valid_doi == 'THIS_IS_A_DOI!'

    def test_oai_extract_url(self):
        identifiers = 'I might be a url but rly I am naaaahhttt'
        extraction_attempt = helpers.oai_extract_url(identifiers)
        extraction_attempt

    def test_process_contributors(self):
        args = ['Stardust Rhodes', 'Golddust Rhodes', 'Dusty Rhodes']
        response = helpers.oai_process_contributors(args)
        assert isinstance(response, list)
