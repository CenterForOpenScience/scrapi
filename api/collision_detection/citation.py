"""
Script to be ran against raw database and create groups of potential
conflicting source articles.
Author: Joshua Carp
Date: 2013
Modified by: Fabian von Feilitzsch
Date: 2014
Repurposed for citation collision detection
"""

# Imports
import glob
import json
from fuzzywuzzy import fuzz


def mean(values):
    """returns mean of a list of values

    :param values: list of values to be evaluated
    :returns: float: mean of values
    """
    if not values:
        return 0
    return float(sum(values)) / len(values)


def read_fixtures(fixture_dir):
    """ returns list of features within a given fixture_dir

    :param fixture_dir: string of directory Name
    :return: list: list of fixtures within specified directory
    """
    # Initialize fixtures
    fixtures = []

    fixture_names = glob.glob('%s/*.json' % (fixture_dir))

    for fixture_name in fixture_names:
        with open(fixture_name) as fp:
            fixture = json.load(fp)
            fixtures.append(fixture)

    return fixtures

unique_fields = [
    'DOI',
    'URL',
    'PMID',
    'PMCID',
]

# Identity function
I = lambda i: i


def match_factory(field, fun, access_fun=I):
    """Create a fuzzy matching function for a given field.

    :param field: Name of field in record dictionary
    :param fun: Matching function to apply to field values
            (must take two arguments)
    :param access_fun: Access function for fields in record; defaults to
            identity function
    :return: function: Matching function that takes two records
    """
    def match(record0, record1):

        value0 = access_fun(record0.get(field, None))
        value1 = access_fun(record1.get(field, None))
        if value0 and value1:
            return fun(value0, value1)

    return match


def access_author(author):
    """ Access function for author field. """
    try:
        return author[0]['family']
    except IndexError:
        pass
    except KeyError:
        pass


def access_issued(issued):
    """ Access function for issued field. """
    try:
        return issued['date-parts'][0][0]
    except IndexError:
        pass
    except KeyError:
        pass

fuzzy_rules = [
    match_factory('title', fuzz.token_set_ratio),
    match_factory('container-title', fuzz.token_set_ratio),
    match_factory('author', fuzz.token_set_ratio, access_author),
    match_factory('issued', fuzz.token_set_ratio, access_issued),
]


def unique_group_compare(group0, group1, unique_fields):
    """Compare two groups of records according to unique fields
    (e.g., DOI, URL). If any unique fields are shared among groups,
    they match.

    :param group0: First group of records
    :param group1: Second group of records
    :param unique_fields: Keys to unique fields
    :return: boolean: Are any unique fields shared?
    """
    # Iterate over fields
    for field in unique_fields:

        # Get values for each group
        values0 = [record[field] for record in group0 if field in record]
        values1 = [record[field] for record in group1 if field in record]

        # Match if intersection is not empty
        if set(values0).intersection(values1):
            return True

    # No match
    return False


def fuzzy_group_compare(group0, group1, fuzzy_rules):
    """Compare two groups of records using fuzzy matching. If any
    pairs of records are similar enough across groups, they match.

    :param group0: First group of records
    :param group1: Second group of records
    :param fuzzy_rules: List of matching rules
    :return: boolean: Do any rules match?
    """
    # Iterate over first group
    for record0 in group0:

        # Iterate over second group
        for record1 in group1:
            # Initialize scores
            fuzzy_scores = []

            # Get score for each rule
            for fuzzy_rule in fuzzy_rules:
                # Get score
                fuzzy_score = fuzzy_rule(record0, record1)

                # Append score if not None
                if fuzzy_score is not None:
                    fuzzy_scores.append(fuzzy_score)

            # Match if mean is above threshold
            if mean(fuzzy_scores) > 75:
                return True

    # No match
    return False


def detect(records):
    """Detect groups of conflicted records.

    :param records: list of CSL-formatted records
    :return: list of lists of conflicted records
    """
    # Quit if no records provided
    if not records:
        return []

    # Set first group to array containing first record
    groups = [[records[0]]]

    # Iterate over remaining records
    for record in records[1:]:
        # Initialize grouped to False
        grouped = False
        # Iterate over groups
        for group in groups:
            # If record matches group, append
            if unique_group_compare(group, [record], unique_fields) or \
                    fuzzy_group_compare(group, [record], fuzzy_rules):
                # Append record to matching group
                group.append(record)
                grouped = True
                break
        # If record matched no groups, create new group
        if not grouped:
            groups.append([record])

    # Return list of non-singleton groups
    return [group for group in groups if len(group) > 1]
