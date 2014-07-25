import consume


return_list = consume.consume()


def test_return_list_exists():

    assert isinstance(return_list, list)

def test_return_list_contains_tupels():

    for item in return_list:
        assert isinstance(item, tuple)
