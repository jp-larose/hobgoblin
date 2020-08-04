"""Test model properties."""

import pytest
from gremlin_python.statics import long

from hobgoblin import element, exception, manager
from hobgoblin.properties import datatypes


def test_set_change_property(person, lives_in):
    # vertex
    assert not person.name
    person.name = 'leif'
    assert person.name == 'leif'
    person.name = 'leifur'
    assert person.name == 'leifur'
    # edge
    assert not lives_in.notes
    lives_in.notes = 'notable'
    assert lives_in.notes == 'notable'
    lives_in.notes = 'more notable'
    assert lives_in.notes == 'more notable'


def test_property_default(knows):
    assert knows.notes == 'N/A'
    knows.notes = 'notable'
    assert knows.notes == 'notable'


def test_false_bool_default(place):
    assert place.incorporated.value is False


def test_validation(person):
    person.age = 10
    with pytest.raises(Exception):
        person.age = 'hello'


def test_setattr_validation(person):
    setattr(person, 'age', 10)
    assert person.age == 10
    with pytest.raises(Exception):
        setattr(person, 'age', 'hello')


def test_set_id_long(person):
    person.id = 1
    assert isinstance(person.id, long)


def test_id_class_attr_throws(person_class):
    with pytest.raises(exception.ElementError):
        person_class.id


# Vertex properties
def test_set_change_vertex_property(person):
    assert not person.birthplace
    person.birthplace = 'Iowa City'
    assert isinstance(person.birthplace, element.VertexProperty)
    assert person.birthplace.value == 'Iowa City'
    person.birthplace = 'U of I Hospital'
    assert person.birthplace.value == 'U of I Hospital'


def test_vertex_property_default():
    """Makes sure that a brand new VertexProperty (i.e., with no value set) is
    still representable. Addresses issue #52.
    """
    vp = element.VertexProperty(int)
    assert repr(vp) == "<VertexProperty(type=0, value=None)>"


def test_validate_vertex_prop(person):
    assert not person.birthplace
    person.birthplace = 1
    assert person.birthplace.value == '1'


def test_set_change_list_card_vertex_property(person):
    assert not person.nicknames
    person.nicknames = 'sly'
    assert isinstance(person.nicknames, list)
    assert isinstance(person.nicknames, manager.ListVertexPropertyManager)
    assert isinstance(person.nicknames[0], element.VertexProperty)
    assert person.nicknames[0].value == 'sly'
    assert person.nicknames('sly') == person.nicknames[0]
    person.nicknames = set(['sly', 'guy'])
    assert isinstance(person.nicknames, list)
    assert person.nicknames('sly').value == 'sly'
    assert person.nicknames('guy').value == 'guy'
    person.nicknames = ('sly', 'big', 'guy')
    assert isinstance(person.nicknames, list)
    assert [v.value for v in person.nicknames] == ['sly', 'big', 'guy']
    person.nicknames = ['sly', 'big', 'guy', 'guy']
    assert isinstance(person.nicknames, list)
    assert len(person.nicknames('guy')) == 2
    assert [v.value for v in person.nicknames] == ['sly', 'big', 'guy', 'guy']
    person.nicknames.append(1)
    assert person.nicknames('1').value == '1'


def test_list_card_vertex_property_validation(person):
    person.nicknames = [1, 1.5, 2]
    assert [v.value for v in person.nicknames] == ['1', '1.5', '2']


def test_set_change_set_card_vertex_property(place):
    assert not place.important_numbers
    place.important_numbers = 1
    assert isinstance(place.important_numbers, set)
    assert isinstance(place.important_numbers,
                      manager.SetVertexPropertyManager)
    number_one, = place.important_numbers
    assert isinstance(number_one, element.VertexProperty)
    assert number_one.value == 1
    assert place.important_numbers(1) == number_one
    place.important_numbers = [1, 2]
    assert isinstance(place.important_numbers, set)
    assert {v.value for v in place.important_numbers} == set([1, 2])
    place.important_numbers.add(3)
    assert {v.value for v in place.important_numbers} == set([1, 2, 3])
    place.important_numbers = (1, 2, 3, 4)
    assert isinstance(place.important_numbers, set)
    assert {v.value for v in place.important_numbers} == set([1, 2, 3, 4])
    place.important_numbers = set([1, 2, 3])
    assert isinstance(place.important_numbers, set)
    assert {v.value for v in place.important_numbers} == set([1, 2, 3])
    with pytest.raises(exception.ValidationError):
        place.important_numbers.add('dude')


def test_set_card_union(place):
    place.important_numbers = set([1, 2, 3])
    place.important_numbers = place.important_numbers.union({3, 4, 5})


def test_set_card_64bit_integer(place):
    place.important_numbers = set([long(1), long(2), long(3)])
    assert all(isinstance(i.value, long) for i in place.important_numbers)


def test_set_card_validation_vertex_property(place):
    with pytest.raises(exception.ValidationError):
        place.important_numbers = set(['hello', 2, 3])


def test_cant_set_vertex_prop_on_edge():
    with pytest.raises(exception.MappingError):

        class MyEdge(element.Edge):
            vert_prop = element.VertexProperty(datatypes.String)


def test_meta_property_set_update(place):
    assert not place.historical_name
    place.historical_name = ['hispania', 'al-andalus']
    place.historical_name('hispania').notes = 'roman rule'
    assert place.historical_name('hispania').notes == 'roman rule'
    place.historical_name('hispania').year = 300
    assert place.historical_name('hispania').year == 300
    place.historical_name('al-andalus').notes = 'muslim rule'
    assert place.historical_name('al-andalus').notes == 'muslim rule'
    place.historical_name('al-andalus').year = 700
    assert place.historical_name('al-andalus').year == 700


def test_meta_property_validation(place):
    assert not place.historical_name
    place.historical_name = ['spain']
    with pytest.raises(exception.ValidationError):
        place.historical_name('spain').year = 'hello'


class TestString:
    def test_validation(self, string):
        assert string.validate(1) == '1'

    def test_to_db(self, string):
        assert string.to_db('hello') == 'hello'

    def test_to_ogm(self, string):
        assert string.to_ogm('hello') == 'hello'

    def test_initval_to_db(self, string_class):
        string = string_class('hello')
        assert string.to_db() == 'hello'


class TestInteger:
    def test_validation(self, integer):
        assert integer.validate('1') == 1
        with pytest.raises(Exception):
            integer.validate('hello')

    def test_to_db(self, integer):
        assert integer.to_db(1) == 1

    def test_to_ogm(self, integer):
        assert integer.to_db(1) == 1

    def test_initval_to_db(self, integer_class):
        integer = integer_class(1)
        assert integer.to_db() == 1


class TestFloat:
    def test_validation(self, flt):
        assert flt.validate(1.2) == 1.2
        with pytest.raises(Exception):
            flt.validate('hello')

    def test_to_db(self, flt):
        assert flt.to_db(1.2) == 1.2

    def test_to_ogm(self, flt):
        assert flt.to_db(1.2) == 1.2

    def test_initval_to_db(self, flt_class):
        flt = flt_class(1.2)
        assert flt.to_db() == 1.2


class TestBoolean:
    def test_validation_true(self, boolean):
        assert boolean.validate(True)

    def test_validation_false(self, boolean):
        assert not boolean.validate(False)

    def test_to_db_true(self, boolean):
        assert boolean.to_db(True)

    def test_to_db_false(self, boolean):
        assert not boolean.to_db(False)

    def test_to_ogm_true(self, boolean):
        assert boolean.to_ogm(True)

    def test_to_ogm_false(self, boolean):
        assert not boolean.to_ogm(False)

    def test_initval_to_db_true(self, boolean_class):
        boolean = boolean_class(True)
        assert boolean.to_db()

    def test_initval_to_db_true(self, boolean_class):
        boolean = boolean_class(False)
        assert not boolean.to_db()
