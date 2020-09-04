import pytest

from hobgoblin import exception, properties
from hobgoblin.properties import datatypes


def test_property_mapping(person_class, lives_in_class):

    db_name, data_type = person_class.mapping.ogm_properties['name']
    assert db_name == 'name'
    assert isinstance(data_type, datatypes.String)
    db_name, data_type = person_class.mapping.ogm_properties['age']
    assert db_name == 'custom__person__age'
    assert isinstance(data_type, datatypes.Integer)
    db_name, data_type = lives_in_class.mapping.ogm_properties['notes']
    assert db_name == 'notes'
    assert isinstance(data_type, datatypes.String)

    ogm_name, data_type = person_class.mapping.db_properties['name']
    assert ogm_name == 'name'
    assert isinstance(data_type, datatypes.String)
    ogm_name, data_type = person_class.mapping.db_properties[
        'custom__person__age']
    assert ogm_name == 'age'
    assert isinstance(data_type, datatypes.Integer)
    ogm_name, data_type = lives_in_class.mapping.db_properties['notes']
    assert ogm_name == 'notes'
    assert isinstance(data_type, datatypes.String)


def test_metaprop_mapping(place):
    place.historical_name = ['Iowa City']
    db_name, data_type = place.historical_name(
        'Iowa City').mapping.ogm_properties['notes']
    assert db_name == 'notes'
    assert isinstance(data_type, datatypes.String)
    db_name, data_type = place.historical_name(
        'Iowa City').mapping.ogm_properties['year']
    assert db_name == 'year'
    assert isinstance(data_type, datatypes.Integer)


def test_label_creation(place, lives_in):
    assert place.mapping.label == 'place'
    assert lives_in.mapping.label == 'lives_in'


def test_mapper_func(place, knows):
    assert callable(place.mapping._mapper_func)
    assert callable(knows.mapping._mapper_func)


def test_getattr_getdbname(person, lives_in):
    db_name = person.mapping.name
    assert db_name == 'name'
    db_name = person.mapping.age
    assert db_name == 'custom__person__age'
    db_name = lives_in.mapping.notes
    assert db_name == 'notes'


def test_getattr_doesnt_exist(person):
    with pytest.raises(exception.MappingError):
        db_name = person.mapping.doesnt_exist


def test_db_name_factory(person, place):
    assert person.mapping.nicknames == 'person__nicknames'
    assert place.mapping.zipcode == 'place__zipcode'
