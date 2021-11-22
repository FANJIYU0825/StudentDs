"""Repositories for the DDB tables.

Work in progress, but the idea needs to happen.
"""
from . import LOGGER, ddb_util


#
# Base Class


class Repository(object):
    """Base Repository class.

    Implements all required functionality automatically. Child classes are meant
    to extend this class with other functions - e.g. querying on other indices,
    or implementing custom queries.
    """

    def __init__(self, table_name, ddb, logger=LOGGER):
        self.table_name = table_name
        ppk, psk = ddb_util.DDB_KEYS[table_name]
        self.ppk_name, self.ppk_type = ppk
        self.psk_name, self.psk_type = psk
        self.ddb = ddb
        self.logger = logger

    def delete(self, items, index_name=None):
        self.ddb.delete(
            items=items,
            table_name=self.table_name,
            index=index_name)

    def query(self, ppk_value, projection=None, drop_missing_attrs=False,
              override_capacity=None):
        # NOTE: create new methods for alternative indexes
        return self.ddb.query(
            table_name=self.table_name,
            condition='#ppk = :ppk',  # forcing this handles reserved
            attr_names={'#ppk': self.ppk_name},
            attr_values={':ppk': {self.ppk_type: ppk_value}},
            projection=projection,
            drop_missing_attrs=drop_missing_attrs,
            capacity=override_capacity)

    def query_one(self, ppk_value, psk_value=None, projection=None,
                  drop_missing_attrs=False, error_not_found=False):
        if self.psk_name is not None:
            condition = '#ppk = :ppk and #psk = :psk'
            attr_names = {'#ppk': self.ppk_name, '#psk': self.psk_name}
            attr_values = {':ppk': {self.ppk_type: ppk_value},
                           ':psk': {self.psk_type: psk_value}}
        else:
            condition = '#ppk = :ppk'
            attr_names = {'#ppk': self.ppk_name}
            attr_values = {':ppk': {self.ppk_type: ppk_value}}
        return self.ddb.query_one(
            table_name=self.table_name,
            projection=projection,
            condition=condition,
            attr_names=attr_names,
            attr_values=attr_values,
            drop_missing_attrs=drop_missing_attrs,
            error_not_found=error_not_found)

    def scan(self, projection=None, _filter=None, attr_names=None,
             attr_values=None, max_items=None, drop_missing_attrs=True,
             override_capacity=None):
        return self.ddb.scan(
            table_name=self.table_name,
            projection=projection,
            _filter=_filter,
            attr_names=attr_names,
            attr_values=attr_values,
            max_items=max_items,
            drop_missing_attrs=drop_missing_attrs,
            capacity=override_capacity)

    def update(self, items, attribute, index_name=None):
        self.ddb.update(
            items=items,
            table_name=self.table_name,
            attribute=attribute,
            index=index_name)

    def upsert(self, items, index_name=None):
        self.ddb.upsert(
            items=items,
            table_name=self.table_name,
            index=index_name)


#
# Derived Classes


class ExperienceRepository(Repository):

    def __init__(self, ddb, logger=LOGGER):
        super(ExperienceRepository, self).__init__(
            table_name='experience', ddb=ddb, logger=logger)

    def query_usr_date(self):
        pass

    def query_usr_cls_loc(self):
        pass

    def recent(self, student_pin, days_to_take=7):
        pass


class StudentRepository(Repository):

    def __init__(self, ddb, logger=LOGGER):
        super(StudentRepository, self).__init__(
            table_name='students', ddb=ddb, logger=logger)
