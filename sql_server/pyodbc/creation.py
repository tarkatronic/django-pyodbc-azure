try:
    from django.db.backends.base.creation import BaseDatabaseCreation
except ImportError:
    from django.db.backends.creation import BaseDatabaseCreation
from django.db.backends.util import truncate_name
from django.utils.six import b


class DatabaseCreation(BaseDatabaseCreation):

    def __init__(self, connection):
        # For Django versions < 1.8
        self.data_types = connection.data_types
        self.data_types_suffix = connection.data_types_suffix
        self.data_type_check_constraints = connection.data_type_check_constraints
        super(DatabaseCreation, self).__init__(connection)

    def _create_test_db(self, verbosity, autoclobber):
        settings_dict = self.connection.settings_dict
        if not settings_dict['TEST'].get('CREATE_DB', True):
            # use the existing database instead of creating a new one
            if verbosity >= 1:
                print("Dropping tables ... ")
            self.connection.close()
            test_db_name = self._get_test_db_name()
            settings_dict["NAME"] = test_db_name
            cursor = self.connection.cursor()
            qn = self.connection.ops.quote_name
            sql = "SELECT TABLE_NAME, CONSTRAINT_NAME " \
                  "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS " \
                  "WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'"
            for row in cursor.execute(sql).fetchall():
                objs = (qn(row[0]), qn(row[1]))
                cursor.execute("ALTER TABLE %s DROP CONSTRAINT %s" % objs)
            for table in self.connection.introspection.get_table_list(cursor):
                if verbosity >= 1:
                    print("Dropping table %s" % table)
                cursor.execute('DROP TABLE %s' % qn(table))
            self.connection.connection.commit()
            return test_db_name

        return super(DatabaseCreation, self)._create_test_db(verbosity, autoclobber)

    def _destroy_test_db(self, test_database_name, verbosity):
        "Internal implementation - remove the test db tables."
        settings_dict = self.connection.settings_dict
        if settings_dict['TEST'].get('CREATE_DB', True):
            settings_dict['NAME'] = None
            self.connection.set_autocommit(True)
            #time.sleep(1) # To avoid "database is being accessed by other users" errors.
            to_azure_sql_db = self.connection.to_azure_sql_db
            cursor = self.connection.cursor()
            if not to_azure_sql_db:
                cursor.execute("ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE " % \
                        self.connection.ops.quote_name(test_database_name))
            cursor.execute("DROP DATABASE %s" % \
                    self.connection.ops.quote_name(test_database_name))
        else:
            if verbosity >= 1:
                test_db_repr = ''
                if verbosity >= 2:
                    test_db_repr = " ('%s')" % test_database_name
                print("The database is left undestroyed%s." % test_db_repr)

        self.connection.close()

    def sql_table_creation_suffix(self):
        suffix = []
        collation = self.connection.settings_dict['TEST'].get('COLLATION', None)
        if collation:
            suffix.append('COLLATE %s' % collation)
        return ' '.join(suffix)

    def use_legacy_datetime(self):
        for field in ('DateField', 'DateTimeField', 'TimeField'):
            self.data_types[field] = 'datetime'
