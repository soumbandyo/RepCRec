# Authors:
#
# Soumya Bandyopadhyay
# Sania Lalani


# class to acquire lock exception
class AcquireLockException(Exception):
    def __init__(self, *args):
        self.args = [a for a in args]


# class 'Lock' with read and write values
class Lock:
    def __init__(self):
        pass

    READ = 0  # Read value set as 0
    WRITE = 1  # Write value set as 1


# class 'LockManager' to perform various lock operations
class LockManager:
    # function to set default values
    def __init__(self, site_variables):
        self._site_variables = site_variables
        self._write_lock_table = {}
        self._read_lock_table = {}
        self._init_lock_tables()

    # function to set default values for lock tables
    def _init_lock_tables(self):
        for s in self._site_variables:
            self._write_lock_table[s] = None
            self._read_lock_table[s] = []

    # function to check for write lock
    def txn_has_write_lock(self, transaction, variable):
        return self._lookup_write_lock(variable) == transaction['_id']

    # function to acquire read lock
    def acquire_read_lock(self, transaction, variable):
        if not self.txn_has_read_lock(transaction, variable):
            if self._write_lock_table[variable] != None:
                if self._write_lock_table[variable] != transaction['_id']:
                    raise AcquireLockException(Lock.WRITE, self._write_lock_table[variable])
            self._read_lock_table[variable].append(transaction['_id'])

    # function to look for write lock
    def _lookup_write_lock(self, variable):
        if variable in self._write_lock_table:
            return self._write_lock_table[variable]
        return None

    # function to acquire write lock
    def acquire_write_lock(self, transaction, variable):
        if not self.txn_has_write_lock(transaction, variable):
            if self._write_lock_table[variable] != None:
                if self._write_lock_table[variable] != transaction['_id']:
                    raise AcquireLockException(Lock.WRITE, self._write_lock_table[variable])
            elif self._read_lock_table[variable] != []:
                for txn in self._read_lock_table[variable]:
                    if txn != transaction['_id']:
                        raise AcquireLockException(Lock.READ, self._read_lock_table[variable])
            self._write_lock_table[variable] = transaction['_id']

    # function to look for read lock
    def _lookup_read_lock(self, variable):
        if variable in self._read_lock_table:
            return self._read_lock_table[variable]
        return []

    # function to release all lock
    def release_all_locks(self, transaction=None):
        if not transaction:
            self._init_lock_tables()
        else:
            self._release_write_locks(transaction)
            self._release_read_locks(transaction)

    # function to release read lock
    def _release_read_locks(self, transaction):
        for variable in self._read_lock_table.keys():
            try:
                self._read_lock_table[variable].remove(transaction['_id'])
            except ValueError:
                continue

    # function to release write lock
    def _release_write_locks(self, transaction):
        for variable in self._write_lock_table.keys():
            if self._write_lock_table[variable] == transaction['_id']:
                self._write_lock_table[variable] = None

    # function to check for read lock
    def txn_has_read_lock(self, transaction, variable):
        transactions = self._lookup_read_lock(variable)
        if transactions:
            for t in transactions:
                if t == transaction['_id']:
                    return True
        return False

    def release_read_lock(self, transaction, variable):
        if self._read_lock_table[variable] != []:
            try:
                self._read_lock_table[variable].remove(transaction['_id'])
            except ValueError:
                print 'This Tx %s didn\'t have a read lock on the variable' \
                      % transaction['_id']
        else:
            print 'There is no read lock on variable %s' % variable

    # function to release write lock
    def _release_write_locks(self, transaction):
        for variable in self._write_lock_table.keys():
            if self._write_lock_table[variable] == transaction['_id']:
                self._write_lock_table[variable] = None

    def release_write_lock(self, transaction, variable):
        if self._write_lock_table[variable] != None:
            if self._write_lock_table[variable] == transaction['_id']:
                self._write_lock_table[variable] = None
        else:
            print 'There is no write lock on variable %s' % variable

    # function to release read lock
    def _release_read_locks(self, transaction):
        for variable in self._read_lock_table.keys():
            try:
                self._read_lock_table[variable].remove(transaction['_id'])
            except ValueError:
                continue

    # function to release all lock
    def release_all_locks(self, transaction=None):
        if not transaction:
            self._init_lock_tables()
        else:
            self._release_write_locks(transaction)
            self._release_read_locks(transaction)

