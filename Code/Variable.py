# Authors:
#
# Soumya Bandyopadhyay
# Sania Lalani

from Transaction import TransactionType


# class 'VariableStatus' with active and recovering values
class VariableStatus:
    def __init__(self):
        pass

    ACTIVE = 0  # Active value set as 0
    RECOVERING = 1  # Recovering value set as 1

# class 'Variable' to perform various operations
class Variable:
    # function to set default values
    def __init__(self, var_id, value):
        self._id = var_id
        self._committed_values = [(0, value)]
        self._uncommitted_value = None
        self._status = VariableStatus.ACTIVE

    # function to read committed value from a variable
    def read_committed(self, transaction=None):
        if transaction and transaction['_type'] == TransactionType.READ_ONLY:
            for tick, val in self._committed_values:
                if tick <= transaction['_ts']:
                    retval = val
                else:
                    break
            return retval
        else:
            return self._committed_values[-1][1]

    # function to write in a variable
    def write(self, transaction, value):
        self._uncommitted_value = (transaction['_id'], value)

    # function to check variable status 'RECOVERING'
    def is_recovering(self):
        return self._status == VariableStatus.RECOVERING

    # function to read uncommitted value from a variable
    def read_uncommitted(self, transaction):
        if self._uncommitted_value[0] == transaction['_id']:
            return self._uncommitted_value[1]

        # Transaction with write lock, but hasn't written
        return self.read_committed(transaction)

    # function to load uncommited value
    def load_uncommitted(self, value):
        self._uncommitted_value = value

    # function to dump commited value
    def dump_committed(self):
        return self._committed_values

    # function to set variable status
    def recover(self):
        self._status = VariableStatus.RECOVERING

    # function to check if replicated
    def is_replicated(self):
        var_index = int(self._id[1:])
        return (var_index % 2) == 0

    # function to commit value in a variable
    def commit(self, timestamp):
        if self.is_recovering():
            self._status = VariableStatus.ACTIVE
        self._committed_values.append((timestamp, self._uncommitted_value))

    # function to load commited value
    def load_committed(self, values):
        self._committed_values = values

    # function to commit value in a variable
    def commit(self, timestamp):
        if self.is_recovering():
            self._status = VariableStatus.ACTIVE
        self._committed_values.append((timestamp, self._uncommitted_value))

    # function to dump uncommited value
    def dump_uncommitted(self):
        return self._uncommitted_value

    # function to load state
    def load_state(self, committed_values, uncommitted_values):
        self.load_committed(committed_values)
        self.load_uncommitted(uncommitted_values)

