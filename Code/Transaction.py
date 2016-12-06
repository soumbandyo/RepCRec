# Authors:
#
# Soumya Bandyopadhyay
# Sania Lalani


# class 'TransactionStatus' with active, aborted and waiting values
class TransactionStatus:
    def __init__(self):
        pass

    ACTIVE = 0  # Active value set as 0
    ABORTED = 1  # Aborted value set as 1
    WAITING = 2  # Waiting value set as 2


# class 'TransactionType' with read only and read write values
class TransactionType:
    def __init__(self):
        pass

    READ_ONLY = 0  # Read Only value set as 0
    READ_WRITE = 1  # Read Write value set as 1

# class 'Transaction' to perform various transaction operations
class Transaction:
    # function to set default values
    def __init__(self, txn_id, ts, txn_type=TransactionType.READ_WRITE,
                 txn_status=TransactionStatus.ACTIVE):
        self._id = txn_id  # ID
        self._ts = ts  # Timestamp
        self._status = txn_status  # Status
        self._type = txn_type  # Type

    # function to set status 'waiting'
    def wait(self):
        if self._status != TransactionStatus.WAITING:
            self._status = TransactionStatus.WAITING

    # function to check status 'aborted'
    def is_aborted(self):
        return self._status == TransactionStatus.ABORTED

    # function to get transaction id
    def get_id(self):
        return self._id

    # function to set status 'aborted'
    def abort(self):
        if self._status != TransactionStatus.ABORTED:
            self._status = TransactionStatus.ABORTED

    # function to get timestamp
    def get_ts(self):
        return self._ts

    # function to check status 'active'
    def is_active(self):
        return self._status == TransactionStatus.ACTIVE

    # function to check status 'waiting'
    def is_waiting(self):
        return self._status == TransactionStatus.WAITING

    # function to get status
    def get_status(self):
        return self._status

    # function to set status 'active'
    def activate(self):
        if self._status != TransactionStatus.ACTIVE:
            self._status = TransactionStatus.ACTIVE

    # function to get type
    def get_type(self):
        return self._type



