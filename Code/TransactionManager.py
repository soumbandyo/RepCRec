# Authors:
#
# Soumya Bandyopadhyay
# Sania Lalani

from Transaction import Transaction
from Transaction import TransactionType

import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer

MAX_SITES = 10
MAX_VARS = 20
PORT = 7777
SITE_PORTS = [9090, 9091, 9092, 9093, 9094, 9095, 9096, 9097, 9098, 9099]


# class 'TransactionManager' to perform various transaction operations
class TransactionManager:
    # function to set default values
    def __init__(self):
        self._sites = {}
        self._init_sites()
        self._ts = 0
        self._txn_sites = {}
        self._transactions = {}
        self._wait_list = []
        self._list = []
        self._init_server(PORT)

    # function to initialize server
    def _init_server(self, port=PORT):
        self._server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self._server.register_function(self.inc_ts)
        self._server.register_function(self.begin)
        self._server.register_function(self.beginRO)
        self._server.register_function(self.write)
        self._server.register_function(self.read)
        self._server.register_function(self.end)
        self._server.register_function(self.dump_all)
        self._server.register_function(self.dump_var)
        self._server.register_function(self.dump_site)
        self._server.register_function(self.dump_site_var)
        self._server.register_function(self.fail)
        self._server.register_function(self.recover)
        self._server.serve_forever()

    # handles a deadlock situation. Detects a deadlock and then aborts the transaction with a higher timestamp
    def detect_deadlock(self, command_tuple, lock_type, conflicting_txns):
        txn_id = command_tuple[1]
        transaction = self._transactions[txn_id]
        if isinstance(conflicting_txns, str):
            conflicting_txn = self._transactions[conflicting_txns]
            check = self._add_list((conflicting_txn.get_id(), transaction.get_id()))
            if check:
                if transaction.get_ts() > conflicting_txn.get_ts():
                    return self._abort(transaction)
                else:
                    return self._abort(conflicting_txn)
            else:
                self._add_to_waitlist(command_tuple)
                return 'Waitlisted Tx %s at time_stamp %d' % (txn_id, self._ts)
        elif isinstance(conflicting_txns, list):
            for conflicting_txn_id in conflicting_txns:
                conflicting_txn = self._transactions[conflicting_txn_id]
                check = self._add_list((conflicting_txn.get_id(), transaction.get_id()))
                if check:
                    if transaction.get_ts() > conflicting_txn.get_ts():
                        return self._abort(transaction)
                    else:
                        return self._abort(conflicting_txn)
                else:
                    self._add_to_waitlist(command_tuple)
                    return 'Waitlisted transaction %s at time_stamp %d' % (txn_id, self._ts)

    # checks for a cycle in the wait for graph  and returns true or false accordingly
    def _add_list(self, list_tuple):
        self._list.append(list_tuple)
        if len(self._list) == 1:
            return False
        l_len = len(self._list)
        for i in range(0, l_len, 1):
            firstItem = self._list[i]
            for j in range(i + 1, l_len, 1):
                secondItem = self._list[j]
                if firstItem[0] == secondItem[1] and secondItem[0] == firstItem[1]:
                    return True
        return False

    # function to assign timestamp
    def inc_ts(self):
        self._ts += 1

    # function to initialize sites
    def _init_sites(self):
        i = 1
        for port in SITE_PORTS:
            site_client = xmlrpclib.ServerProxy('http://localhost:%d' % port,
                                                allow_none=True)
            self._sites[i] = site_client
            i += 1

    # transaction begins
    def begin(self, txn_id):
        if txn_id not in self._transactions:
            self._transactions[txn_id] = Transaction(txn_id, self._ts)
            self._txn_sites[txn_id] = []
            return 'Began Tx %s with time_stamp %d' % \
                   (txn_id, self._transactions[txn_id].get_ts())
        else:
            raise Exception('Tx with ID %s already exists' % txn_id)

    # index variables to respective sites
    def _get_sites_for_var(self, var_id):
        if var_id > MAX_VARS:
            raise Exception('Unknown variable')
        elif (var_id % 2) == 0:
            return self._sites.keys()
        else:
            return [1 + (var_id % 10)]

    # transaction writes if it obtains lock
    def write(self, txn_id, var, value):
        transaction = self._transactions[txn_id]
        if not transaction.is_aborted():
            var_id = int(var[1:])
            site_ids = self._get_sites_for_var(var_id)
            succeeded_writes = 0
            for s in site_ids:
                if self._sites[s].is_up():
                    self._add_to_txn_sites(transaction, s)
                    retval = self._sites[s].write(transaction, var, int(value))
                    if retval['status'] == 'exception':
                        args = retval['args']
                        return self.detect_deadlock(('write', txn_id, var, value), args[0], args[1])
                    elif retval['status'] == 'success':
                        succeeded_writes += 1
            if succeeded_writes > 0:
                if transaction.is_waiting():
                    transaction.activate()
                return 'Wrote var %s for txn %s at time_stamp %d' % \
                       (var, txn_id, self._ts)
            else:
                # Unable to find site to read from
                self._add_to_waitlist(('write', txn_id, var, value))
                return 'Unable to write %s, no site available' % var
        else:
            return 'Tx %s is in aborted state' % txn_id

    # write to all the copies
    def _add_to_txn_sites(self, transaction, site_id):
        if transaction.get_type() == TransactionType.READ_WRITE:
            txn_id = transaction.get_id()
            if not site_id in self._txn_sites[txn_id]:
                self._txn_sites[txn_id].append(site_id)

    # retries all the transaction waiting to gain locks
    def _retry_waiting_txns(self):
        retstr = ''
        i = 0
        while i < len(self._wait_list):
            operation = self._wait_list[i]
            txn_id = operation[1]
            transaction = self._transactions[txn_id]
            if operation[0] == 'write':
                var, value = operation[2:]
                retstr += '\n' + self.write(txn_id, var, value)
            elif operation[0] == 'read':
                var = operation[2]
                retstr += '\n' + self.read(txn_id, var)
            if not transaction.is_waiting():
                self._wait_list.remove(operation)
            else:
                i += 1
        return retstr

    # read only transition begins
    def beginRO(self, txn_id):
        if txn_id not in self._transactions:
            self._transactions[txn_id] = Transaction(txn_id, self._ts,
                                                     txn_type=TransactionType.READ_ONLY)
            return 'Began read-only Tx %s with time_stamp %d' % \
                   (txn_id, self._transactions[txn_id].get_ts())
        else:
            raise Exception('Tx with ID %s already exists' % txn_id)

    # transaction reads
    def read(self, txn_id, var):
        transaction = self._transactions[txn_id]
        if not transaction.is_aborted():
            var_id = int(var[1:])
            site_ids = self._get_sites_for_var(var_id)
            for s in site_ids:
                if self._sites[s].is_up():
                    self._add_to_txn_sites(transaction, s)
                    retval = self._sites[s].read(transaction, var)
                    if retval:
                        if retval['status'] == 'success':
                            if transaction.is_waiting():
                                transaction.activate()
                            return 'Read var %s for Tx %s at time_stamp %d, value: %s' \
                                   % (var, txn_id, self._ts, repr(retval['data']))
                        elif retval['status'] == 'exception':
                            args = retval['args']
                            return self.detect_deadlock(('read', txn_id, var), args[0], args[1])
            # If we reach here, we weren't able to find a site to read from
            self._add_to_waitlist(('read', txn_id, var))
            return 'Unable to read %s, no site available' % var
        else:
            return 'Tx %s is in aborted state' % txn_id

    # adds the the waitlisted transaction to a wait list
    def _add_to_waitlist(self, command_tuple):
        txn_id = command_tuple[1]
        transaction = self._transactions[txn_id]
        if not transaction.is_waiting():
            print'Waitlisted Tx %s at time_stamp %d' % (txn_id, self._ts)
            self._wait_list.append(command_tuple)
            transaction.wait()

    # aborts the transaction accessing data from a site in case of a site failure
    def _abort_site_txns(self, site_id):
        retstr = ''
        for txn_id, txn_sites in self._txn_sites.iteritems():
            if site_id in txn_sites:
                transaction = self._transactions[txn_id]
                if not transaction.is_aborted():
                    retstr += self._abort(transaction)
                continue
        return retstr

    # aborts the transaction
    def _abort(self, transaction):
        txn_id = transaction.get_id()
        if txn_id in self._txn_sites:
            for s in self._txn_sites[transaction.get_id()]:
                self._sites[s].abort(transaction)
                transaction.abort()
            retstr = 'Aborted Tx %s at time_stamp %d' % (txn_id, self._ts)
            retstr += '\n' + self._retry_waiting_txns()
            return retstr
        else:
            return 'Tx %s not found on transaction manager' % txn_id

    # gives the committed values of all copies of all variables at all sites, sorted per site
    def dump_all(self):
        site_var = {}
        for s, obj in self._sites.iteritems():
            site_var[str(s)] = obj.dump()
        return site_var

    # gives the committed values of all copies of all variables at a site
    def dump_site(self, site_id):
        return self._sites[site_id].dump()

    # gives the committed values of all copies of a variable at all sites
    def dump_var(self, var_id):
        var_index = int(var_id[1:])
        sites = self._get_sites_for_var(var_index)
        sites_var = {}
        for s in sites:
            sites_var[str(s)] = self._sites[s].dump(var_id)
        return sites_var

    # gives the committed values of all copies of a variable at a site
    def dump_site_var(self, site_id, var_id):
        return self._sites[site_id].dump(var_id)

    # handles site recovery
    def recover(self, site_id):
        self._sites[site_id].recover()
        retstr = self._retry_waiting_txns()
        if retstr:
            return 'Site %s recovered at time_stamp %d\n' % (site_id, self._ts) + retstr
        else:
            return 'Site %s recovered at time_stamp %d' % (site_id, self._ts)

    # handles site failure
    def fail(self, site_id):
        if site_id in self._sites:
            self._sites[site_id].fail()
            retstr = self._abort_site_txns(site_id)
            if retstr:
                return 'Site %s failed at time_stamp %d\n' % (site_id, self._ts) + retstr
            else:
                return 'Site %s failed at time_stamp %d\n' % (site_id, self._ts)
        else:
            return 'Unknown site %s' % site_id

    # transaction ends
    def end(self, txn_id):
        transaction = self._transactions[txn_id]
        if not transaction.is_aborted():
            if transaction.get_type() == TransactionType.READ_WRITE:
                for s in self._txn_sites[txn_id]:
                    if self._sites[s].is_up():
                        self._sites[s].commit(transaction, self._ts)
                    else:
                        retstr = self._abort(transaction)
                        if retstr:
                            return 'One of the site accessed by Tx failed, aborting\n' + retstr
                        else:
                            return 'One of the site accessed by Tx failed, aborting'
                retstr = self._retry_waiting_txns()
                if retstr:
                    return 'Ended Tx %s at time_stamp %d\n' % (txn_id, self._ts) + retstr
                else:
                    return 'Ended Tx %s at time_stamp %d' % (txn_id, self._ts)
            else:
                return 'Ended txn %s at time_stamp %d' % (txn_id, self._ts)
        else:
            return 'Tx %s is in aborted state' % txn_id


if __name__ == '__main__':
    txn_mgr = TransactionManager()
