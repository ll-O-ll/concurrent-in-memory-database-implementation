#!/usr/bin/python3
#
# exception.py
#
# Definition for all the errors in EasyDB
#
 
# customized exception for a cycle caused by foreign keys
class IntegrityError(Exception):
    pass


# customized exception for foreign key not found in the database
class InvalidReference(Exception):
    pass


# customized exception for the error code NOT_FOUND (id not found)
class ObjectDoesNotExist(Exception):
    pass


# customized exception for the error code TXN_ABORT (update aborted)
class TransactionAbort(Exception):
    pass


# customized exception for the error code BAD_TABLE, BAD_QUERY, BAD_VALUE, BAD_ROW, and BAD_REQUEST.
class PacketError(Exception):
    pass
