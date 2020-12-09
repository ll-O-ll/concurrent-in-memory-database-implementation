#!/usr/bin/python3
#
# easydb.py
#
# Definition for the Database class in EasyDB client
#

import math
import socket
import struct

from orm.easydb import packet
from orm.easydb import exception


def is_valid_name(name, is_column_name):
    # Ensure that the name is a string
    if type(name) != str:
        raise TypeError("Illegal name '" + str(name) + "': not a string")

    # Ensure that the first character of the name is an alphabetical letter
    if len(name) == 0 or not name[0].isalpha():
        raise ValueError("Illegal name '" + str(name) + "': begins with a non-alphabetic character")

    # Underscores are allowed - we remove them from the local copy of the column name so that isalnum() doesn't
    # encounter them when parsing the string (so that it only checks that the rest of the characters are alphanumeric)
    name = name.replace('_', '')

    # Ensure that all other characters in the name are alphanumeric characters
    # (we include the first letter in the search since it has already been confirmed to be alphabetic)
    if not name.isalnum():
        raise ValueError("Illegal name '" + str(name) + "': contains non-alphanumeric characters")

    # Ensure that the name isn't a reserved word
    if is_column_name and name == "id":
        raise ValueError("Reserved word 'id' cannot be used as a column name")

    # No errors, so the name is valid!
    return True


def verify_tables(tables):
    # List of the names of all tables that have been approved (this list is populated by the loop below)
    verified_tables = []

    # Verify each individual table in the 'tables' tuple
    # Note that a TypeError will be raised if 'tables' is not iterable
    for table in tables:
        (table_name, table_schema) = (table[0], table[1])

        if not is_valid_name(table_name, False):
            # This will never occur, since is_valid_name will raise an exception upon encountering
            # an illegal name. This exception will propagate up to the caller since it's not handled here
            return False

        # If a table with the same name has already been verified, we have a duplicate (which isn't allowed)
        if table_name in verified_tables:
            raise ValueError("Duplicate definition of table " + table_name)

        # List of all columns in the current table that have been approved (this list is populated by the loop below)
        verified_table_columns = []

        # Verify that each entry inside the table_schema is valid (i.e. specifies a valid name and type for the column)
        # Note that a TypeError will be raised if 'table_schema' is not iterable
        for table_column in table_schema:
            (table_column_name, table_column_type) = (table_column[0], table_column[1])

            if not is_valid_name(table_column_name, True):
                # This will never occur, since is_valid_name will raise an exception upon encountering
                # an illegal name. This exception will propagate up to the caller since it's not handled here
                return False

            # If a column with the same name has already been verified, we have a duplicate (which isn't allowed)
            if table_column_name in verified_table_columns:
                raise ValueError("Duplicate column " + table_column_name + " in table " + table_name)

            if type(table_column_type) == type:
                if table_column_type not in (str, float, int):
                    raise ValueError("Unsupported type " + str(table_column_type) + "for column " + table_column_name)
            elif type(table_column_type) == str:
                # We require all foreign key references to be already defined (this is the simplification specified in
                # the assignment specification to ensure there are no cycles in foreign key references)
                if table_column_type not in verified_tables:
                    raise exception.IntegrityError(table_column_type + " references a table not yet defined")
            else:
                raise ValueError(str(table_column_type) + " is not a valid type or foreign key reference")

            # If there were any errors in the column, they would've been caught above and the appropriate exception
            # would've been raised
            # Reaching this point of the code means the column has been verified, so add it to 'verified_table_columns'
            verified_table_columns.append(table_column_name)

        # If there were any errors in the table, they would've been caught above and the appropriate exception
        # would've been raised
        # Reaching this point of the code means the table has been verified, so add it to 'verified_tables'
        verified_tables.append(table_name)

    # If all tables have been approved, return true
    # Note that if any error was found, an exception will have been thrown so it is guaranteed that
    # all tables are valid if the code reaches this point
    return True


def get_table_columns(table_schema):
    # maps the column name to the column type
    columns = {}

    # We must include the implicit ID field in this mapping (its column number is 0)
    columns["id"] = "self"

    for col_name, col_type in table_schema:
        columns[col_name] = col_type

    return columns


class Database:
    def __repr__(self):
        return "<EasyDB Database object>"

    def __init__(self, tables):
        if verify_tables(tables):
            self.tables = tables
        self.db_socket = None
        pass

    def connect(self, host, port):
        self.db_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.db_socket.connect((host, port))

        db_server_response = self.db_socket.recv(4096)
        (db_server_response_code,) = struct.unpack("!i", db_server_response)

        if db_server_response_code == packet.SERVER_BUSY:
            print("Database.connect(): Server replied with packet.SERVER_BUSY")
            self.db_socket.close()
            return False
        elif db_server_response_code != packet.OK:
            self.db_socket.close()
            raise Exception("Database.connect(): Server replied with " + str(db_server_response_code))

        return True

    def close(self):
        exit_request = (packet.EXIT, 1)
        exit_command_sent = self.db_socket.sendall(struct.pack("!2i", *exit_request)) is None
        self.db_socket.shutdown(socket.SHUT_RDWR)
        self.db_socket.close()

        if not exit_command_sent:
            raise Exception("Database.close(): Unable to send EXIT command to gracefully close connection")

    def insert(self, table_name, values):
        # Ensure that the type of table_name is a string (perform the appropriate conversion if necessary)
        if type(table_name) != str:
            table_name = str(table_name)

        # Find the corresponding table number and schema for the table referenced by table_name
        (target_table_number, target_table_schema) = self.get_table_number_and_schema(table_name)

        # If the table wasn't found, raise a PacketError exception
        if target_table_number == 0:
            raise exception.PacketError("Database.insert(): Couldn't find table " + table_name)

        # We will construct the packet that forms the INSERT command piece by piece, appending
        # each piece to the insert_command_bytes array
        insert_command_bytes = bytearray()

        # Insert the request struct into the INSERT command
        insert_request = (packet.INSERT, target_table_number)
        insert_command_bytes.extend(struct.pack("!2i", *insert_request))

        # Ensure that there are enough entries in 'values'
        if len(target_table_schema) != len(values):
            raise exception.PacketError(
                "Database.insert(): Expected " + str(len(target_table_schema)) + " values, got " + str(len(values)))

        # Construct the row struct for the INSERT command

        # The first entry of the row struct consists of the count of values
        insert_command_bytes.extend(struct.pack("!i", len(values)))

        # The next entry is an array of value structs
        #
        # We will build each value struct from the entries in 'values and then add it to the INSERT command packet,
        # ensuring along the way these entries are of the correct type
        for (i, value) in enumerate(values):
            table_column_i_schema = target_table_schema[i]

            # Determine what type is required and ensure that the passed-in value is of that correct type
            # If it is of the correct type, construct the value_struct appropriately, and then add it to the
            # INSERT command packet

            # Values of type INTEGER
            if type(value) == int and table_column_i_schema[1] == int:
                value_struct = (packet.INTEGER, 8, value)
                insert_command_bytes.extend(struct.pack("!2iq", *value_struct))

            # Values of type FLOAT
            elif type(value) == float and table_column_i_schema[1] == float:
                value_struct = (packet.FLOAT, 8, value)
                insert_command_bytes.extend(struct.pack("!2id", *value_struct))

            # Values of type STRING
            elif type(value) == str and table_column_i_schema[1] == str:
                # Calculate the "rounded up" length of the string and pad it with the appropriate
                # number of NULL characters at the end
                rounded_str_len = int(math.ceil(len(value) / 4)) * 4
                value = value + ('\x00' * (rounded_str_len - len(value)))

                value_struct = (packet.STRING, rounded_str_len, value.encode(encoding="ascii"))
                insert_command_bytes.extend(struct.pack("!2i%ds" % rounded_str_len, *value_struct))

            # Values of type FOREIGN
            elif type(value) == int and type(table_column_i_schema[1]) == str:
                value_struct = (packet.FOREIGN, 8, value)
                insert_command_bytes.extend(struct.pack("!2iq", *value_struct))

            # Value is of the wrong type
            else:
                raise exception.PacketError("Database.insert(): Element " + str(i) + " - Expected " + str(
                    table_column_i_schema[1]) + ", got " + str(type(value)))

        if self.db_socket.sendall(insert_command_bytes) is not None:
            raise Exception("Database.insert(): Unable to send the entire insert command and row data")

        # Fetch the server's response to the INSERT command issued above
        db_server_response = self.db_socket.recv(4096)

        (db_server_response_code,) = struct.unpack("!i", db_server_response[0:4])

        if db_server_response_code == packet.OK:
            # The server will return a key that follows the response code iff the response code is packet.OK
            inserted_item_key = struct.unpack("!2q", db_server_response[4:])
            return inserted_item_key
        elif db_server_response_code == packet.BAD_FOREIGN:
            raise exception.InvalidReference("Database.insert(): Server flagged an invalid foreign reference")
        else:
            # Any errors with the insert request are printed to the console for debugging purposes
            raise Exception("Database.insert(): Server responded with %d" % db_server_response_code)

    def update(self, table_name, pk, values, version=0):
        # This will be similar to insert with the addition of the struct key

        # Ensure that the type of table_name is a string (perform the appropriate conversion if necessary)
        if type(table_name) != str:
            table_name = str(table_name)

        # Ensure that the ID of the row to fetch is of the valid type (integer)
        if type(pk) != int:
            raise exception.PacketError("Database.update(): argument 'pk' - expected int, got %s" % str(type(pk)))

        if type(version) != int:
            if version is None:
                version = 0
            else:
                raise exception.PacketError(
                    "Database.update(): argument 'version' - expected int, got %s" % str(type(version)))

        # Find the corresponding table number and schema for the table referenced by table_name
        (table_number, table_schema) = self.get_table_number_and_schema(table_name)

        # If the table wasn't found, raise a PacketError exception
        if table_number == 0:
            raise exception.PacketError("Database.update(): Couldn't find table " + table_name)

        # Ensure that there are enough entries in 'values'
        if len(table_schema) != len(values):
            raise exception.PacketError(
                "Database.update(): Expected " + str(len(table_schema)) + " values, got " + str(len(values)))

        # We will construct the packet that forms the UPDATE command piece by piece, appending
        # each piece to the update_command_bytes array

        # We will build each value struct from the entries in 'values and then add it to the UPDATE command packet,
        # ensuring along the way these entries are of the correct type

        update_command_bytes = bytearray()

        # Insert the request struct into the UPDATE command
        update_request = (packet.UPDATE, table_number)
        update_command_bytes.extend(struct.pack("!2i", *update_request))

        # add the struct key to the packet
        update_key = (pk, version)  # long id; long version;
        update_command_bytes.extend(struct.pack("!2q", *update_key))

        # Construct the row struct for the UPDATE command

        # The first entry of the row struct consists of the count of values
        update_command_bytes.extend(struct.pack("!i", len(values)))

        # The next entry is an array of value structs
        #
        # We will build each value struct from the entries in 'values and then add it to the INSERT command packet,
        # ensuring along the way these entries are of the correct type
        for (i, value) in enumerate(values):
            table_column_i_schema = table_schema[i]

            # Determine what type is required and ensure that the passed-in value is of that correct type
            # If it is of the correct type, construct the value_struct appropriately, and then add it to the
            # UPDATE command packet

            # Values of type INTEGER
            if type(value) == int and table_column_i_schema[1] == int:
                value_struct = (packet.INTEGER, 8, value)
                update_command_bytes.extend(struct.pack("!2iq", *value_struct))

            # Values of type FLOAT
            elif type(value) == float and table_column_i_schema[1] == float:
                value_struct = (packet.FLOAT, 8, value)
                update_command_bytes.extend(struct.pack("!2id", *value_struct))

            # Values of type STRING
            elif type(value) == str and table_column_i_schema[1] == str:
                # Calculate the "rounded up" length of the string and pad it with the appropriate
                # number of NULL characters at the end
                rounded_str_len = int(math.ceil(len(value) / 4)) * 4
                value = value + ('\x00' * (rounded_str_len - len(value)))

                value_struct = (packet.STRING, rounded_str_len, value.encode(encoding="ascii"))
                update_command_bytes.extend(struct.pack("!2i%ds" % rounded_str_len, *value_struct))

            # Values of type FOREIGN
            elif type(value) == int and type(table_column_i_schema[1]) == str:
                value_struct = (packet.FOREIGN, 8, value)
                update_command_bytes.extend(struct.pack("!2iq", *value_struct))

            # Value is of the wrong type
            else:
                raise exception.PacketError("Database.update(): Element " + str(i) + " - Expected " + str(
                    table_column_i_schema[1]) + ", got " + str(type(value)))

        if self.db_socket.sendall(update_command_bytes) is not None:
            raise Exception("Database.update(): Unable to send the entire update command and row data")

        # Fetch the server's response to the UPDATE command issued above
        db_server_response = self.db_socket.recv(4096)

        (db_server_response_code,) = struct.unpack("!i", db_server_response[0:4])  # first four bytes

        if db_server_response_code == packet.OK:
            # The server will return a version that follows the response code iff the response code is packet.OK
            (version,) = struct.unpack("!q", db_server_response[4:])
            return version
        elif db_server_response_code == packet.TXN_ABORT:
            raise exception.TransactionAbort("Database.update(): Atomic update failed ")
        elif db_server_response_code == packet.NOT_FOUND:
            raise exception.ObjectDoesNotExist(
                "Database.update(): Server could not find an existing row id {} in {}".format(pk, table_name))
        elif db_server_response_code == packet.BAD_FOREIGN:
            raise exception.InvalidReference("Database.update(): Server flagged an invalid foreign reference")
        else:
            # Any errors with the insert request are printed to the console for debugging purposes
            raise Exception("Database.update(): Server responded with %d" % db_server_response_code)

    def drop(self, table_name, pk):
        # In EasyDB, the drop command performs cascade delete, which means that if a row is referenced by rows in other tables, those rows will also be deleted.
        # Issue, is this done automatically by the server or does the programmer need to worry about this?

        # Ensure that the type of table_name is a string (perform the appropriate conversion if necessary)
        if type(table_name) != str:
            table_name = str(table_name)

        # Ensure that the ID of the row to fetch is of the valid type (integer)
        if type(pk) != int:
            raise exception.PacketError("Database.drop(): argument 'pk' - expected int, got %s" % str(type(pk)))

        # Find the corresponding table number and schema for the table referenced by table_name
        (table_number, table_schema) = self.get_table_number_and_schema(table_name)

        # If the table wasn't found, raise a PacketError exception
        if table_number == 0:
            raise exception.PacketError("Database.drop(): Couldn't find table " + table_name)

        # Insert the request struct into the DROP command
        drop_request = (packet.DROP, table_number)
        drop_command_bytes = struct.pack("!2iq", *drop_request, pk)

        if self.db_socket.sendall(drop_command_bytes) is not None:
            raise Exception("Database.drop(): Unable to send the entire drop command")

        db_server_response = self.db_socket.recv(4096)

        (db_server_response_code,) = struct.unpack("!i", db_server_response[0:4])  # first four bytes

        print(db_server_response)

        if db_server_response_code == packet.OK:
            # The server will return nothing that follows the response code iff the response code is packet.OK
            return None

        elif db_server_response_code == packet.NOT_FOUND:
            raise exception.ObjectDoesNotExist(
                "Database.DROP(): Server could not find an existing row id {} in {}".format(pk, table_name))

        else:
            Exception("Database.drop(): Server responded with %d" % db_server_response_code)

    def get(self, table_name, pk):
        # Ensure that the type of table_name is a string (perform the appropriate conversion if necessary)
        if type(table_name) != str:
            table_name = str(table_name)

        # Ensure that the ID of the row to fetch is of the valid type (integer)
        if type(pk) != int:
            raise exception.PacketError("Database.get(): argument 'pk' - expected int, got %s" % str(type(pk)))

        # Find the corresponding table number and schema for the table referenced by table_name
        (table_number, table_schema) = self.get_table_number_and_schema(table_name)

        # If the table wasn't found, raise a PacketError exception
        if table_number == 0:
            raise exception.PacketError("Database.get(): Couldn't find table " + table_name)

        get_request = (packet.GET, table_number)
        # The get command consists of the GET request, followed by the id of the desired row
        get_command_bytes = struct.pack("!2iq", *get_request, pk)

        if self.db_socket.sendall(get_command_bytes) is not None:
            raise Exception("Database.get(): Unable to send the entire get command")

        # Fetch the entire server response to the GET command
        db_server_response = self.db_socket.recv(4096)
        index_in_response = 0

        # Extract the response code (an integer)
        (db_server_response_code,) = struct.unpack("!i", db_server_response[index_in_response: index_in_response + 4])
        index_in_response += 4

        if db_server_response_code == packet.NOT_FOUND:
            raise exception.ObjectDoesNotExist(
                "Database.get(): Couldn't find row %d from table %s (number: %d)" % (pk, table_name, table_number))
        elif db_server_response_code != packet.OK:
            raise Exception("Database.get(): Server responded with %d" % db_server_response_code)

        # The server responded with packet.OK, so extract the values that were returned

        (received_row_version,) = struct.unpack("!q", db_server_response[index_in_response: index_in_response + 8])
        index_in_response += 8

        (received_row_value_count,) = struct.unpack("!i", db_server_response[index_in_response: index_in_response + 4])
        index_in_response += 4
        assert (received_row_value_count == len(table_schema))

        # Loop through each value object in the returned row, decode the data contained in each value object, and
        # add it to the list of received row values
        received_row_values = []
        for i in range(received_row_value_count):
            (value_i_type, value_i_size) = struct.unpack("!ii",
                                                         db_server_response[index_in_response: index_in_response + 8])
            index_in_response += 8

            if value_i_type == packet.INTEGER:
                (value_i,) = struct.unpack("!q",
                                           db_server_response[index_in_response: index_in_response + value_i_size])
                index_in_response += value_i_size
            elif value_i_type == packet.FLOAT:
                (value_i,) = struct.unpack("!d",
                                           db_server_response[index_in_response: index_in_response + value_i_size])
                index_in_response += value_i_size
            elif value_i_type == packet.STRING:
                (value_i,) = struct.unpack("!%ds" % value_i_size,
                                           db_server_response[index_in_response: index_in_response + value_i_size])
                # Strings are padded with NULL characters to ensure that their lengths are multiples of 4
                # Remove this padding characters before returning the actual string
                value_i = value_i.decode("ascii").replace("\x00", "")
                index_in_response += value_i_size
            elif value_i_type == packet.FOREIGN:
                (value_i,) = struct.unpack("!q",
                                           db_server_response[index_in_response: index_in_response + value_i_size])
                index_in_response += value_i_size
            else:
                raise Exception("Database.get(): Invalid type received from server")

            received_row_values.append(value_i)

        return received_row_values, received_row_version

    def scan(self, table_name, op, column_name=None, value=None):
        # Ensure that the type of table_name is a string (perform the appropriate conversion if necessary)
        if type(table_name) != str:
            table_name = str(table_name)

        # Find the corresponding table number and schema for the table referenced by table_name
        (table_number, table_schema) = self.get_table_number_and_schema(table_name)

        # If the table wasn't found, raise a PacketError exception
        if table_number == 0:
            raise exception.PacketError("Database.scan(): Couldn't find table " + table_name)

        # If the column name does not exists in the table raise a PacketError exception
        # We don't care about column name if operator is AL
        table_columns = get_table_columns(table_schema)  # use keys to get the list of column names from the dictionary
        if op == packet.operator.AL:  # because it's checking for id
            col_number = 0
        else:
            if column_name in table_columns.keys():
                col_number = list(table_columns.keys()).index(column_name)

                # IDs and foreign fields have special restrictions on what operations they supported, so we need to
                # know if this current command involves scanning id or foreign field columns
                is_foreign_or_id = (type(table_columns[column_name]) == str)

                # If the operator is not supported in EasyDB raise a PacketError
                if is_foreign_or_id and (op != packet.operator.EQ and op != packet.operator.NE):
                    raise exception.PacketError("Database.scan(): The operator {} isn't supported with IDs".format(op))
                elif op not in range(1, 8):
                    raise exception.PacketError("Database.scan(): The operator {} isn't supported by EasyDB".format(op))
            else:
                raise exception.PacketError(
                    "Database.scan(): The column {} does not exists in the table {}".format(column_name, table_name))

        # Assuming all error handling is taken care of, start building the command packet that will be sent
        # to the server
        scan_command_bytes = bytearray()

        # Build the scan request and append it to the command packet
        scan_request = (packet.SCAN, table_number)
        scan_command_bytes.extend(struct.pack("!2i", *scan_request))

        # Add the column index and the operator to the data packet
        scan_command_bytes.extend(struct.pack("!2i", col_number, op))

        if op == packet.operator.AL:  # when operator is everything
            # col_number = 0  # set to 0? will refer to ids field from server's side
            value_struct = (packet.NULL, 0, 1)  # test with integer value
            scan_command_bytes.extend(struct.pack("!3i", *value_struct))

        # Value of type INTEGER
        elif type(value) == int and table_columns[column_name] == int:
            value_struct = (packet.INTEGER, 8, value)
            scan_command_bytes.extend(struct.pack("!2iq", *value_struct))

        # Value of type STRING 
        elif type(value) == str and table_columns[column_name] == str:
            rounded_str_len = int(math.ceil(len(value) / 4)) * 4
            value = value + ('\x00' * (rounded_str_len - len(value)))
            value_struct = (packet.STRING, rounded_str_len, value.encode(encoding="ascii"))
            scan_command_bytes.extend(struct.pack("!2i%ds" % rounded_str_len, *value_struct))

        # Value of type FLOAT 
        elif type(value) == float and table_columns[column_name] == float:
            value_struct = (packet.FLOAT, 8, value)
            scan_command_bytes.extend(struct.pack("!2id", *value_struct))

        # Values of type FOREIGN
        elif type(value) == int and is_foreign_or_id:
            value_struct = (packet.FOREIGN, 8, value)
            scan_command_bytes.extend(struct.pack("!2iq", *value_struct))

        # Invalid data type passed as right operand 'value'
        # If the right operand is the wrong data type, raise a PacketError 
        else:
            raise exception.PacketError("Database.scan(): Expected type " + str(
                table_columns[column_name]) + ", got " + str(type(value)) + "for column ".format(column_name))

        if self.db_socket.sendall(scan_command_bytes) is not None:
            raise Exception("Database.scan(): Unable to send the entire scan command and row data")

        # Fetch the server's response to the SCAN command issued above
        db_server_response = self.db_socket.recv(4096)

        (db_server_response_code,) = struct.unpack("!i", db_server_response[0:4])

        if db_server_response_code == packet.OK:
            # Get the number of ids which match the query
            # print(scan_command_bytes)
            (count,) = struct.unpack("!i", db_server_response[4:8])
            ids_list = []
            # Obtain each id (long type) from the list every 8 bytes
            for i in range(count):
                (received_id_i,) = struct.unpack("!q", db_server_response[8 + 8 * i: 16 + 8 * i])
                ids_list.append(received_id_i)
            return ids_list
        else:
            raise Exception("Database.scan(): Server responded with %d" % db_server_response_code)

    def get_table_number_and_schema(self, target_table_name):
        target_table_number = 0
        target_table_schema = None

        # Find the table being specified by target_table_name
        for (table_number, (table_name, table_schema)) in enumerate(self.tables):
            if table_name == target_table_name:
                # Note: table numbers start at 1, but enumerate returns zero-based indices
                target_table_number = table_number + 1
                target_table_schema = table_schema
                break

        return target_table_number, target_table_schema
