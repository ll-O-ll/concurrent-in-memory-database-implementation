#!/usr/bin/python3
#
# main.py
#
# starts interactive shell for testing the EasyDB driver
#

import easydb
from easydb import operator

tb = (
    ("User", (  # table_name
        ("firstName", str),  # (column_name, type)
        ("lastName", str),
        ("height", float),
        ("age", int),
    )),

    ("Account", (
        ("user", "User"),  # (column_name, table_reference)
        ("type", str),
        ("balance", float),
    )),
)


class List(list):
    def get(self, index, default):
        try:
            return self[index]
        except:
            return default


def main():
    import sys
    args = List(sys.argv)

    if len(args) >= 2 and args[1] == "run":
        import code
        host = args.get(2, "localhost")
        port = int(args.get(3, 7000))

        # create db object
        db = easydb.Database(tb)

        items = {
            'db': db,
            'operator': operator,
        }

        # start the database connection
        successfully_connected = db.connect(host, port)
        if not successfully_connected:
            print("Unable to connect to database server")
            return

        inserted_user_row_key = db.insert("User", ["one'st", "lst", 45.0, 4])
        inserted_account_row_key = db.insert("Account", [inserted_user_row_key[0], "types", 11.1])
        # print(db.get("User", inserted_user_row_key[0]))
        # print(db.get("Account", inserted_account_row_key[0]))
        print(db.scan("Account", operator.AL, "type", "types"))
        # start interactive mode
        code.interact(local=items)

        # close database connection
        db.close()

    else:
        print("usage:", sys.argv[0], "run [HOST [PORT]]")
        print("\tstarts interactive shell")


if __name__ == "__main__":
    main()
