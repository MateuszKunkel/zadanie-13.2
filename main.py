import sqlite3
from sqlite3 import Error

# from sqlalchemy import create_engine


# LAUNCH_________________________________________


def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def create_connection_in_memory():
    conn = None
    try:
        conn = sqlite3.connect(":memory:")
        print(f"Połączono na sqlite ver. {sqlite3.version}")
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Połączono z plikiem {db_file}, sqlite ver. {sqlite3.version}")
        return conn
    except Error as e:
        print(e)
    return conn


# ADD_FUNCTIONS____________________________________


def add_receiver(conn, receiver):
    sql = """INSERT INTO receivers(imię, nazwisko, miasto, adres)
        VALUES (?,?,?,?)"""
    c = conn.cursor()
    c.execute(sql, receiver)
    conn.commit()
    return c.lastrowid


def add_parcel(conn, parcel):
    sql = """INSERT INTO parcels(receiver_id, wysokość, długość, szerokość, data_nadania, dostarczono)
        VALUES (?,?,?,?,?,?)"""
    c = conn.cursor()
    c.execute(sql, parcel)
    conn.commit()
    return c.lastrowid


# UPDATE_FUNCTIONS__________________________________


def update(conn, table, id, **kwargs):
    """
    :param kwargs: has to be dict of attributes and values
    """
    par = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(par)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f""" UPDATE {table}
            SET {parameters}
            WHERE id = ?"""
    try:
        c = conn.cursor()
        c.execute(sql, values)
        conn.commit()
    except sqlite3.OperationalError as e:
        print(e)


# SELECT_FUNCTIONS__________________________________


def select_where(conn, table, **query):
    """
    :param query: has to be dict of attributes and values
    :return: all rows
    """
    c = conn.cursor()
    qs = []
    val = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        val += (v,)
    q = " AND ".join(qs)
    c.execute(f"SELECT * FROM {table} WHERE {q}", val)
    rows = c.fetchall()
    return rows


def select_all(conn, table):
    """
    :return: all rows
    """
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table}")
    rows = c.fetchall()
    return rows


# DELETE_FUNCTIONS__________________________________


def delete_where(conn, table, **kwargs):
    """
    :param kwargs: has to be dict of attributes and values
    """
    qs = []
    val = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        val += (v,)
    q = " AND ".join(qs)

    sql = f"DELETE FROM {table} WHERE {q}"
    c = conn.cursor()
    c.execute(sql, val)
    conn.commit()
    print("DELETED")


def delete_all(conn, table):
    sql = f"DELETE FROM {table}"
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    print("DELETED ALL")


# MAIN_FUNCTIONS______________________________________


if __name__ == "__main__":
    db_file = "post_station.db"

    create_receivers_sql = """
    CREATE TABLE IF NOT EXISTS receivers  (
        id integer PRIMARY KEY,
        imię text NOT NULL,
        nazwisko text NOT NULL,
        miasto text NOT NULL,
        adres text NOT NULL
    );
    """

    create_parcels_sql = """
    CREATE TABLE IF NOT EXISTS parcels  (
        id integer PRIMARY KEY,
        receiver_id integer NOT NULL,
        wysokość integer NOT NULL,
        długość integer NOT NULL,
        szerokość integer NOT NULL,
        data_nadania text,
        dostarczono VARCHAR(15) NOT NULL,
        FOREIGN KEY (receiver_id) REFERENCES receivers (id)
    );
    """

    conn = create_connection(db_file)
    if conn is not None:
        execute_sql(conn, create_receivers_sql)
        execute_sql(conn, create_parcels_sql)

    # receiver = ("imie", "nazwisko", "miasto", "adres")
    # receiver_id = add_receiver(conn, receiver)
    # parcel = (receiver_id, "wysokość", "długość", "szerokość", "data_nadania", "dostarczono")
    # parcel_id = add_parcel(conn, parcel)

    conn.commit()
    conn.close()
