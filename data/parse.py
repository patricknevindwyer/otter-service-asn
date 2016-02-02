#!/bin/python
import fileinput
import argparse
import uuid
import sqlite3
from ipaddress import IPv4Address

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--file", nargs="+", required=True)
    return parser.parse_args()

def createTables(conn):
    """
    Build the basic tables we need
    :param conn:
    :return:
    """
    conn.execute("DROP TABLE IF EXISTS entity")
    conn.execute("DROP TABLE IF EXISTS asn")
    conn.execute("DROP TABLE IF EXISTS ipv4")

    conn.execute('''
        CREATE TABLE entity (id text)
    ''')

    conn.execute('''
        CREATE TABLE asn (id text, cc text, source text, state text, asn text, date text)
    ''')

    conn.execute('''
        CREATE TABLE ipv4 (
            id text,
            cc text,
            source text,
            state text,
            ip_start text,
            ip_end text,
            ip_start_int integer,
            ip_end_int integer,
            ip_count integer,
            date text)
    ''')

    conn.execute("create index iprange_idx on ipv4 (ip_start_int, ip_end_int)")
    conn.execute("create index asnid_idx on asn (id)")
    conn.commit()

def writeRecords(conn, recordKeeper):
    """
    Write out a record set

    :param args:
    :param recordKeeper:
    :return:
    """
    print ("Writing out records...")
    print ("\t%d distinct entities" % (len(recordKeeper)))

    for (id, block) in recordKeeper.items():
        # write the entity record
        conn.execute('''
        INSERT INTO entity VALUES (?)
        ''', (block['id'],))

        # write the ASNs
        asns = []
        for asn in block['asn']:
            asns.append(
                [block['id'], asn['cc'], asn['source'], asn['state'], asn['asn'], asn['date']]
            )
        conn.executemany("INSERT INTO asn VALUES (?, ?, ?, ?, ?, ?)", asns)

        # write the ipv4s
        ipv4s = []
        for ipv4 in block['ipv4']:
            ipv4s.append(
                [block['id'], ipv4['cc'], ipv4['source'], ipv4['state'],
                 ipv4['ip_start'], ipv4["ip_end"], ipv4['ip_start_int'],
                 ipv4['ip_end_int'], ipv4['ip_count'], ipv4['date']]
            )
        conn.executemany("INSERT INTO ipv4 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ipv4s)
        conn.commit()

def process(args):

    # build our DB
    conn = sqlite3.connect(args.db)

    createTables(conn)

    # start record processing
    recordInFile = 0
    recordKeeper = {}

    with fileinput.input(files=args.file) as f:
        for line in f:

            # reset the record counter for each file
            if fileinput.isfirstline():

                # write out the recordKeeper
                if len(recordKeeper) > 0:
                    writeRecords(conn, recordKeeper)
                recordInFile = 0
                recordKeeper = {}

            # hooray, parse around comments
            if line.startswith("#"):
                pass
            else:
                if recordInFile == 0:
                    # header line
                    print("Starting [%s]" % (fileinput.filename()))
                    bits = line.split("|")
                    print("Parsing %s records from %s (%s records)" % (bits[1], bits[5], bits[3]))
                elif line.strip().endswith("summary"):
                    # summary line
                    bits = line.split("|")
                    print("\t%s %s records" % (bits[4], bits[2]))
                else:
                    # record processing

                    # find our common bits
                    bits = line.split("|")
                    recType = bits[2]
                    cc = bits[1]
                    opaque_id = ""
                    state = bits[6]
                    source = bits[0]

                    # check for an opaque id to link everything together
                    if len(bits) >= 8:
                        opaque_id = bits[7]
                    else:
                        # create a temporary opaque id
                        opaque_id = str(uuid.uuid4())

                    # make sure this is in recordKeeper
                    if not opaque_id in recordKeeper:
                        recordKeeper[opaque_id] = {"asn": [], "ipv4": [], "id": str(uuid.uuid4())}

                    if recType == "ipv4":
                        ip_base = IPv4Address(bits[3])
                        ip_count = bits[4]
                        range_date = bits[5]

                        ip_start = str(ip_base)
                        ip_start_int = int(ip_base)
                        ip_end = str(ip_base + (int(ip_count) - 1))
                        ip_end_int = int(IPv4Address(ip_end))

                        recordKeeper[opaque_id]["ipv4"].append(
                            {
                                "cc": cc,
                                "state": state,
                                "source": source,
                                "ip_start": ip_start,
                                "ip_start_int": ip_start_int,
                                "ip_end": ip_end,
                                "ip_end_int": ip_end_int,
                                "ip_count": ip_count,
                                "date": range_date
                            }
                        )
                    elif recType == "ipv6":
                        # not recording
                        pass
                    elif recType == "asn":
                        asn = bits[3]
                        asn_date = bits[5]
                        recordKeeper[opaque_id]["asn"].append(
                            {
                                "cc": cc,
                                "state": state,
                                "source": source,
                                "asn": asn,
                                "date": asn_date
                            }
                        )

                recordInFile += 1
    writeRecords(conn, recordKeeper)

    c = conn.cursor()
    c.execute("SELECT count(*) FROM entity")
    print ("Wrote %d entities" % (c.fetchone()))

if __name__ == "__main__":
    pargs = args()
    print("Parsing %d files into db (%s)" % (len(pargs.file), pargs.db))
    process(pargs)