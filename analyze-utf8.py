#!/usr/bin/env python

from datetime import datetime
from optparse import OptionParser
import mysql.connector
import os
import sys
import time
import traceback

# WARNING: This should ideally be ran against a 
# replica or a static copy of production data.
# This tool may run ANALYZE TABLE and incur load 
# on the server being analyzed. It will run SELECT
# on columns that may potentially be not-indexed
# at worst.

cwd = os.path.dirname(os.path.realpath(__file__))

# Default connection parameters to MySQL servers that will 
# be analyzed.

mysql_user = 'mysqluser'
mysql_pass = 'mysqlpass'
mysql_port = 3306

# The meta parameters is a connection to a temporary MySQL server
# this will host the tables for the list of server -group_restore_hosts 
# and the log table when analyzing tables - group_db_tbl_double_encoding

meta_host = '127.0.0.1'
meta_port = 33306
meta_user = 'msandbox'
meta_pass = 'msandbox'
meta_db = 'utf8db'

def date(unixtime, format = '%m/%d/%Y %H:%M:%S'):
    d = datetime.fromtimestamp(unixtime)
    return d.strftime(format)

def _out(tag, *msgs):
    s = ''

    if not msgs:
        return

    for msg in msgs:
        s += str(msg)

    print "[%s] %s: %s" % (date(time.time()), tag, s)

def _say(*msgs):
    _out('INFO', *msgs)

def _warn(*msgs):
    _out('WARN', *msgs)

def _error(*msgs):
    _out('ERROR', *msgs)

def _die(*msgs):
    _out('FATAL', *msgs)
    raise Exception(str(msgs))

class AnalyzeTableUtf8(object):
    sql_chunk = ("SELECT COUNT(*) AS affected_rows FROM ( "
    "SELECT "
    "    LENGTH(CONVERT(REGEXP_REPLACE("
    "        BINARY `{0}`, CONCAT('[', UNHEX('01'), '-', UNHEX('7F'), ']'), '') using utf8mb4)"
    "    )/CHAR_LENGTH(CONVERT(REGEXP_REPLACE("
    "        BINARY `{0}`, CONCAT('[', UNHEX('01'), '-', UNHEX('7F'), ']'), '') using utf8mb4)"
    "    ) c_to_b, "
    "    (CONVERT(`{0}` USING BINARY) RLIKE CONCAT('[', UNHEX('80'), '-', UNHEX('FF'), ']{{3}}')) err "
    "FROM `{1}` "
    "LIMIT {2}, {3} "
    ") t "
    "WHERE c_to_b < 3.0 AND err = 1")

    sql_full = ("SELECT COUNT(*) AS affected_rows FROM ( "
    "SELECT "
    "    LENGTH(CONVERT(REGEXP_REPLACE("
    "        BINARY `{0}`, CONCAT('[', UNHEX('01'), '-', UNHEX('7F'), ']'), '') using utf8mb4)"
    "    )/CHAR_LENGTH(CONVERT(REGEXP_REPLACE("
    "        BINARY `{0}`, CONCAT('[', UNHEX('01'), '-', UNHEX('7F'), ']'), '') using utf8mb4)"
    "    ) c_to_b, "
    "    (CONVERT(`{0}` USING BINARY) RLIKE CONCAT('[', UNHEX('80'), '-', UNHEX('FF'), ']{{3}}')) err "
    "FROM `{1}` "
    "HAVING c_to_b < 3.0 AND err = 1 "
    ") t")

    def __init__(self, meta_con, mysql_con, opts, ns):
        self.collection = ns[0]
        self.db = ns[1]
        self.tbl = ns[2]
        self.meta_con = meta_con
        self.mysql_con = mysql_con
        self.opts = opts
        self.counts = dict()

    def get_columns(self):
        cur = self.mysql_con.cursor()
        sql = ("SELECT column_name as col FROM information_schema.columns "
               "WHERE table_schema = '%s' AND table_name = '%s' AND "
               "data_type IN ('varchar', 'longtext', 'char', 'mediumtext', 'text', 'tinytext')")
        cur.execute(sql % (self.db, self.tbl))
        rows = cur.fetchall()
        cols = []
        if cur.rowcount > 0:
            for row in rows:
                cols.append(row[0])

        cur.close()
        return cols

    def get_table_rows(self):
        cur = self.mysql_con.cursor(named_tuple=True, buffered=True)

        if self.opts.analyze:
            cur.execute('ANALYZE TABLE `%s`' % self.tbl)

        sql = (("SELECT table_rows FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'") % 
                (self.db, self.tbl))
        cur.execute(sql)
        row = cur.fetchone()
        rowcount = row.table_rows
        cur.close()
        return rowcount

    def run(self):
        _say('Counting for potential double encoding %s/%s/%s' % (
            self.collection, self.db, self.tbl))

        cols = self.get_columns()
        rowcount = self.get_table_rows()

        for col in cols:
            if self.opts.chunk and rowcount > 2000000:
                self.analyze_col_chunk(col, rowcount)
            else:
                self.analyze_col_full(col)

    def analyze_col_chunk(self, col, rowcount):
        try:
            offset = (self.opts.chunk_rows * self.opts.chunk_factor - self.opts.chunk_rows)
            cur = self.mysql_con.cursor(named_tuple=True, buffered=True)
            count = 0
            percent = 0
            percentd = 0
            sys.stdout.write('%s %% ..0' % col)
            sys.stdout.flush()

            while True:
                sql = self.sql_chunk.format(col, self.tbl, offset, self.opts.chunk_rows)
                cur.execute(sql)
                row = cur.fetchone()
                count = count + row.affected_rows

                offset = offset + (self.opts.chunk_rows * self.opts.chunk_factor)
                percent = int(round((float(offset)/rowcount)*100, -1))

                if percent%10 == 0 and percent != percentd:
                    sys.stdout.write('..%d' % percent)
                    sys.stdout.flush()
                    percentd = percent

                if offset > rowcount:
                    print '%'
                    break

            self.log_encoding_count(col, count)
            _say('%s %s' % (col.ljust(15), count))
            cur.close()
        except mysql.connector.ProgrammingError as err:
            _error(str(err))
        finally:
            if cur is not None:
                cur.close()

    def analyze_col_full(self, col):
        try:
            cur = self.mysql_con.cursor(named_tuple=True, buffered=True)
            sql = self.sql_full.format(col, self.tbl)
            
            cur.execute(sql)
            row = cur.fetchone()
            self.log_encoding_count(col, row.affected_rows)
            _say('%s %s' % (col.ljust(15), row.affected_rows))
            cur.close()
        except mysql.connector.ProgrammingError as err:
            _error(str(err))
        finally:
            if cur is not None:
                cur.close()

    def log_encoding_count(self, col, count):
        sql = ("REPLACE INTO group_db_tbl_double_encoding ("
               "collection, db, tbl, col, double_enc_est) "
               "VALUES ('%s', '%s', '%s', '%s', %d)")
        cur = self.meta_con.cursor(named_tuple=True, buffered=True)
        cur.execute(sql % (self.collection, self.db, self.tbl, col, count))
        cur.close()

class AnalyzeDbUtf8(object):
    def __init__(self, meta_con, mysql_con, opts, ns):
        self.meta_con = meta_con
        self.mysql_con = mysql_con
        self.collection = ns[0]
        self.db = ns[1]
        self.opts = opts

    def get_tables(self):
        cur = self.mysql_con.cursor()
        sql = ("SELECT DISTINCT table_name tbl FROM information_schema.tables "
               "WHERE table_schema = '%s' AND table_type <> 'VIEW'")
        cur.execute(sql % (self.db))
        rows = cur.fetchall()
        tbls = []
        if cur.rowcount > 0:
            for row in rows:
                tbls.append(row[0])

        cur.close()
        return tbls

    def run(self):
        _say('Running analysis on %s/%s' % (self.collection, self.db))
        tbls = self.get_tables()

        for tbl in tbls:
            tbl_analyzer = AnalyzeTableUtf8(self.meta_con, self.mysql_con, 
                                            self.opts, [self.collection, 
                                            self.db, tbl])
            tbl_analyzer.run()

class AnalyzeServerUtf8(object):
    def __init__(self, meta_con, server, opts):
        self.meta_con = meta_con
        self.collection = server
        self.opts = opts

    def get_databases(self):
        con = self.make_con('information_schema', self.opts.server)
        cur = con.cursor()
        sql = ("SELECT DISTINCT schema_name db FROM information_schema.schemata "
               "WHERE schema_name not in ('information_schema', 'sys', 'performance_schema', 'mysql')")
        cur.execute(sql)
        rows = cur.fetchall()
        dbs = []
        if cur.rowcount > 0:
            for row in rows:
                dbs.append(row[0])

        cur.close()
        con.close()
        return dbs

    def make_con(self, db, server):
        cur = self.meta_con.cursor(named_tuple=True, buffered=True)
        cur.execute('SELECT ip FROM group_restore_hosts WHERE collection = "%s"' % server)
        row = cur.fetchone()
        mysql_host = row.ip

        mysql_con = mysql.connector.connect(
            user=mysql_user, password=mysql_pass, database=db, host=mysql_host, 
            port=mysql_port, charset='utf8mb4', allow_local_infile=True, 
            autocommit=True, use_pure=True)

        return mysql_con

    def run(self):
        dbs = self.get_databases()
        for db in dbs:
            mysql_con = self.make_con(db, self.opts.server)
            db_analyzer = AnalyzeDbUtf8(self.meta_con, mysql_con, self.opts, 
                                        [self.collection, db])
            db_analyzer.run()
            mysql_con.close()

class AnalyzeUtf8(object):
    def __init__(self):
        parser = OptionParser('Usage: %prog [options]')
        parser.add_option('-S', '--server', dest='server', type='string',
            help='The collection to analyze')
        parser.add_option('-D', '--database', dest='database', type='string',
            help='The database to analyze')
        parser.add_option('-T', '--table', dest='table', type='string',
            help='The table to analyze')
        # Run analyze table first to refresh row count estimates 
        # on tables, ideally this should be set to True if 
        # chunk_table is also set to True
        parser.add_option('-a', '--anayze', dest='analyze', action='store_true',
            help='Wether to run ANALYZE TABLE to refresh row count estimate',
            default=False)
        parser.add_option('-c', '--chunk', dest='chunk', action='store_true',
            help='Wether to chunk the analysis by --chunk-rows to refresh row count estimate',
            default=False)
        parser.add_option('-r', '--chunk-rows', dest='chunk_rows', type="int",
            help='How many rows to examine per batch', default=100000)
        # When a table grows beyond 2000000 it will be sample by 10%
        # Set this to one if you do not want sampling
        parser.add_option('-f', '--chunk-factor', dest='chunk_factor', type="int",
            help='How many rows to examine per batch', default=1)

        (self.opts, args) = parser.parse_args()
        self.ns = []
        if self.opts.server is not None:
            self.ns.append(self.opts.server) 
        if self.opts.database is not None:
            self.ns.append(self.opts.database)
        if self.opts.table is not None:
            self.ns.append(self.opts.table)  

        if self.opts.table is not None and len(self.ns) < 3:
            parser.error('Server/collection and database is required when --table is specified')

        if self.opts.database is not None and self.opts.server is None:
            parser.error('Server/collection is required when --database is specified')

    def run(self):
        meta_con = mysql.connector.connect(
            user=meta_user, password=meta_pass, host=meta_host,
            database=meta_db, port=meta_port, charset='utf8mb4', 
            allow_local_infile=True, autocommit=True, use_pure=True)

        server = AnalyzeServerUtf8(meta_con, self.opts.server, self.opts)
        if len(self.ns) == 3:
            mysql_con = server.make_con(self.opts.database, self.opts.server)
            analyzer = AnalyzeTableUtf8(meta_con, mysql_con, self.opts, self.ns)
            analyzer.run()
        elif len(self.ns) == 2:
            mysql_con = server.make_con(self.opts.database, self.opts.server)
            analyzer = AnalyzeDbUtf8(meta_con, mysql_con, self.opts, self.ns)
            analyzer.run()
        else:
            server.run()

        meta_con.close()
        return 0

if __name__ == "__main__":
    try:
        analyzer = AnalyzeUtf8()
        sys.exit(analyzer.run())
    except Exception, e:
        traceback.print_exc()
        sys.exit(1)
        

