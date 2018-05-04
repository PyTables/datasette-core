import sqlite3
from .utils import (
    DatasetteError,
    detect_spatialite,
    escape_sqlite,
    get_all_foreign_keys,
    sqlite_timelimit,
)


def managed_errors(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (sqlite3.OperationalError) as e:
            raise DatasetteError(str(e))
    return func_wrapper


class SQLite3_connector:
    def __init__(
            self, path, plugin_manager=None, max_returned_rows=1000,
            sqlite_functions=None, sqlite_extensions=None):
        self.path = path
        self.plugin_manager = plugin_manager
        self.max_returned_rows = max_returned_rows
        self.sqlite_functions = sqlite_functions or []
        self.sqlite_extensions = sqlite_extensions
        self.conn = None

    def prepare_connection(self, conn):
        conn.row_factory = sqlite3.Row
        conn.text_factory = lambda x: str(x, 'utf-8', 'replace')
        for name, num_args, func in self.sqlite_functions:
            conn.create_function(name, num_args, func)
        if self.sqlite_extensions:
            conn.enable_load_extension(True)
            for extension in self.sqlite_extensions:
                conn.execute("SELECT load_extension('{}')".format(extension))
        if self.plugin_manager:
            self.plugin_manager.hook.prepare_connection(conn=conn)
        self.conn = conn

    @managed_errors
    def inspect(self):
        # List tables and their row counts
        tables = {}
        views = []
        with sqlite3.connect('file:{}?immutable=1'.format(self.path), uri=True, check_same_thread=False) as conn:
            self.prepare_connection(conn)
            table_names = [
                r['name']
                for r in conn.execute('select * from sqlite_master where type="table"')
            ]
            views = [v[0] for v in conn.execute('select name from sqlite_master where type = "view"')]
            for table in table_names:
                try:
                    count = conn.execute(
                        'select count(*) from {}'.format(escape_sqlite(table))
                    ).fetchone()[0]
                except sqlite3.OperationalError:
                    # This can happen when running against a FTS virtual tables
                    # e.g. "select count(*) from some_fts;"
                    count = 0
                # Figure out primary keys
                table_info_rows = [
                    row for row in conn.execute(
                        'PRAGMA table_info("{}")'.format(table)
                    ).fetchall()
                    if row[-1]
                ]
                table_info_rows.sort(key=lambda row: row[-1])
                primary_keys = [str(r[1]) for r in table_info_rows]
                label_column = None
                # If table has two columns, one of which is ID, then label_column is the other one
                column_names = [r[1] for r in conn.execute(
                    'PRAGMA table_info({});'.format(escape_sqlite(table))
                ).fetchall()]
                if column_names and len(column_names) == 2 and 'id' in column_names:
                    label_column = [c for c in column_names if c != 'id'][0]
                tables[table] = {
                    'name': table,
                    'columns': column_names,
                    'primary_keys': primary_keys,
                    'count': count,
                    'label_column': label_column,
                    'hidden': False,
                }

            foreign_keys = get_all_foreign_keys(conn)
            for table, info in foreign_keys.items():
                tables[table]['foreign_keys'] = info

            # Mark tables 'hidden' if they relate to FTS virtual tables
            hidden_tables = [
                r['name']
                for r in conn.execute(
                    '''
                        select name from sqlite_master
                        where rootpage = 0
                        and sql like '%VIRTUAL TABLE%USING FTS%'
                    '''
                )
            ]

            if detect_spatialite(conn):
                # Also hide Spatialite internal tables
                hidden_tables += [
                    'ElementaryGeometries', 'SpatialIndex', 'geometry_columns',
                    'spatial_ref_sys', 'spatialite_history', 'sql_statements_log',
                    'sqlite_sequence', 'views_geometry_columns', 'virts_geometry_columns'
                ]

            for t in tables.keys():
                for hidden_table in hidden_tables:
                    if t == hidden_table or t.startswith(hidden_table):
                        tables[t]['hidden'] = True
                        continue

        return tables, views

    @managed_errors
    def execute(self, sql, params=None, truncate=False, time_limit_ms=1000):
        with sqlite_timelimit(self.conn, time_limit_ms):
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, params or {})
                if self.max_returned_rows and truncate:
                    rows = cursor.fetchmany(self.max_returned_rows + 1)
                    truncated = len(rows) > self.max_returned_rows
                    rows = rows[:self.max_returned_rows]
                else:
                    rows = cursor.fetchall()
                    truncated = False
            except Exception as e:
                print('ERROR: conn={}, sql = {}, params = {}: {}'.format(
                    self.conn, repr(sql), params, e
                ))
                raise
        if truncate:
            return rows, truncated, cursor.description
        else:
            return rows

    def get_foreign_columns_and_rows(self, table, rows):
        "Fetch foreign key resolutions"
        fks = {}
        labeled_fks = {}
        tables_info, _ = self.inspect()
        foreign_keys = tables_info[table]['foreign_keys']['outgoing']
        for fk in foreign_keys:
            label_column = tables_info.get(fk['other_table'], {}).get('label_column')
            if not label_column:
                # No label for this FK
                fks[fk['column']] = fk['other_table']
                continue
            ids_to_lookup = set([row[fk['column']] for row in rows])
            sql = 'select "{other_column}", "{label_column}" from {other_table} where "{other_column}" in ({placeholders})'.format(
                other_column=fk['other_column'],
                label_column=label_column,
                other_table=escape_sqlite(fk['other_table']),
                placeholders=', '.join(['?'] * len(ids_to_lookup)),
            )
            try:
                results = self.execute(sql, list(set(ids_to_lookup)))
            except sqlite3.OperationalError:
                # Probably hit the timelimit
                pass
            else:
                for id, value in results:
                    labeled_fks[(fk['column'], id)] = (fk['other_table'], value)

        return fks, labeled_fks

    def is_view(self, table):
        return bool(list(self.execute(
            "SELECT count(*) from sqlite_master WHERE type = 'view' AND name=:n",
            {'n': table}
        ))[0][0])

    def get_definition(self, table, type='table'):
        res = None
        rows = list(self.execute(
            "SELECT sql FROM sqlite_master WHERE name = :n AND type = :t",
            {'n': table, 't': type}
        ))
        if rows:
            res = rows[0][0]
        return res
