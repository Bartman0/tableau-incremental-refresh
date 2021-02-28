import argparse
import datetime as dt
import getpass
import json
import logging
import math
import os
import shutil
import time
import zipfile
from datetime import time
from pathlib import Path

import jaydebeapi as db
import tableauserverclient as tsc
from tableaudocumentapi import Datasource
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName
from tableauserverclient import ConnectionCredentials, DailyInterval

from utils import datasource_quote_date

WORK_DIR = "work"

# globals
config = dict()
updates = dict()
projects = dict()


def handler(signum, frame):
    """Handler for the alarm timeout"""
    raise RuntimeError("timeout waiting for jobs to finish")


def database_connect(name):
    """Function that opens a database connection using the configuration based on the database name as input"""
    database = config['databases'][name]
    connection = db.connect(database['class'], database['url'], driver_args=database['args'], jars=database['jars'])
    return connection


def get_database_values(database, datasource, update_value):
    """Function that collects some important values from the database based on the configuration settings for the given database and datasource:
    - the minimum value of the functional ordered column after the last seen update value
    - the last update value as already available in the database reference table
    """
    with database_connect(database) as connection:
        with connection.cursor() as cursor:
            # get the minimun value of the functional ordered column that can be seen after the last update value
            cursor.execute(f"""select min({config['datasources'][datasource]['functional_ordered_column']}) 
                                from {config['datasources'][datasource]['reference_table']} 
                                where {config['parameters']['update_datetime_column']} > {update_value}
                                """)
            result_rows = cursor.fetchall()
            if len(result_rows) < 1:
                return None, None, None
            functional_ordered_column_type = cursor.description[0][1]
            functional_ordered_column_value_min = result_rows[0][0]
            # dates, times and text must be quoted in the database query
            if functional_ordered_column_type in {db.DATE, db.TIME, db.DATETIME, db.STRING, db.TEXT}:
                functional_ordered_column_value_min = f"'{functional_ordered_column_value_min}'"
            # get the maximum update value that is currently present in the reference table
            cursor.execute(f"""select max({config['parameters']['update_datetime_column']}) 
                                from {config['datasources'][datasource]['reference_table']}""")
            result_rows = cursor.fetchall()
            last_update_value = result_rows[0][0]
    return functional_ordered_column_value_min, last_update_value, functional_ordered_column_type


def hyper_prepare(hyper_path, functional_ordered_column, column_value):
    """Function that prepares the given hyper file: based on the hyper's path, the functional ordered column and its value,
     the hyper file is cleaned from the latest set of data by deleting all data with the given column value or greater values
     """
    path_to_database = Path(hyper_path).expanduser().resolve()
    logging.info(f"full path to hyper file: {path_to_database}")
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, user_agent=os.path.basename(__file__)) as hyper:
        table_name = TableName("Extract", "Extract")
        with Connection(endpoint=hyper.endpoint, database=path_to_database) as connection:
            # delete all rows where the functional ordered column is a candidate for updating
            rows_affected = connection.execute_command(command=f'DELETE FROM {table_name} WHERE "{functional_ordered_column}" >= {column_value}')
            # retrieve the remaining max value of the functional ordered column
            with connection.execute_query(query=f'SELECT max("{functional_ordered_column}") FROM {table_name}') as result:
                rows = list(result)
                functional_ordered_column_previous = rows[0][0]
    return rows_affected, functional_ordered_column_previous


def datasource_prepare(server, project, ds, hyper_dir, download_dir):
    """Function that prepares the data source on the given server in the given project:
    - get the functional ordered column and the last update value of the reference table
    - clean up the hyper extract by deleting to be refreshed data
    - set the last refresh value to be applied in the next incremental update of the hyper extract
    """
    global projects
    global updates

    p = projects[project]
    for datasource in tsc.Pager(server.datasources):
        logging.debug("{0} ({1})".format(datasource.name, datasource.project_name))
        if datasource.name == ds and datasource.project_name == project:
            logging.info("{0}: {1}".format(datasource.name, datasource.project_name, datasource.id))
            # if we got a hyper_dir, try getting the hyper file locally, i.e. do not include the data extract
            if hyper_dir is None:
                ds_file = server.datasources.download(datasource.id, filepath=WORK_DIR, include_extract=True)
            else:
                ds_file = server.datasources.download(datasource.id, filepath=WORK_DIR, include_extract=False)
            # extract the file if it is a zip, i.e. a .tdsx file
            if zipfile.is_zipfile(ds_file):
                with zipfile.ZipFile(ds_file) as zf:
                    zf.extractall()
            # load the datasource from file and do some sanity checks
            tds = Datasource.from_file(ds_file)
            if not tds.has_extract():
                logging.error(f"datasource {ds} does not contain an extract")
                return
            if tds.extract.connection.dbclass != 'hyper':
                logging.error(f"datasource {ds} is not based on a hyper extract")
                return
            if not tds.extract.has_refresh():
                logging.error(f"datasource {ds} does not have refresh information")
                return
            database = tds.connections[0].dbname

            # get the update value that we saw the last time
            last_update_value = updates['datasources'][ds]['last_update_value']
            if last_update_value is None or last_update_value == "":
                logging.error(f"datasource {ds} does not have a last update value set, please provide one")
                return

            # using the last update value, get the minimum involved value for the functional and ordered column
            functional_ordered_column_value_min, last_update_value, functional_ordered_column_type = get_database_values(database, ds, last_update_value)
            if functional_ordered_column_value_min is None:
                logging.info(f"no data to be processed for {datasource}")
                return
            hyper_file = tds.extract.connection.dbname
            new_dbname = None

            # if we got a hyper_dir, try getting the hyper file locally, i.e. copy it from that directory
            if hyper_dir is not None:
                hyper_file = os.path.join(os.pathsep, hyper_dir, tds.extract.connection.dbname)
                work_hyper_file = os.path.join(os.pathsep, os.getcwd(), download_dir, tds.extract.connection.dbname)
                new_dbname = download_dir + "/" + tds.extract.connection.dbname
                logging.info(f"work hyper file: {work_hyper_file}")
                target_dir = f"{os.path.dirname(work_hyper_file)}"
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                shutil.copy2(hyper_file, target_dir)
                logging.info(f"hyper file copied to: {target_dir}")
                hyper_file = work_hyper_file
            logging.info(f"hyper file located at: {hyper_file}")
            # prepare the hyper file, i.e. delete all relevant, to be updated, data and get the previous value of the functional ordered column
            logging.info(f"datasource {ds}, minimum value for functional ordered column: {functional_ordered_column_value_min}")
            rows_affected, functional_ordered_column_value_previous = hyper_prepare(hyper_file,
                                                                        config['datasources'][ds]['functional_ordered_column'],
                                                                        functional_ordered_column_value_min)
            logging.info(f"datasource {ds} with hyper file {hyper_file}: {rows_affected} rows were deleted")
            # set the previous value of the functional ordered column in the extract events so the incremental refresh gets the valid continuation point
            tds.extract.refresh.refresh_events[-1].increment_value = datasource_quote_date(functional_ordered_column_value_previous)
            if new_dbname is not None:
                tds.extract.connection.dbname = new_dbname
            # save the new datasource file and upload it back to the server with the new hyper extract
            tds.save_as(ds_file)
            credentials = ConnectionCredentials(config['databases'][database]['args']['user'],
                                                config['databases'][database]['args']['password'], embed=True)
            new_ds = tsc.DatasourceItem(p.id)
            new_ds.name = ds
            server.datasources.publish(new_ds, ds_file, mode=tsc.Server.PublishMode.Overwrite, connection_credentials=credentials)
            # set the new last update value
            logging.info(f"datasource {ds}, new update value: {last_update_value}")
            if functional_ordered_column_type in {db.DATE, db.TIME, db.DATETIME, db.STRING, db.TEXT}:
                updates['datasources'][ds]['last_update_value'] = f"'{last_update_value}'"
            else:
                updates['datasources'][ds]['last_update_value'] = last_update_value


def update_incremental_schedule(server, project, ds):
    schedule_to_update = None
    # look up the schedule that has the same name as the datasource
    for s in tsc.Pager(server.schedules):
        logging.debug(f"schedule id: {s.id}, schedule name: {s.name}")
        if s.name == ds:
            schedule_to_update = s
    if schedule_to_update is None:
        logging.error(f"can not find a matching schedule to update: {ds}")
    else:
        # round the schedule time to the next slot of 15 minutes
        now_plus_15 = dt.datetime.now() + dt.timedelta(minutes=15)
        interval_item = DailyInterval(start_time=time(now_plus_15.hour, math.floor(now_plus_15.minute/15)*15))
        schedule_to_update.interval_item = interval_item
        # update the schedule
        server.schedules.update(schedule_item=schedule_to_update)


def get_projects(server):
    """Function that retrieves all projects from the Tableau server"""
    projects = dict()
    for p in tsc.Pager(server.projects):
        projects[p.name] = p
    return projects


def main():
    global config
    global updates
    global projects

    parser = argparse.ArgumentParser(description='perform incremental refresh on datasources',
                                     fromfile_prefix_chars='@')
    parser.add_argument('--config', '-c', required=True, help='configuration file')
    parser.add_argument('--server', '-s', required=True, help='server address')
    parser.add_argument('--username', '-u', required=True, help='username to sign in with into server')
    parser.add_argument('-p', required=True, help='password corresponding to the username, use @file.txt to read from file',
                        default=None)
    parser.add_argument('--project', '-P', required=True, help='project to create extracts for', default=None)
    parser.add_argument('--site', '-S', default=None)
    parser.add_argument('--timeout', '-t', type=int, help='max wait time in seconds', default=0)
    parser.add_argument('--frequency', '-f', type=int, help='check frequency in seconds', default=10)
    parser.add_argument('--hyper', '-H', required=False, help='local hyper directory (when executed on server)')
    parser.add_argument('--download', '-D', required=False, help='local download directory for hyper extracts')

    parser.add_argument('--logging-level', '-l', choices=['debug', 'info', 'error'], default='error',
                        help='desired logging level (set to error by default)')

    parser.add_argument('datasource', help='one or more datasources to refresh', nargs='+')

    args = parser.parse_args()

    if args.p is None:
        password = getpass.getpass("Password: ")
    else:
        password = args.p

    # Set logging level based on user input, or error by default
    logging_level = getattr(logging, args.logging_level.upper())
    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)s: %(message)s')

    # read in the config and updates globals
    with open(args.config, 'r') as config_file:
        config = json.load(config_file)
    with open(config['parameters']['update_values'], 'r') as updates_file:
        updates = json.load(updates_file)

    # authenticate with the Tableau server
    tableau_auth = tsc.TableauAuth(args.username, password, args.site)
    # use api version corresponding with server version
    server = tsc.Server(args.server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        # fill the projects global variable
        projects = get_projects(server)

        for ds in args.datasource:
            datasource_prepare(server, args.project, ds, args.hyper, args.download)
            update_incremental_schedule(server, args.project, ds)

            with open(config['parameters']['update_values'], 'w') as updates_file:
                json.dump(updates, updates_file, indent=2)


if __name__ == '__main__':
    main()
