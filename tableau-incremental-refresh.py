import argparse
import getpass
import json
import logging
import os
import signal
import sys
import time
from datetime import time
from pathlib import Path
import zipfile

import jaydebeapi as db
import tableauserverclient as tsc
from tableaudocumentapi import Datasource
from tableauhyperapi import HyperProcess, Telemetry, Connection, TableName
from tableauserverclient import ConnectionCredentials, HourlyInterval

# globals
from utils import datasource_quote_date

WORK_DIR = "work"

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
    - the two distinct minimum values of the functional ordered column after the last seen update value
    - the last update value as already available in the database reference table
    """
    with database_connect(database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"""select distinct {config['datasources'][datasource]['functional_ordered_column']} 
                                from {config['datasources'][datasource]['reference_table']} 
                                where {config['parameters']['update_datetime_column']} > {update_value}
                                order by {config['datasources'][datasource]['functional_ordered_column']}
                                limit 2""")
            result_rows = cursor.fetchall()
            functional_ordered_column_type = cursor.description[0][1]
            functional_ordered_column_value_min = result_rows[0][0]
            if len(result_rows) > 1:
                functional_ordered_column_value_previous = result_rows[1][0]
            else:
                if functional_ordered_column_type in {db.DATE, db.TIME, db.DATETIME, db.STRING, db.TEXT}:
                    functional_ordered_column_value_previous = ''
                else:
                    functional_ordered_column_value_previous = -sys.maxsize - 1
            if functional_ordered_column_type in {db.DATE, db.TIME, db.DATETIME, db.STRING, db.TEXT}:
                functional_ordered_column_value_min = f"'{functional_ordered_column_value_min}'"
                functional_ordered_column_value_previous = f"'{functional_ordered_column_value_previous}'"
            cursor.execute(f"""select max({config['parameters']['update_datetime_column']}) 
                                from {config['datasources'][datasource]['reference_table']}""")
            result_rows = cursor.fetchall()
            last_update_value = result_rows[0][0]
    return functional_ordered_column_value_min, functional_ordered_column_value_previous, last_update_value


def hyper_prepare(hyper_path, functional_ordered_column, column_value):
    """Function that prepares the given hyper file: based on the hyper's path, the functional ordered column and its value,
     the hyper file is cleaned from the latest set of data by deleting all data with the given column value or greater values
     """
    path_to_database = Path(hyper_path).expanduser().resolve()
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, user_agent=os.path.basename(__file__)) as hyper:
        with Connection(endpoint=hyper.endpoint, database=path_to_database) as connection:
            table_name = TableName("Extract", "Extract")
            rows_affected = connection.execute_command(command=f'DELETE FROM {table_name} WHERE "{functional_ordered_column}" >= {column_value}')
            return rows_affected


def datasource_prepare(server, project, ds):
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
            ds_file = server.datasources.download(datasource.id, filepath=WORK_DIR, include_extract=True)
            if zipfile.is_zipfile(ds_file):
                with zipfile.ZipFile(ds_file) as zf:
                    zf.extractall()
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

            update_value = updates['datasources'][ds]['last_update_value']
            if update_value is None or update_value == "":
                logging.error(f"datasource {ds} does not have a last update value set, please provide one")
                return

            functional_ordered_column_value_min, functional_ordered_column_value_previous, last_update_value = get_database_values(database, ds, update_value)
            hyper_file = tds.extract.connection.dbname
            rows_affected = hyper_prepare(hyper_file, config['datasources'][ds]['functional_ordered_column'],
                                          functional_ordered_column_value_min)
            logging.info(f"datasource {ds} with hyper file {hyper_file}: {rows_affected} rows were deleted")
            tds.extract.refresh.refresh_events[-1].increment_value = datasource_quote_date(functional_ordered_column_value_previous)
            tds.save_as(ds_file)
            credentials = ConnectionCredentials(config['databases'][database]['args']['user'], config['databases'][database]['args']['password'], embed=True)
            new_ds = tsc.DatasourceItem(p.id)
            new_ds.name = ds
            server.datasources.publish(new_ds, ds_file, mode=tsc.Server.PublishMode.Overwrite, connection_credentials=credentials)
            updates['datasources'][ds]['last_update_value'] = last_update_value
            # incremental refreshes can not be done yet through the API   |-(
            # try:
            #     job = (datasource, server.datasources.refresh(datasource))
            # except ServerResponseError as e:
            #     logging.error("exception while processing [{1}]: {0}".format(str(e), ds))
            #     return None
            # return job


def get_schedules(server, project, ds):
    schedule_to_get = None
    for s in tsc.Pager(server.schedules):
        logging.info(f"schedule id: {s.id}, schedule name: {s.name}")
        if s.name == "Test2 RKO":
            schedule_to_get = s
    interval_item = HourlyInterval(start_time=time(23, 45), end_time=time(23, 00), interval_value=1)
    # this works, but you cannot set the refresh type
    #server.schedules.add_to_schedule(schedule_id=schedule.id, datasource=datasource)
    schedule_to_get.interval_item = interval_item
    server.schedules.update(schedule_item=schedule_to_get)
    pass


def wait_for_jobs(server, jobs, timeout, frequency):
    """Procedure that waits for the registered extract refresh jobs to finish on the Tableau server"""
    signal.alarm(timeout)
    n = 0
    while n < len(jobs):
        time.sleep(frequency)
        running_jobs = server.jobs
        if running_jobs is None:
            logging.debug("no jobs returned, assuming all jobs are done")
            n = len(jobs)
        else:
            n = 0
            for id in jobs.keys():
                logging.debug("checking job id: {0}".format(id))
                job = running_jobs.get(jobs[id][1].id)
                if job is None:
                    n += 1  # assume job is done
                else:
                    logging.debug("checking job for datasource: {0}, finish code: {1}".format(jobs[id][0].name,
                                                                                              job.finish_code))
                    if job.finish_code == '1':
                        raise RuntimeError("refresh job exited unexpectedly for datasource {}".format(jobs[id][0].name))
                    if job.finish_code == '0':
                        n += 1
    logging.debug(f"all datasources have been refreshed")
    signal.alarm(0)


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
    parser.add_argument('--wait', '-w', action='store_true', help='wait for the refresh to finish', default=None)
    parser.add_argument('--timeout', '-t', type=int, help='max wait time in seconds', default=0)
    parser.add_argument('--frequency', '-f', type=int, help='check frequency in seconds', default=10)

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
    logging.basicConfig(level=logging_level)

    with open(args.config, 'r') as f:
        config = json.load(f)
    with open(config['parameters']['update_values'], 'r') as f:
        updates = json.load(f)

    signal.signal(signal.SIGALRM, handler)

    # authenticate with the Tableau server
    tableau_auth = tsc.TableauAuth(args.username, password, args.site)
    # use api version corresponding with server version
    server = tsc.Server(args.server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        projects = get_projects(server)

        jobs = dict()
        for ds in args.datasource:
            get_schedules(server, args.project, ds)
            datasource_prepare(server, args.project, ds)
            # jobs[job.id] = (datasource, job)

        if args.wait:
            wait_for_jobs(server, jobs, args.timeout, args.frequency)

    with open(config['parameters']['update_values'], 'w') as f:
        json.dump(updates, f, indent=2)


if __name__ == '__main__':
    main()
