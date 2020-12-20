import argparse
import tableauserverclient as TSC
from tableauserverclient import ServerResponseError
import tableauhyperapi as TH
import logging
import signal
from tableaudocumentapi import Datasource


def handler(signum, frame):
    raise RuntimeError("timeout waiting for jobs to finish")


def main():
    # lees configuratie uit, met naam van update kolom, functionele ID, datasource naam, vorige_update_datum
    # lees database config uit bestand? of gewoon ook op de command-line? jdbc url, user, passwd

    parser = argparse.ArgumentParser(description='perform incremental refresh on datasources',
                                     fromfile_prefix_chars='@')
    parser.add_argument('--server', '-s', required=True, help='server address')
    parser.add_argument('--username', '-u', required=True, help='username to sign in with into server')
    parser.add_argument('-p', required=True, help='password corresponding to the username, use @file.txt to read from file',
                        default=None)
    parser.add_argument('--project', '-P', required=True, help='project to create extracts for', default=None)
    parser.add_argument('--site', '-S', default=None)
    parser.add_argument('-w', action='store_true', help='wait for the refresh to finish', default=None)
    parser.add_argument('-m', type=int, help='max wait time in seconds', default=0)
    parser.add_argument('-f', type=int, help='check frequency in seconds', default=10)

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

    signal.signal(signal.SIGALRM, handler)

    tableau_auth = TSC.TableauAuth(args.username, password, args.site)
    # use api version corresponding with server version
    server = TSC.Server(args.server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        for ds in TSC.Pager(server.datasources):
            logging.debug("{0} ({1})".format(ds.name, ds.project_name))
            if ds.name in args.datasource and ds.project_name == args.project:
                logging.info("{0}: {1}".format(ds.name, ds.project_name, ds.id))
                ds_file = server.datasources.download(ds.id, include_extract=False)
                tds = Datasource.from_file(ds_file)
                pass
                try:
                    jobs[ds.id] = (ds, server.datasources.refresh(ds))
                except ServerResponseError as e:
                    logging.error("exception while processing [{1}]: {0}".format(str(e), ds.name))
        if args.w:
            signal.alarm(args.m)
            n = 0
            while n < len(jobs):
                time.sleep(args.f)
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
                                raise RuntimeError(
                                    "refresh job exited unexpectedly for datasourse {}".format(jobs[id][0].name))
                            if job.finish_code == '0':
                                n += 1
            logging.debug("all jobs are finished")
            signal.alarm(0)


if __name__ == '__main__':
    main()
