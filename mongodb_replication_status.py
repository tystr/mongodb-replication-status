#!/usr/bin/env python
# mongodb_replicaset_status.py
# Author: Tyler Stroud <ststroud@gmail.com>
# Date: 2012-11-06

"""
This script monitors replication status of a replicaset
"""

from daemon import runner
import logging
from pymongo import Connection
from pymongo.errors import AutoReconnect
from time import sleep
import smtplib
from email.mime.text import MIMEText
import sys
from argparse import ArgumentParser
from ConfigParser import RawConfigParser, NoOptionError

class MongoDBReplicationStatus(object):
    last_primary = None
    def __init__(self, host, poll_interval=5, lag_threshold=30,
                 max_connect_retries=5, log_level=logging.INFO,
                 pidfile='/tmp/mongodb_replication_status.pid',
                 logfile='/var/log/mongodb_replication_status.log'):

        self.poll_interval = poll_interval
        self.lag_threshold = lag_threshold
        self.max_connect_retries = max_connect_retries

        self.stdin_path = '/dev/null'
        self.stdout_path = logfile
        self.stderr_path = logfile
        self.pidfile_path = pidfile
        self.pidfile_timeout = 5

        self.hostnames = host

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.getLevelName(log_level))
        self.logger_handler = logging.FileHandler(logfile)
        self.logger_handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
        self.logger.addHandler(self.logger_handler)

    def set_notifier(self, notifier):
        assert isinstance(notifier, Notifier), ('"notifier" must be an instance'
                                             'of "Notifier"')
        self.notifier = notifier

    def get_members(self):
        """ Connect to the primary member and refresh the replica set status """
        if self.last_primary is not None:
            connection = self.get_connection(self.last_primary)
            if connection is not None and connection.is_primary:
                return connection['admin'].command('replSetGetStatus')['members']

        for hostname in [h for h in self.hostnames if h != self.last_primary]:
            connection = self.get_connection(hostname)
            if not isinstance(connection, Connection):
                continue # failed to connect to the current iteration's hostname, so continue and try the next hostname

            if connection.is_primary:
                self.last_primary = hostname
                return connection['admin'].command('replSetGetStatus')['members']

        # There is no primary, so wait 5 seconds and try again
        sleep(5)
        return self.get_members()

    def get_primary_optime(self, members):
        """ Returns the optime of the primary member """
        for member in members:
            if 'PRIMARY' == member['stateStr']:
                return member['optime'].time

    def get_connection(self, hostname):
        """ Attempt to create a mongodb Connection to the given hostname """
        retries = self.max_connect_retries
        while retries > 0:
            try:
                return Connection(hostname)
            except AutoReconnect:
                self.logger.warning(
                    'WARNING: Failed to connect to hostname "%s". Trying again in 5 seconds. (%s tries left).'
                    % (hostname, retries))
                retries -= 1
                sleep(5)

        errmsg = 'ERROR: All %s attempts to connect to hostname "%s" failed. Host may be down.'\
                 % (self.max_connect_retries, hostname)
        self.logger.error(errmsg)
        self.notifier.send_to_all(errmsg, '[ALERT] Host %s may be down' % hostname)

    def run(self):
        while True:
            members = self.get_members()
            message = ''
            for member in members:
                lag = self.get_primary_optime(members) - member['optime'].time
                if lag > self.lag_threshold:
                    message += 'WARNING: Member "%s" is %s seconds behind the primary\n' % (member['name'], lag)
                    self.logger.warning(message)
                self.logger.debug('DEBUG: Member "%s" is %s seconds behind the primary' % (member['name'], lag))
            if message is not None:
                self.notifier.send_to_all(message)
            sleep(self.poll_interval)

class Notifier(object):
    def __init__(self, from_email, recipient_emails, smtp_host='localhost'):
        self.from_email = from_email
        self.recipient_emails = recipient_emails
        self.smtp_host = smtp_host

    def send_to_all(self, message, subject='[ALERT] Replication Status Warning'):
        message = MIMEText(message)
        message['Subject'] = subject
        mailer = smtplib.SMTP(self.smtp_host)
        return mailer.sendmail(self.from_email, self.recipient_emails, str(message))

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-c', '--config',help='The path to the configuration file', dest='FILE', required=True)
    parser.add_argument('action', choices=('start', 'stop', 'restart'))
    args = parser.parse_args()

    config_parser = RawConfigParser()
    config_file = open(args.FILE)
    try:
        config_parser.readfp(config_file)
    finally:
        config_file.close()

    status = MongoDBReplicationStatus(
        config_parser.get('main', 'host').split(','),
        config_parser.getint('main', 'poll_interval'),
        config_parser.getint('main', 'lag_threshold'),
        config_parser.getint('main', 'max_connect_retries'),
        config_parser.get('main', 'log_level'),
        config_parser.get('main', 'pidfile'),
        config_parser.get('main', 'logfile'),
    )
    notifier = Notifier(config_parser.get('main', 'from_email'),
                      config_parser.get('main', 'recipients'),
                      config_parser.get('main', 'smtp_host'))
    status.set_notifier(notifier)

    sys.argv = sys.argv[0], args.action # overwrite sys.argv to be what daemon_runner expects
    daemon_runner = runner.DaemonRunner(status)
    daemon_runner.daemon_context.files_preserve = [status.logger_handler.stream]
    daemon_runner.do_action()
