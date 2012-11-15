#!/usr/bin/env python
# replicaset_status.py
# Author: Tyler Stroud <ststroud@gmail.com>
# Date: 2012-11-06

"""
This script monitors replication status of a replicaset
"""

from daemon import runner
import logging
from pymongo import Connection
from time import sleep
import smtplib
from email.mime.text import MIMEText

class MongoDBReplicationStatus(object):
    poll_interval = 5
    lag_threshold = 30 # lag threshold in seconds
    log_level = logging.INFO
    last_primary = None

    def __init__(self, hostnames):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/var/log/replication_status.log'
        self.stderr_path = '/var/log/replication_status.log'
        self.pidfile_path = '/tmp/replication_status.pid'
        self.pidfile_timeout = 5

        self.hostnames = hostnames

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        self.logger_handler = logging.FileHandler('/var/log/replication_status.log')
        self.logger_handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s'))
        self.logger.addHandler(self.logger_handler)

    def get_members(self):
        """ Connect to the primary member and refresh the replica set status """
        if self.last_primary is not None:
            connection = Connection(self.last_primary)
            if connection.is_primary:
                return connection['admin'].command('replSetGetStatus')['members']

        for hostname in [h for h in self.hostnames if h != self.last_primary]:
            connection = Connection(hostname)
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

    def run(self):
        while True:
            members = self.get_members()
            for member in members:
                lag = self.get_primary_optime(members) - member['optime'].time
                if lag > self.lag_threshold:
                    notifier = Notify()
                    message = 'Member "%s" is %s seconds behind the primary' % (member['name'], lag)
                    notifier.notify_alert(message)
                    self.logger.warning('WARNING: %s' % message)
                self.logger.debug('DEBUG: Member "%s" is %s seconds behind the primary' % (member['name'], lag))
            sleep(self.poll_interval)

class Notify(object):
    smtp_host = 'localhost'
    from_email = 'from_email'
    recipient_emails = ['recipient1', 'recipient2']

    def notify_alert(self, message, subject='[ALERT] Replication Status Warning'):
        message = MIMEText(message)
        message['Subject'] = subject
        mailer = smtplib.SMTP(self.smtp_host)
        return mailer.sendmail(self.from_email, self.recipient_emails, str(message))

if __name__ == '__main__':
    status = MongoDBReplicationStatus(['host1:27017', 'host2:27017', 'host3:27017'])
    daemon_runner = runner.DaemonRunner(status)
    daemon_runner.daemon_context.files_preserve = [status.logger_handler.stream]
    daemon_runner.do_action()
