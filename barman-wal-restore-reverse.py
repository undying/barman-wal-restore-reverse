#! /usr/bin/env python

from __future__ import print_function
import sys
import os

import tempfile
import argparse
import subprocess


DEFAULT_REMOTE_USER = 'barman'
DEFAULT_WAL_DIR = '/var/tmp/barman-wal-restore'


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--destination',
        default=DEFAULT_WAL_DIR,
        help='WAL destination directory. It can be Barman spool or postgres pg_wal directory. Defaults to "%(default)s".'
    )
    parser.add_argument(
        '-U', '--user',
        default=DEFAULT_REMOTE_USER,
        help='The user used for the ssh connection to the Barman server. Defaults to "%(default)s".'
    )
    parser.add_argument(
        'barman_host',
        metavar='BARMAN_HOST',
        help='The server name configured in Barman from which WALs are taken.'
    )
    parser.add_argument(
        'server_name',
        metavar='SERVER_NAME',
        help='The server name configured in Barman from which WALs are taken.'
    )
    parser.add_argument(
        'backup_id',
        metavar='BACKUP_ID',
        help='The ID of backup in Barman.'
    )

    return parser.parse_args(args=args)


def build_ssh_command(config):
    ssh_command = [
        'ssh',
        '%s@%s' % (config.user, config.barman_host),
        'barman'
    ]

    return ssh_command


def get_last_wal(config):
    match = 'Last available'
    match_len = len(match)

    ssh_command = (
        'ssh',
        '%s@%s' % (config.user, config.barman_host),
        'barman',
        'show-backup',
        '%s' % config.server_name,
        '%s' % config.backup_id
    )

    status = subprocess.Popen(
        ssh_command,
        stdout=subprocess.PIPE
    )

    with status.stdout:
        for line in status.stdout.readlines():
            l = line.strip().decode('utf-8')
            if len(l) < match_len:
                continue

            if l[:match_len] == match:
                return l.split()[-1]

    raise AssertionError()


def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def wal_split(wal):
    return (wal[:16], wal[16:])


def int_to_hex(num, size=0):
    return hex(num)[2:].rstrip('L').zfill(size).upper()


def hex_to_int(num):
    return int(num, 16)


def wal_decr(wal):
    major_hex, minor_hex = wal_split(wal)
    minor = hex_to_int(minor_hex)
    if minor > 0:
        minor_hex = int_to_hex(minor-1, 8)
    else:
        major_hex = int_to_hex(hex_to_int(major_hex) - 1, 16)
        minor_hex = int_to_hex(255, 8)

    return "%s%s" % (major_hex, minor_hex)


def get_wal(config, wal, wal_temp):
    ssh_command = (
        'ssh',
        '%s@%s' % (config.user, config.barman_host),
        'barman',
        'get-wal',
        '%s' % config.server_name,
        '%s' % wal
    )

    ssh_process = subprocess.Popen(
        ssh_command,
        stdout=wal_temp[0]
    )
    ssh_process.communicate()
    assert(ssh_process.returncode == 0)

    wal_dst = os.path.join(config.destination, wal)
    assert(os.path.getsize(wal_temp[1]) > 0)
    assert(not os.path.isfile(wal_dst))

    os.rename(wal_temp[1], wal_dst)
    print_err('Got %s' % wal)


def main():
    config = parse_args()
    assert(os.path.isdir(config.destination))

    print_err("Searching for last WAL number...")
    last_wal = get_last_wal(config)
    print_err("Found: %s" % last_wal)

    print_err("Calculating previous WAL...")
    next_wal = wal_decr(last_wal)
    print_err("Next WAL is: %s" % next_wal)

    while not os.path.isfile(os.path.join(config.destination, next_wal)):
        try:
            wal_temp = tempfile.mkstemp(dir=config.destination)
            get_wal(config, next_wal, wal_temp)
        except (KeyboardInterrupt, SystemExit):
            os.unlink(wal_temp[1])
            sys.exit()

        next_wal = wal_decr(next_wal)

    print_err("WAL already exists: %s" % next_wal)


if __name__ == '__main__':
    main()

