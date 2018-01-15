#!/usr/bin/python3.5

from __future__ import print_function
import hvac
import argparse
import getpass
import json
import sys

default_vault_server = 'https://bcon.prod.wow.avvo.com:8200'
vault_root_path = 'services/jobrunner/credentials/secret/'

def log_console(*args, **kwargs):
    print(__file__ + ': ', file=sys.stderr, end='')
    print(*args, file=sys.stderr, **kwargs)

def get_client(vault_server=default_vault_server, vault_appid=None):
    client = hvac.Client(url=vault_server, verify=False)
    vault_user = getpass.getuser()

    log_console('Connecting [%s] to vault server [%s] using %s' %
        (vault_user, vault_server, 'ldap' if vault_appid is None else 'appid'))
    if vault_appid is None:
        client.auth_ldap(vault_user, getpass.getpass(prompt='Please enter your password to connect to vault: '))
    else:
        client.auth_app_id(vault_appid, vault_user)

    log_console('Vault key root path: [%s]' % vault_root_path)
    return client

def read_key(client, key, showToConsole=False):
    log_console('Reading key [%s] from vault' % key)
    secret_value = client.read(vault_root_path + key)
    value = secret_value['data']['value']
    if showToConsole:
        print(value)
    return value

def write_key(client, key, value):
    log_console('Writing key [%s] with value [%s] into vault' % (key, value))
    client.write(vault_root_path + key, value=value)
    log_console('Done')

def delete_key(client, key):
    log_console('Deleting key [%s] from vault' % key)
    client.delete(vault_root_path + key)
    log_console('Done')

def list_key(client):
    log_console('Listing keys from vault')
    print(client.list(vault_root_path))
    log_console('Done')

def vault_action(args, vault_server):
    client = None
    try:
        if args.action == 'read':
            client = get_client(vault_server, vault_appid=args.appid)
            read_key(client, args.key, showToConsole=True)
        elif args.action == 'write':
            client = get_client(vault_server, vault_appid=args.appid)
            write_key(client, args.key, args.value)
        elif args.action == 'delete':
            client = get_client(vault_server, vault_appid=args.appid)
            delete_key(client, args.key)
        elif args.action == 'list':
            client = get_client(vault_server, vault_appid=args.appid)
            list_key(client)
        else:
            log_console('Invalid action, only support read/write/delete')
            exit(1)

    finally:
        if client is not None:
            client.logout()

if __name__ == '__main__':
    """This script allows easier access to vault from cluster/gateway"""

    parser = argparse.ArgumentParser(description = 'Read, write/update, or delete vault key used by the data team')
    parser.add_argument('--vault_server', required=False, default=default_vault_server, help='the vault server to connect to')
    parser.add_argument('--appid', required=False, default=None, help='the vault app id to be used instead of ldap')

    subparsers = parser.add_subparsers(help='vault actions')
    subparsers.required = True
    subparsers.dest = 'action'

    # create the parser for the 'read' command
    parser_read = subparsers.add_parser('read', help='read a key from vault store')
    parser_read.add_argument('--key', required=True, help='the vault key to be operated on')

    # create the parser for the 'write' command
    parser_write = subparsers.add_parser('write', help='write a key to vault store')
    parser_write.add_argument('--key', required=True, help='the vault key to be operated on')
    parser_write.add_argument('--value', required=True, help='value of the vault key to be written')

    # create the parser for the 'delete' command
    parser_delete = subparsers.add_parser('delete', help='delete a key from vault store')
    parser_delete.add_argument('--key', required=True, help='the vault key to be operated on')

    # create the parser for the 'list' command
    parser_list = subparsers.add_parser('list', help='list all entries under given key from vault store')

    args = parser.parse_args()

    vault_action(args, args.vault_server)
