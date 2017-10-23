#!/usr/bin/env python
"""
Created on 19/10/2017

@author: askdaddy
"""
import argparse
import configparser
import os
import pprint
import sys
from json import JSONDecodeError

import pymysql

try:
    import json
except ImportError:
    import simplejson as json

VAR_DEL_MARK = 'nil'


class InventoryCtl(object):
    def __init__(self):
        self.myconfig = None
        self.facts_hostname_var = None
        self.conn = None

        self.args = []
        self.read_settings()
        self.parse_cli_args()

        self.run_command()

    def read_settings(self):
        """ Reads the settings from the mysql.ini file """

        config = configparser.ConfigParser()
        config.read(os.path.dirname(os.path.realpath(__file__)) + '/mysql.ini')

        self.myconfig = dict(config.items('server'))
        if 'port' in self.myconfig:
            self.myconfig['port'] = config.getint('server', 'port')

        self.facts_hostname_var = config.get('config', 'facts_hostname_var')

    def parse_cli_args(self):
        """ Command line argument processing """

        parser = argparse.ArgumentParser(
            description='Query or send control commands to the Ansible Dynamic Inventory based on MySQL'
        )

        # Sub commands
        subparsers = parser.add_subparsers(dest='cmd')

        # Groups
        parser_group = subparsers.add_parser('group', help='Create or Update a Group')
        parser_group.add_argument('-U', '--update', action='store_true',
                                  help='Enable Update mode. In this mode, the existing Group will be updated')
        parser_group.add_argument('-n', '--name', help='Name of the Group', required=True)
        parser_group.add_argument('-p', '--parent', help='Name of parent Group')
        parser_group.add_argument('-v', '--variable', action='append', nargs=2, metavar=('key', 'val'),
                                  help='Add multiple Variables to an entire Group')

        # Hosts
        parser_host = subparsers.add_parser('host', help='Create or Update a host')
        parser_host.add_argument('-U', '--update', action='store_true',
                                 help='Enable Update mode. In this mode, the existing Host will be updated')
        parser_host.add_argument('-n', '--name',
                                 help='Hostname of the host')
        parser_host.add_argument('-H', '--host', required=True,
                                 help='IP address or Domain of the Host')
        parser_host.add_argument('-g', '--group', default=None,
                                 help='The name of the group to join. \
                                     If not set, the Host will NOT join any group')
        parser_host.add_argument('-e', '--enabled', type=int, default=1, choices=[0, 1],
                                 help='Whether to enable this Host')
        parser_host.add_argument('-v', '--variable', action='append', nargs=2, metavar=('key', 'val'),
                                 help='Add/Edit multiple Variables to an entire Host, \
                                 e.g. ssh_user, ssh_password, ssh_key. \
                                 If val is `%s`, the variable will be removed.' % VAR_DEL_MARK)
        # List Hosts && Groups
        parser_list = subparsers.add_parser('ls', help='List the Hosts or groups that match one or more patterns')
        parser_list.add_argument('-g', '--group', action='store_true', help='Whether to show Groups or not')
        parser_list.add_argument('-r', '--regular', help='Searching condition: regular expressions,default=*',
                                 default='*')

        # Delete Host or Groups
        parser_del = subparsers.add_parser('del', help='Delete the Host or Group that with its Id')
        parser_del.add_argument('-i', '--id', type=int,
                                help='Host or Group Id', required=True)

        if len(sys.argv[1:]) == 0:
            parser.print_help()
            parser.exit()
        self.args = parser.parse_args()

    def run_command(self):
        self._connect()
        print('Command: %s' % self.args.cmd)
        if self.args.cmd == 'host':
            self._cmd_host()
        elif self.args.cmd == 'group':
            self._cmd_group()
        elif self.args.cmd == 'ls':
            self._cmd_ls()
        elif self.args.cmd == 'del':
            self._cmd_del()
        else:
            print('Invalid instruction.')
        self._disconnect()

    def _cmd_host(self):
        host = dict()

        host['host'] = self.args.host
        if self.args.name is None:
            host['hostname'] = host['host']
        else:
            host['hostname'] = self.args.name

        # Fetch the data
        cursor = self.conn.cursor(pymysql.cursors.DictCursor)

        # if set group name [-g/--group]
        # convert group name to group id
        if self.args.group is not None:
            sql = "SELECT * FROM `group` WHERE `group`.`name` = '%s'"
            cursor.execute(sql, self.args.group)
            groupdata = cursor.fetchone()
            if groupdata is None:
                raise Exception('Group name does not exist. ', self.args.group)
            else:
                print(groupdata)
                host['group'] = groupdata['id']
        else:
            host['group'] = None

        # fetch host data
        sql = """SELECT `host`.`id` as `host_id`,`host`.`host`,`host`.`hostname`,`host`.`enabled`,`host`.`variables`, 
              `group`.`id` as `group_id`,`group`.`name` as `group_name`,`group`.`enabled` as `group_enabled`, `group`.`variables` as `group_variables`   
              FROM  (`host` LEFT JOIN `hostgroups` ON `host`.`id` = `hostgroups`.`host_id` 
              LEFT JOIN `group` ON `hostgroups`.`group_id`=`group`.`id`) 
              WHERE `host`.`host` = '%s';"""
        cursor.execute(sql % host['host'])
        hostdata = cursor.fetchone()
        if hostdata is None:
            # Insert a new host
            if self.args.name:
                host[self.facts_hostname_var] = self.args.name
            # combination variables
            if self.args.variable is not None:
                host['variables'] = json.dumps({item[0]: item[1] for item in self.args.variable})

            sql = """INSERT INTO `host` 
                  (`host`, `hostname`, `variables`, `enabled`) VALUES 
                  ('%s', '%s', '%s', %d);"""
            cursor.execute(sql % (host['host'], host['hostname'], host['variables'], self.args.enabled))
            if host['group'] is not None:
                # Add group
                lastrowid = cursor.lastrowid
                sql = """INSERT INTO `hostgroups` 
                      (`host_id`, `group_id`) VALUES 
                      (%d, %d);"""
                cursor.execute(sql % (lastrowid, host['group']))
            self.conn.commit()
        else:
            # Update host
            if self.args.update:
                print('Update mode')
                affected_rows = 0

                if host['group'] is not None:
                    sql = """UPDATE `hostgroups` SET `group_id` = %d WHERE `host_id` = %d;"""
                    affected_rows += cursor.execute(sql % (host['group'], hostdata['host_id']))
                    print('set group to %d' % host['group'])

                # update hostname
                if self.args.name is not None:
                    # TODO Verify the name is valid
                    sql = """UPDATE `host` SET `hostname` = '%s' WHERE `host`.`id` = %d;"""
                    affected_rows += cursor.execute(sql % (self.args.name, hostdata['host_id']))
                    print('set hostname to %s' % self.args.name)

                # modify variables
                if self.args.variable is not None:
                    try:
                        if hostdata['variables'] is not None:
                            variables = json.loads(hostdata['variables'])
                            for var in self.args.variable:
                                variables[var[0]] = var[1]
                        else:
                            variables = {item[0]: item[1] for item in self.args.variable}

                        for _k, _v in variables.copy().items():
                            if _v == VAR_DEL_MARK:
                                variables.pop(_k, None)

                        var_json = json.dumps(variables)
                        sql = """UPDATE `host` SET `variables` = '%s' WHERE `host`.`id` = %d;"""
                        affected_rows += cursor.execute(sql % (var_json, hostdata['host_id']))
                        print('set variables to %s' % var_json)
                    except JSONDecodeError as e:
                        print(e)
                        raise Exception('Host does not have valid JSON', host['host'], host['variables'])

                # change enable state
                if hostdata['enabled'] != self.args.enabled:
                    sql = """UPDATE `host` SET `enabled` = %d WHERE `host`.`id` = %d;"""
                    affected_rows += cursor.execute(sql % (self.args.enabled, hostdata['host_id']))
                    print('set enabled to %d' % self.args.enabled)

                # commit to db
                if affected_rows != 0:
                    print('Update %s affected rows: %d' % (host['host'], affected_rows))
                    self.conn.commit()
                elif affected_rows == 0:
                    print('Nothing changed.')
            else:
                # only show the host data
                pprint.pprint(hostdata)
                print(' ----- ')
                print('If you want to UPDATE this host, plz attach -U/--update argument')

    def _cmd_group(self):
        pass

    def _cmd_ls(self):
        pass

    def _cmd_del(self):
        pass

    def _connect(self):
        if not self.conn:
            self.conn = pymysql.connect(**self.myconfig)

    def _disconnect(self):
        if self.conn:
            self.conn.close()


InventoryCtl()
