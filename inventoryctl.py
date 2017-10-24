#!/usr/bin/env python
"""
Created on 19/10/2017

@author: askdaddy
"""
import argparse
import ast
import configparser
import os
import pprint
import sys
from distutils.util import strtobool
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
        parser_group.add_argument('-n', '--name', required=True,
                                  help='Name of the Group')
        parser_group.add_argument('-p', '--parent',
                                  help='Name of parent Group')
        parser_group.add_argument('-e', '--enabled', type=int, default=1, choices=[0, 1],
                                  help='Whether to enable this Group')
        parser_group.add_argument('-v', '--variable', action='append', nargs=2, metavar=('key', 'val'),
                                  help='Add multiple Variables to an entire Group \
                                  If val is `%s`, the variable will be removed.' % VAR_DEL_MARK)

        # Hosts
        parser_host = subparsers.add_parser('host', help='Add , Update or Delete a host')
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
        parser_host.add_argument('-d', '--delete', action='store_true',
                                 help='Delete host')
        # List Hosts && Groups
        parser_list = subparsers.add_parser('ls', help='List the Hosts or groups that match one or more patterns')
        parser_list.add_argument('-g', '--group', action='store_true', help='Whether to show Groups or not')
        parser_list.add_argument('-r', '--regular', help='Searching condition: regular expressions,default=*',
                                 default='*')

        if len(sys.argv[1:]) == 0:
            parser.print_help()
            parser.exit()
        self.args = parser.parse_args()

    def run_command(self):
        self._connect()
        print('[Command]: %s' % self.args.cmd)
        if self.args.cmd == 'host':
            self._cmd_host()
        elif self.args.cmd == 'group':
            self._cmd_group()
        elif self.args.cmd == 'ls':
            self._cmd_ls()
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

        def _delete(id):
            print("Remove host from groups")
            rows = cursor.execute("""DELETE FROM `hostgroups` WHERE `hostgroups`.`host_id` = %d;""" % id)
            print('Affected rows: %d' % rows)

            print('Delete host: id = %d' % id)
            rows = cursor.execute("""DELETE FROM `host` WHERE `host`.`id`  = %d;""" % id)
            print('Affected rows: %d' % rows)

            self.conn.commit()

        # if set group name [-g/--group]
        # convert group name to group id
        if self.args.group is not None:
            sql = "SELECT * FROM `group` WHERE `group`.`name` = '%s'"
            cursor.execute(sql % self.args.group)
            groupdata = cursor.fetchone()
            if groupdata is None:
                raise Exception('Group name does not exist. ', self.args.group)
            else:
                print('Assign to group: %s [id=%d]' % (groupdata['name'], groupdata['id']))
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
            affected_rows = 0
            # combination variables
            if self.args.variable is not None:
                host['variables'] = json.dumps({item[0]: item[1] for item in self.args.variable})
            else:
                host['variables'] = None

            print('Add host: %s %s %s %d' % (host['host'], host['hostname'], host['variables'], self.args.enabled))
            sql = """INSERT INTO `host` 
                  (`host`, `hostname`, `variables`, `enabled`) VALUES 
                  ('%s', '%s', '%s', %d);"""
            affected_rows += cursor.execute(
                sql % (host['host'], host['hostname'], host['variables'], self.args.enabled))
            if host['group'] is not None:
                # Add group
                lastrowid = cursor.lastrowid
                sql = """INSERT INTO `hostgroups` 
                      (`host_id`, `group_id`) VALUES 
                      (%d, %d);"""
                affected_rows += cursor.execute(sql % (lastrowid, host['group']))
            self.conn.commit()
            print('Affected rows: %d' % affected_rows)
        else:
            # Delete host
            if self.args.delete is True:
                if self._prompt("Are you sure you want to delete the host: %s[%s] ?! \
                                \nAfter deleting the data will not be restored!! " % (
                        hostdata['host'], hostdata['hostname'])):
                    _delete(hostdata['host_id'])
                else:
                    print('User canceled the operation.')
                return

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
                        if ast.literal_eval(hostdata['variables']) is not None:
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
        g = dict()
        g['name'] = self.args.name

        # a list store all the names to query
        group_names = [g['name']]

        if self.args.parent is not None:
            group_names.append(self.args.parent)

        cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        # fetch group info with name
        sql = """SELECT `child`.`name`,`child`.`id`,`child`.`variables`,`child`.`enabled`,
              `parent`.`name`,`parent`.`id` as `parent_id`, `parent`.`variables` as `parent_variables`, 
              `parent`.`enabled` as `parent_enabled` 
              FROM `group` `child`
              LEFT JOIN `childgroups` ON `child`.`id` = `childgroups`.`child_id`
              LEFT JOIN `group` `parent` ON `childgroups`.`parent_id` = `parent`.`id`
              WHERE `child`.`name` IN (%s);"""
        cursor.execute(sql % ("'" + "','".join(map(str, group_names)) + "'"))
        groupdata = cursor.fetchall()

        groups = {i['name']: i for i in groupdata}
        if self.args.parent is not None and self.args.parent not in groups:
            # parent does not exist
            raise Exception('The specified group[%s] does not exist' % self.args.parent)

        if g['name'] not in groups:
            affected_rows = 0
            # add a new group
            print('Create mode')

            # combination variables
            if self.args.variable is not None:
                g['variables'] = json.dumps({item[0]: item[1] for item in self.args.variable})
            else:
                g['variables'] = None

            sql = """INSERT INTO `group` (`name`, `variables`, `enabled`) 
                  VALUES ('%s', '%s', %d);"""
            affected_rows += cursor.execute(sql % (g['name'], g['variables'], self.args.enabled))
            lastrowid = cursor.lastrowid

            if self.args.parent is not None:
                # the new group has a parent
                sql = """INSERT INTO `childgroups` (`child_id`,`parent_id`) VALUES 
                      (%d,%d);"""
                affected_rows += cursor.execute(sql % (lastrowid, groups[self.args.parent]['id']))
            self.conn.commit()
            print('Affected rows: %d' % affected_rows)
            print('Add group id: %d' % lastrowid)

        elif g['name'] in groups:
            gdata = groups[g['name']]
            affected_rows = 0

            pprint.pprint('%s already exists' % g['name'])
            # group already exists
            if self.args.update:
                print('Update mode')
                # modify enabled
                sql = """UPDATE `group` SET `enabled` = %d WHERE `id` = %d;"""
                affected_rows += cursor.execute(sql % (self.args.enabled, gdata['id']))

                # modify variables
                if self.args.variable is not None:
                    try:
                        if ast.literal_eval(gdata['variables']) is not None:
                            variables = json.loads(gdata['variables'])
                            for var in self.args.variable:
                                variables[var[0]] = var[1]
                        else:
                            variables = {item[0]: item[1] for item in self.args.variable}

                        for _k, _v in variables.copy().items():
                            if _v == VAR_DEL_MARK:
                                variables.pop(_k, None)

                        var_json = json.dumps(variables)
                        sql = """UPDATE `group` SET `variables` = '%s' WHERE `group`.`id` = %d;"""
                        affected_rows += cursor.execute(sql % (var_json, gdata['id']))
                        print('set variables to %s' % var_json)
                    except JSONDecodeError as e:
                        print(e)
                        raise Exception('Group does not have valid JSON', gdata['name'], gdata['variables'])
                # modify parent group
                if self.args.parent is not None:
                    sql = """UPDATE `childgroups` SET `parent_id` = %d WHERE `child_id` = %d;"""
                    affected_rows += cursor.execute(sql % (gdata['id'], groups[self.args.parent]['id']))

                self.conn.commit()

            else:
                print('View mode')
                pprint.pprint(groups[g['name']])
                print(' ----- ')
                print('If you want to UPDATE this group, plz attach -U/--update argument')

    def _cmd_ls(self):
        pass

    def _connect(self):
        if not self.conn:
            self.conn = pymysql.connect(**self.myconfig)

    def _disconnect(self):
        if self.conn:
            self.conn.close()

    def _prompt(self, query):
        sys.stdout.write('''%s [y/n]: ''' % query)
        val = input()
        try:
            ret = strtobool(val)
        except ValueError:
            sys.stdout.write('Please answer with a y/n\n')
            return self._prompt(query)
        return ret


InventoryCtl()
