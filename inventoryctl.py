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
        self.cursor = None

        self.args = []
        self.read_settings()
        self.parse_cli_args()

        # pprint.pprint(self.args)
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
        parser_group.add_argument('-e', '--enabled', type=int, choices=[0, 1],
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
        parser_host.add_argument('-e', '--enabled', type=int, choices=[0, 1],
                                 help='Whether to enable this Host')
        parser_host.add_argument('-v', '--variable', action='append', nargs=2, metavar=('key', 'val'),
                                 help='Add/Edit multiple Variables to an entire Host, \
                                 e.g. ssh_user, ssh_password, ssh_key. \
                                 If val is `%s`, the variable will be removed.' % VAR_DEL_MARK)
        parser_host.add_argument('-d', '--delete', action='store_true',
                                 help='Delete host')
        # List Hosts && Groups
        parser_list = subparsers.add_parser('ls', help='List the Hosts or groups that match one or more patterns')
        parser_list.add_argument('-a', '--all', action='store_true', help='List all, including disabled')
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
        hostdata, groupdata = self._host_fetch()

        if hostdata is None:
            # Add new host
            self._host_add(groupdata)
        else:
            # Delete host
            if self.args.delete is True:
                if self._prompt("Are you sure you want to delete the host: %s[%s] ?! \
                                \nAfter deleting the data will not be restored!! " % (
                        hostdata['host'], hostdata['hostname'])):
                    self._host_delete(hostdata)
                else:
                    print('User canceled the operation.')
                return

            # Update host
            self._host_update(hostdata, groupdata)

    def _cmd_group(self):
        gname = self.args.name
        parent_group = self.args.parent
        groups = self._group_fetch()
        if parent_group is not None and parent_group not in groups:
            # parent does not exist
            raise Exception('The specified group[%s] does not exist' % parent_group)

        if gname not in groups:
            # Add a new group with its parent
            self._group_add(gname, groups)

        elif gname in groups:
            groupdata = groups[gname]

            # Delete host
            if self.args.delete is True:
                if self._prompt("Are you sure you want to delete the group: %s ?! \
                                \nAfter deleting the data will not be restored!! " % gname):
                    self._group_delete(groupdata)
                else:
                    print('User canceled the operation.')
                return

            # group already exists
            # Update group
            self._group_update(gname, groups)

    def _cmd_ls(self):
        # Default list all hosts and their groups info
        if self.args.group:
            self._list_groups()
        else:
            self._list_hosts()

    @property
    def __enabled(self):
        if self.args.enabled is None:
            return 1
        return self.args.enabled

    @property
    def __hostname(self):
        if self.args.name is not None:
            return self.args.name
        return self.args.host

    def _host_fetch(self):
        host = self.args.host
        group = self.args.group
        groupdata = None
        # if set group name [-g/--group]
        # convert group name to group id
        if group is not None:
            sql = "SELECT * FROM `group` WHERE `group`.`name` = '%s'"
            self.__cursor.execute(sql % group)
            groupdata = self.__cursor.fetchone()
            if groupdata is None:
                raise Exception('Group name does not exist. ', self.args.group)
            else:
                print('Assign to group: %s [id=%d]' % (groupdata['name'], groupdata['id']))

        # fetch host data
        sql = """SELECT `host`.`id`,`host`.`host`,`host`.`hostname`,`host`.`enabled`,`host`.`variables`, 
                      `group`.`id` as `group_id`,`group`.`name` as `group_name`,`group`.`enabled` as `group_enabled`, `group`.`variables` as `group_variables`   
                      FROM  (`host` LEFT JOIN `hostgroups` ON `host`.`id` = `hostgroups`.`host_id` 
                      LEFT JOIN `group` ON `hostgroups`.`group_id`=`group`.`id`) 
                      WHERE `host`.`host` = '%s';"""
        self.__cursor.execute(sql % host)
        hostdata = self.__cursor.fetchone()

        return hostdata, groupdata

    def _host_add(self, group):
        affected_rows = 0
        print('Create mode')

        # combination variables
        variables = json.dumps({item[0]: item[1] for item in self.args.variable})
        if self.args.variable is None:
            variables = None

        print('Add host: %s %s %s %d' % (self.args.host, self.__hostname, variables, self.__enabled))
        sql = """INSERT INTO `host` 
                          (`host`, `hostname`, `variables`, `enabled`) VALUES 
                          ('%s', '%s', '%s', %d);"""
        affected_rows += self.__cursor.execute(sql % (self.args.host, self.__hostname, variables, self.__enabled))
        if group is not None:
            # Add group
            lastrowid = self.__cursor.lastrowid
            sql = """INSERT INTO `hostgroups` 
                              (`host_id`, `group_id`) VALUES 
                              (%d, %d);"""
            affected_rows += self.__cursor.execute(sql % (lastrowid, group['id']))
        self.conn.commit()
        print('Affected rows: %d' % affected_rows)

    def _host_delete(self, host):
        print("Remove host from groups")
        rows = self.__cursor.execute("""DELETE FROM `hostgroups` WHERE `hostgroups`.`host_id` = %d;""" % host['id'])
        print('Affected rows: %d' % rows)

        print('Delete host: id = %d' % id)
        rows = self.__cursor.execute("""DELETE FROM `host` WHERE `host`.`id`  = %d;""" % host['id'])
        print('Affected rows: %d' % rows)

        self.conn.commit()

    def _host_update(self, host, group):
        if self.args.update:
            print('Update mode')
            affected_rows = 0

            if group is not None:
                sql = """UPDATE `hostgroups` SET `group_id` = %d WHERE `host_id` = %d;"""
                affected_rows += self.__cursor.execute(sql % (group['id'], host['id']))
                print('set group to %d' % host['group'])

            # update hostname
            if self.args.name is not None:
                # TODO Verify the name is valid
                sql = """UPDATE `host` SET `hostname` = '%s' WHERE `host`.`id` = %d;"""
                affected_rows += self.__cursor.execute(sql % (self.args.name, host['id']))
                print('set hostname to %s' % self.args.name)

            # modify variables
            if self.args.variable is not None:
                try:
                    if ast.literal_eval(host['variables']) is not None:
                        variables = json.loads(host['variables'])
                        for var in self.args.variable:
                            variables[var[0]] = var[1]
                    else:
                        variables = {item[0]: item[1] for item in self.args.variable}

                    for _k, _v in variables.copy().items():
                        if _v == VAR_DEL_MARK:
                            variables.pop(_k, None)

                    var_json = json.dumps(variables)
                    sql = """UPDATE `host` SET `variables` = '%s' WHERE `host`.`id` = %d;"""
                    affected_rows += self.__cursor.execute(sql % (var_json, host['id']))
                    print('set variables to %s' % var_json)
                except JSONDecodeError as e:
                    print(e)
                    raise Exception('Host does not have valid JSON', host['host'], host['variables'])

            # change enable state
            if host['enabled'] != self.__enabled:
                sql = """UPDATE `host` SET `enabled` = %d WHERE `host`.`id` = %d;"""
                affected_rows += self.__cursor.execute(sql % (self.__enabled, host['id']))
                print('set enabled to %d' % self.__enabled)

            # commit to db
            print('Update %s affected rows: %d' % (host['host'], affected_rows))
            self.conn.commit()
        else:
            # only show the host data
            host['group'] = group
            pprint.pprint(host)
            print(' ----- ')
            print('If you want to UPDATE this host, plz attach -U/--update argument')

    def _group_fetch(self):
        # a list store all the names to query
        group_names = [self.args.name]

        if self.args.parent is not None:
            group_names.append(self.args.parent)

        # fetch group info with name
        sql = """SELECT `child`.`name`,`child`.`id`,`child`.`variables`,`child`.`enabled`,
                      `parent`.`name`,`parent`.`id` as `parent_id`, `parent`.`variables` as `parent_variables`, 
                      `parent`.`enabled` as `parent_enabled` 
                      FROM `group` `child`
                      LEFT JOIN `childgroups` ON `child`.`id` = `childgroups`.`child_id`
                      LEFT JOIN `group` `parent` ON `childgroups`.`parent_id` = `parent`.`id`
                      WHERE `child`.`name` IN (%s);"""
        self.__cursor.execute(sql % ("'" + "','".join(map(str, group_names)) + "'"))
        groups = self.__cursor.fetchall()

        return {i['name']: i for i in groups}

    def _group_add(self, gname, groups):
        affected_rows = 0
        # add a new group
        print('Create mode')

        # combination variables
        variables = json.dumps({item[0]: item[1] for item in self.args.variable})
        if self.args.variable is None:
            variables = None

        sql = """INSERT INTO `group` (`name`, `variables`, `enabled`) 
                          VALUES ('%s', '%s', %d);"""
        affected_rows += self.__cursor.execute(sql % (gname, variables, self.__enabled))
        lastrowid = self.__cursor.lastrowid

        if self.args.parent is not None:
            # the new group has a parent
            sql = """INSERT INTO `childgroups` (`child_id`,`parent_id`) VALUES 
                              (%d,%d);"""
            affected_rows += self.__cursor.execute(sql % (lastrowid, groups[self.args.parent]['id']))
        self.conn.commit()
        print('Affected rows: %d' % affected_rows)
        print('Add group id: %d' % lastrowid)

    def _group_delete(self, group):
        print('Break the relationship between groups')
        s = """DELETE FROM `childgroups` WHERE `child_id` = %d OR `parent_id` = %d;"""
        rows = self.__cursor.execute(s % (group['id'], group['id']))
        print('Affected rows: %d' % rows)

        print('Delete group named `%s` with variables %s' % (group['name'], group['variables']))
        s = """DELETE FROM `group` WHERE `id` = %d;"""
        rows = self.__cursor.execute(s % group['id'])
        print('Affected rows: %d' % rows)
        self.conn.commit()

    def _group_update(self, gname, groups):
        affected_rows = 0
        group = groups[gname]
        parent = None
        if self.args.parent in groups:
            parent = group[self.args.parent]

        if self.args.update:
            print('Update mode')
            # modify enabled
            sql = """UPDATE `group` SET `enabled` = %d WHERE `id` = %d;"""
            affected_rows += self.__cursor.execute(sql % (self.args.enabled, group['id']))

            # modify variables
            if self.args.variable is not None:
                try:
                    if ast.literal_eval(group['variables']) is not None:
                        variables = json.loads(group['variables'])
                        for var in self.args.variable:
                            variables[var[0]] = var[1]
                    else:
                        variables = {item[0]: item[1] for item in self.args.variable}

                    for _k, _v in variables.copy().items():
                        if _v == VAR_DEL_MARK:
                            variables.pop(_k, None)

                    var_json = json.dumps(variables)
                    sql = """UPDATE `group` SET `variables` = '%s' WHERE `group`.`id` = %d;"""
                    affected_rows += self.__cursor.execute(sql % (var_json, group['id']))
                    print('set variables to %s' % var_json)
                except JSONDecodeError as e:
                    print(e)
                    raise Exception('Group does not have valid JSON', group['name'], group['variables'])

            # modify parent group
            if parent is not None:
                sql = """UPDATE `childgroups` SET `parent_id` = %d WHERE `child_id` = %d;"""
                affected_rows += self.__cursor.execute(sql % (group['id'], parent['id']))

            self.conn.commit()

        else:
            print('View mode')
            pprint.pprint(groups[g['name']])
            print(' ----- ')
            print('If you want to UPDATE this group, plz attach -U/--update argument')

    def _list_hosts(self):
        sql = """SELECT `host`.`host`,`host`.`hostname`,`host`.`variables` AS `vars`, 
              `group`.`name` AS `group`,`group`.`variables` AS `group_vars` 
              FROM `host` 
              LEFT JOIN `hostgroups` ON `host`.id = `hostgroups`.`host_id` 
              LEFT JOIN `group` ON `hostgroups`.`group_id` = `group`.`id`
              WHERE `host`.`enabled` = 1 ORDER BY `host`.`host`;"""

        if self.args.all:
            sql = """SELECT * FROM `host` 
                  LEFT JOIN `hostgroups` ON `host`.`id` = `hostgroups`.`host_id` 
                  LEFT JOIN `group` ON `hostgroups`.`group_id` = `group`.`id` 
                  ORDER BY `host`.`host`;"""

        self.__cursor.execute(sql)
        pprint.pprint(self.__cursor.fetchall())

    def _list_groups(self):
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

    @property
    def __cursor(self):
        if self.cursor is None:
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        return self.cursor


InventoryCtl()
