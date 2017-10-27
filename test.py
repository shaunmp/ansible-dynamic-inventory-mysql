"""
Created on 26/10/2017

@author: seven
"""
import collections
import pprint

import pymysql


# def construct_trees_by_TingYu(edges):
#     """Given a list of edges [child, parent], return trees. """
#     trees = collections.defaultdict(dict)
#
#     for child, parent in edges:
#         print(child, parent)
#         if child is not None:
#             trees[parent][child] = trees[child]
#         else:
#             trees[parent] = {}
#
#     print(' trees -----')
#     pprint.pprint(trees)
#     # Find roots
#     children, parents = zip(*edges)
#
#     child_set = set(children)
#     pprint.pprint(child_set)
#     parent_set = set(parents)
#     pprint.pprint(parent_set)
#
#     # roots = set(parents).difference(children)
#     roots = child_set ^ parent_set
#     print(' roots ----- ')
#     pprint.pprint(roots)
#
#     return {root: trees[root] for root in roots}


def construct_group_trees(groups):
    trees = collections.defaultdict(dict)

    for child, parent in groups:

        if parent is not None:
            parent_k = parent['name']

            if 'children' not in trees[parent_k]:
                trees[parent_k]['children'] = list()
            trees[parent_k]['children'].append(child)
        else:
            child_k = child['name']
            if child_k not in trees:
                trees[child_k] = child

    pprint.pprint(trees)
    # Find roots
    children, parents = map(list, zip(*groups))

    roots = [children[k] for k, v in enumerate(parents) if v is None]
    pprint.pprint(roots)

    return {root['name']: trees[root['name']] for root in roots}


def test():
    conn = None
    if not conn:
        conn = pymysql.connect(host='localhost', user='root', passwd='123456', port=6603, db='ansible_inventory')
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("""SELECT `gc`.`name` as `child`,`gc`.`variables` as `c_vars`,
`gp`.`name` as `parent` ,`gp`.`variables` as `p_vars`
FROM `group` `gc`
              LEFT JOIN `childgroups` ON `gc`.`id` = `childgroups`.`child_id`
              LEFT JOIN `group` `gp` ON `childgroups`.`parent_id` = `gp`.`id`
              ORDER BY `parent`
              """)
    groupsdata = cursor.fetchall()

    groups = [[{'name': g['child'], 'vars': g['c_vars']},
               {'name': g['parent'], 'vars': g['p_vars']} if g['parent'] is not None else None] for g in groupsdata]
    return construct_group_trees(groups)


if __name__ == '__main__':
    pprint.pprint(test())
    # edges = [[0, 6], [17, 5], [2, 7], [4, 14], [12, 9], [15, 5], [11, 1], [14, 8], [16, 6], [5, 1], [10, 7], [6, 10],
    #          [8, 2], [13, 1], [1, 12], [7, 1], [3, 2], [19, 12], [18, 19], [None, 20]]
    # res = construct_trees_by_TingYu(edges)
    # print(' ===== ')
    # pprint.pprint(res)
