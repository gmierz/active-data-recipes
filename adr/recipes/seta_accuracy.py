from __future__ import print_function, absolute_import

import json
import logging

from argparse import ArgumentParser
from collections import defaultdict

from ..recipe import RecipeParser
from ..query import run_query

log = logging.getLogger('adr')
BRANCH_WHITELIST = [
    'mozilla-inbound',
    'autoland'
]


def run(args, config):
    parser = RecipeParser('date', 'branch')
    parser.add_argument(
        '--timedistance', default=7200, type=int,
        help="Amount of time elapsed between a backout commit revision "
             "and original revision being backed out (in seconds). "
             "Times found greater than this value will be considered as a SETA failure "
             "(a backfill)."
    )

    args = parser.parse_args(args)
    query_args = vars(args)

    config.update({'url': 'http://54.149.21.8/query'})

    # Between these dates on a particular branch
    to_date = query_args['to_date']
    from_date = query_args['from_date']
    branches = query_args['branch']
    timedistance = query_args['timedistance']

    # Clean branch argument (remove default branches)
    branch = [
        b for b in branches if b in BRANCH_WHITELIST
    ]
    if len(branch) > 1:
        log.info("Too many branch names supplied. Using: " + branch[0])

    branch = branch[0]
    query_args['branch'] = branch

    # Find all backout commits, and the revisions they back out.
    backouts = next(run_query('backout_commits_in_date_range', config, **query_args))['data']

    commit_date_query_args = {
        'branch': query_args['branch'],
        'changeset': None
    }

    # For each backout commit
    results = []
    for backout_info in backouts:
        backout_time = backout_info[0]
        backout_cset = backout_info[1][:12]
        cset_backedout = backout_info[2][:12]

        commit_date_query_args['changeset'] = cset_backedout
        orig_cset_time = next(run_query('get_commit_date', config, **commit_date_query_args))['data'][0]
        orig_cset_time = orig_cset_time[0]

        # Get the distance to the original revision
        # by subtracting the backout time from the original revision time
        distance = backout_time - orig_cset_time

        # If the distance between backout and original 
        # is greater than time_distance, it is a SETA failure.
        failed = False
        if distance > timedistance:
            failed = True

        tp = (failed, backout_cset, cset_backedout)
        log.info(tp)
        results.append((failed, backout_cset, cset_backedout))

    failed = [failed for failed, bcset, csetb in results if failed]
    passed = [failed for failed, bcset, csetb in results if not failed]
    log.info('Success Rate: ' + str(100 * (len(passed)/len(results))))
    return (
        ['Failed', 'Backout Changeset', 'Changeset Backedout'],
        [
            [f for f, _, _ in results],
            [bc for _, bc, _ in results],
            [cb for _, _, cb in results]
        ]
    )
