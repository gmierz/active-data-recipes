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


def run(args, config, return_results=False):
    parser = RecipeParser('date', 'branch')
    parser.add_argument(
        '--timedistance', default=7200, type=int,
        help="Amount of time elapsed between a backout commit revision "
             "and original revision being backed out (in seconds). "
             "Times found greater than this value will be considered as a SETA failure "
             "(a backfill)."
    )
    parser.add_argument(
        '--maxchangesets', default=None, type=int,
        help="Maximum number of changesets to process."
    )

    args = parser.parse_args(args)
    query_args = vars(args)

    config.update({'url': 'http://54.149.21.8/query'})

    # Between these dates on a particular branch
    to_date = query_args['to_date']
    from_date = query_args['from_date']
    branches = query_args['branch']
    timedistance = query_args['timedistance']
    maxchangesets = query_args['maxchangesets']

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

    commits = [info[2] for info in backouts]
    commit_date_query_args = {
        'branch': query_args['branch'],
        'changesets': commits
    }
    orig_cset_times = next(run_query('get_commit_date', config, **commit_date_query_args))['data']
    orig_cset_times = {
        cset[:12]: time
        for time, cset, _ in orig_cset_times
    }

    # For each backout commit
    results = []
    for count, backout_info in enumerate(backouts):
        if maxchangesets and count >= maxchangesets:
            break

        backout_time = backout_info[0]
        backout_cset = backout_info[1][:12]
        cset_backedout = backout_info[2][:12]

        orig_cset_time = orig_cset_times[cset_backedout]

        # Get the distance to the original revision
        # by subtracting the backout time from the original revision time
        distance = backout_time - orig_cset_time

        # If the distance between backout and original 
        # is greater than time_distance, it is a SETA failure.
        failed = False
        if distance > timedistance:
            failed = True

        tp = (failed, backout_cset, cset_backedout)
        log.info("Result for changeset (" + str(count) + "): " + str(tp))
        results.append((failed, backout_cset, cset_backedout))

    failed = [failed for failed, bcset, csetb in results if failed]
    passed = [failed for failed, bcset, csetb in results if not failed]
    success_rate = 100 * (len(passed)/len(results))

    if not return_results:
        return (
            ['success Rate', '# Of Passing', '# Of Failed', 'Total Changesets Analyzed'],
            [success_rate, len(passed), len(failed), len(results)]
        )
    return results
