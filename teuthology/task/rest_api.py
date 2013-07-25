import logging
import contextlib

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Start up rest_api.

    To start on on all clients::

        tasks:
        - ceph:
        - rest_api:

    To only run on certain clients::

        tasks:
        - ceph:
        - rest_api: [client.0, client.3]

    or

        tasks:
        - ceph:
        - rest_api:
            client.0:
            client.3:

    The general flow of things here is:
        1. Find clients on which rest_api is supposed to run (api_clients)
        2. Generate keyring values
        3. Start up ceph-rest-api daemons
    On cleanup:
        4. Stop the daemons
        5. Delete keyring value files.
    """
    log.info('INPUT**************************************************')
    api_clients = []
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    log.info(remotes)
    if config == None:
        for _, role_v in remotes.iteritems():
            for node in role_v:
                api_clients.append(node)
    else:
        for role_v in config:
            api_clients.append(role_v)
    log.info('OUTPUT**************************************************')
    log.info(api_clients)
    for rems, roles in remotes.iteritems():
        log.info(rems)
        log.info(roles)
        log.info('------')
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for rems, roles in remotes.iteritems():
        for id_ in roles:
            if id_ in api_clients:
                keyring = '/etc/ceph/ceph.client.rest{id}.keyring'.format(
                        id=id_)
                rems.run(
                    args=[
                        '{tdir}/adjust_ulimits'.format(tdir=testdir),
                        'ceph-coverage',
                        coverage_dir,
                        'sudo',
                        'ceph-authtool',
                        '--create-keyring',
                        '--gen-key',
                        '--name=client.rest{id}'.format(id=id_),
                        keyring,
                        run.Raw('&&'),
                        'sudo',
                        'chmod',
                        '0644',
                        keyring,
                        ],
                    )
                run_cmd = [
                    '{tdir}/adjust-ulimits'.format(tdir=testdir),
                    'ceph-coverage',
                    coverage_dir,
                    'sudo',
                    '{tdir}/daemon-helper'.format(tdir=testdir),
                    'kill',
                    'ceph-rest-api',
                    '-n',
                    'client.rest{id}'.format(id=id_), ]
                ctx.daemons.add_daemon(rems, 'client',
                        'client{id}'.format(id=id_)
                        args=run_cmd,
                        logger=log.getChild('client.rest{id}'.format(id=id_))
                        stdin=run.PIPE,
                        wait=False,
                        )
    try:
        yield

    finally:
        for rems, roles in remotes.iteritems():
       	    for id_ in roles:
                if id_ in api_clients:
                    keyring = '/etc/ceph/ceph.client.rest{id}.keyring'.format(
                            id=id_)
                    rems.run( 
                        args=[
                            '{tdir}/adjust_ulimits'.format(tdir=testdir),
                            'ceph-coverage',
                            coverage_dir,
                            'sudo',
                            'rm',
                            '-fr',
                            keyring,
                            ],
                        )
        """
        TO DO: destroy daemons started -- modify iter_daemons_of_role
        """
        log.info('DONE**************************************************')
        log.info('rest api done')
        log.info(api_clients)
