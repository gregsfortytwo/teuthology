import contextlib
import logging

from orchestra import run
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def default_image_name(role):
    return 'testimage.{role}'.format(role=role)

@contextlib.contextmanager
def create_image(ctx, config):
    """
    Create an rbd image.

    For example::

        tasks:
        - ceph:
        - rbd.create_image:
            client.0:
                image_name: testimage
                image_size: 100
            client.1:
    """
    assert isinstance(config, dict) or isinstance(config, list), \
        "task create_image only supports a list or dictionary for configuration"

    if isinstance(config, dict):
        images = config.items()
    else:
        images = [(role, None) for role in config]

    for role, properties in images:
        if properties is None:
            properties = {}
        name = properties.get('image_name', default_image_name(role))
        size = properties.get('image_size', 1024)
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Creating image {name} with size {size}'.format(name=name,
                                                                 size=size))
        remote.run(
            args=[
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/rbd',
                '-c', '/tmp/cephtest/ceph.conf',
                '-p', 'rbd',
                'create',
                '-s', str(size),
                name,
                ],
            )
    try:
        yield
    finally:
        log.info('Deleting rbd images...')
        for role, properties in images:
            if properties is None:
                properties = {}
            name = properties.get('image_name', default_image_name(role))
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    '/tmp/cephtest/archive/coverage',
                    '/tmp/cephtest/binary/usr/local/bin/rbd',
                    '-c', '/tmp/cephtest/ceph.conf',
                    '-p', 'rbd',
                    'rm',
                    name,
                    ],
                )

@contextlib.contextmanager
def modprobe(ctx, config):
    """
    Load the rbd kernel module..

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
    """
    for role in config:
        (remote,) = ctx.cluster.only(role).remotes.keys()
        remote.run(
            args=[
                'sudo',
                'modprobe',
                'rbd',
                ],
            )
    try:
        yield
    finally:
        log.info('Unloading rbd kernel module...')
        for role in config:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'sudo',
                    'modprobe',
                    '-r',
                    'rbd',
                    ],
                )

@contextlib.contextmanager
def dev_create(ctx, config):
    """
    Map block devices to rbd images.

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
        - rbd.dev_create:
            client.0: testimage.client.0
    """
    assert isinstance(config, dict) or isinstance(config, list), \
        "task dev_create only supports a list or dictionary for configuration"

    if isinstance(config, dict):
        role_images = config.items()
    else:
        role_images = [(role, None) for role in config]

    for role, image in role_images:
        if image is None:
            image = default_image_name(role)
        (remote,) = ctx.cluster.only(role).remotes.keys()

        # add udev rule for creating /dev/rbd/pool/image
        remote.run(
            args=[
                'echo',
                'KERNEL=="rbd[0-9]*", PROGRAM="/tmp/cephtest/binary/usr/local/bin/crbdnamer %n", SYMLINK+="rbd/%c{1}/%c{2}"',
                run.Raw('>'),
                '/tmp/cephtest/51-rbd.rules',
                ],
            )
        remote.run(
            args=[
                'sudo',
                'mv',
                '/tmp/cephtest/51-rbd.rules',
                '/etc/udev/rules.d',
                ],
            )

        secretfile = '/tmp/cephtest/data/{role}.secret'.format(role=role)
        teuthology.write_secret_file(remote, role, secretfile)

        remote.run(
            args=[
                'sudo',
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/rbd',
                '-c', '/tmp/cephtest/ceph.conf',
                '--user', role.rsplit('.')[-1],
                '--secret', secretfile,
                '-p', 'rbd',
                'map',
                image,
                ],
            )
    try:
        yield
    finally:
        log.info('Unmapping rbd devices...')
        for role, image in role_images:
            if image is None:
                image = default_image_name(role)
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'sudo',
                    'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    '/tmp/cephtest/archive/coverage',
                    '/tmp/cephtest/binary/usr/local/bin/rbd',
                    '-c', '/tmp/cephtest/ceph.conf',
                    '-p', 'rbd',
                    'unmap',
                    '/dev/rbd/rbd/{imgname}'.format(imgname=image),
                    ],
                )
            remote.run(
                args=[
                    'sudo',
                    'rm',
                    '/etc/udev/rules.d/51-rbd.rules',
                    ],
                wait=False,
                )

@contextlib.contextmanager
def task(ctx, config):
    create_image(ctx, config)