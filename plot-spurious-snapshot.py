#!/usr/bin/env python
# -*- coding: utf-8 -*-#
#
#
# Copyright (C) 2015, S3IT, University of Zurich. All rights reserved.
#
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

__docformat__ = 'reStructuredText'

import rbd
import pygraphviz as pgv
import argparse
import os
import re
import sys
import rados
import logging
from keystoneclient.auth.identity import v3
from keystoneauth1 import session
from keystoneclient.v3 import client as keystone_client
from novaclient import client as nova_client
from cinderclient import client as cinder_client
from glanceclient import client as glance_client
import cinderclient.exceptions as cex
import novaclient.exceptions as nex

log = logging.getLogger()
log.addHandler(logging.StreamHandler())

volume_re = re.compile('^volume-(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})')
disk_re = re.compile('(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})_disk$')
disk_clone_re = re.compile('(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})_disk_clone_\w{32}$')
uuid_re = re.compile('^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$')
# no image exist in db with id "to_be_deleted_by_glance" so no need to query again.
#disk_clone_deleted = re.compile('(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})_disk_clone_\w{32}_to_be_deleted_by_glance$')
#disk_deleted = re.compile('(?P<uuid>\w{8}-\w{4}-\w{4}-\w{4}-\w{12})_disk_to_be_deleted_by_glance$')
#to be deleted by glance if does not have a children
images = {}
to_delete= []


class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if envvar and envvar in os.environ:
            default = os.environ[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required,
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


def make_session(opts):
    """Create a Keystone session"""
    auth = v3.Password(auth_url=opts.os_auth_url,
                       username=opts.os_username,
                       password=opts.os_password,
                       project_name=opts.os_project_name,
                       user_domain_name=opts.os_user_domain_name,
                       project_domain_name=opts.os_project_domain_name)
    sess = session.Session(auth=auth)
    return sess


def cluster_connect(pool, conffile, rados_id):
    # cluster = rados.Rados(conffile=conffile, rados_id=rados_id)
    cluster = rados.Rados(conffile=conffile)
    cluster.connect()
    ioctx = cluster.open_ioctx(pool)
    return ioctx


def color_by_name(name):
    return 'red' if 'to_be_deleted_by_glance' in name else 'gray'


def shape_by_name(name):
    # glance images have "disk_clone" in the name, VM root disks have
    # "disk" Of course, deleted images have
    # "disk_clone_to_be_deleted_by_glance" while VM root disks of
    # deleted instances have "disk_to_be_deleted_by_glance
    #
    # box: glance images
    # polygon: vm images
    if 'disk_clone' in name:
        return 'box'
    else:
        return 'ellipse'


def style_if_exsist(name):
    if not exists_in_os(name):
        return 'filled'
    return ''


def exists_in_os(vol):
    if volume_re.search(vol):
        # look for persistent volume in cinder either as root disk or as attached

        uuid = volume_re.search(vol).group('uuid')
        log.info("Checking if cinder volume %s exists", uuid)
        try:
            volume=cclient.volumes.get(uuid)
            log.info("Volume %s exists.", uuid)
            if volume.attachments:
                if volume.attachments[0]['attachment_id']:
                    print ("Volume is attached to %s",volume.attachments[0]['server_id'])
                    return True
        except cex.NotFound:
            log.error("Not Found: %s rbd image should be deleted", uuid)
            to_delete.append("volume: rbd -p %s rm %s " % (cfg.pool, vol))
            return False

    elif disk_re.search(vol):
        # look for VM root disks in nova
        uuid = disk_re.search(vol).group('uuid')
        log.info("Checking if cinder ephemeral disk %s exists", uuid)
        try:
            volume = nclient.servers.get(uuid)
            log.info("Instance %s exists.", uuid)
            return True
        except nex.NotFound:
            log.error("Not Found: %s rbd image should be deleted", uuid)
            to_delete.append("disk: rbd -p %s rm %s " %(cfg.pool,vol))
            return False

    elif disk_clone_re.search(vol):
        #  look for image in glance confirm deleted images are effectively removed in glance.
        log.info("Checking if glance image disk %s exists", vol)
        images_list = gclient.images.list()
        for img in images_list:
            try:
                # check if there is any image that has this vol in as its direct_url
                if vol in img.direct_url:
                    log.info("Instance %s exists.", vol)
                    return True
            except:
                pass
        log.error("Not Found: %s rbd image should be deleted", vol)
            to_delete.append("disk_clone rbd -p %s rm %s " % (cfg.pool, vol))
        return False

    elif uuid_re.search(vol):
        log.info("Checking if glance image %s exists", vol)
        try:
            img = gclient.images.get(vol)
            log.info("Image %s exists.", vol)
            return True
        except cex.NotFound:
            log.error("Not Found: %s rbd image should be deleted", uuid)
            to_delete.append("uuid: rbd -p %s rm %s " % (cfg.pool, vol))
            return False
    log.info("No api found %s volume", vol)
    return False

def fill_graph(cfg, graph, root, ioctx, vol, descend=True, ascend=True):
    # Find parent, snapshots and children for this image and call this
    shape = shape_by_name(vol)
    style = style_if_exsist(vol)
    print("Starting from root %s" % vol)

    graph.add_node(vol,
                   color=color_by_name(vol),
                   shape=shape,
                   style=style)

    # Find snapshots
    for snapshot in root.list_snaps():
        snap = rbd.Image(ioctx, vol, snapshot=snapshot['name'], read_only=True)
        snapname = '%s\n@%s' % (vol, snapshot['name'])
        print("Found snapshot %s" % snapname)
        graph.add_node(snapname,
                       color=color_by_name(snapshot['name']),
                       shape=shape)
        graph.add_edge(vol, snapname)

        if descend:
            for children in snap.list_children():
                if children[0] != cfg.pool:
                    continue
                child = images.get(children[1])
                if not child:
                    child = rbd.Image(ioctx, children[1], read_only=True)
                print("Found children %s in ceph" % children[1])
                fill_graph(cfg, graph, child, ioctx, children[1], ascend=False)
                graph.add_edge(snapname, children[1])
    if ascend:
        try:
            parent = root.parent_info()
            if parent[0] == cfg.pool:
                image = images.get(parent[1])
                if not image:
                    image = rbd.Image(ioctx, parent[1], read_only=True)
                    
                fill_graph(cfg, graph, image, ioctx, parent[1], descend=False)
                print("Found parent %s" % parent[1])
                graph.add_node('%s\n@%s' % (parent[1], parent[2]), color=color_by_name(parent[2]), shape=shape_by_name(parent[1]))
                graph.add_edge('%s\n@%s' % (parent[1], parent[2]), vol)
        except rbd.ImageNotFound:
            # No parent, ignore
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--os-username',
                        action=EnvDefault,
                        envvar="OS_USERNAME",
                        help='OpenStack administrator username. If not supplied, the value of the '
                             '"OS_USERNAME" environment variable is used.')
    parser.add_argument('--os-password',
                        action=EnvDefault,
                        envvar="OS_PASSWORD",
                        help='OpenStack administrator password. If not supplied, the value of the '
                             '"OS_PASSWORD" environment variable is used.')
    parser.add_argument('--os-project-name',
                        action=EnvDefault,
                        envvar="OS_PROJECT_NAME",
                        help='OpenStack administrator project name. If not supplied, the value of the '
                             '"OS_PROJECT_NAME" environment variable is used.')
    parser.add_argument('--os-auth-url',
                        action=EnvDefault,
                        envvar="OS_AUTH_URL",
                        help='OpenStack auth url endpoint. If not supplied, the value of the '
                             '"OS_AUTH_URL" environment variable is used.')
    parser.add_argument('--os-user-domain-name',
                        action=EnvDefault,
                        envvar="OS_USER_DOMAIN_NAME",
                        default='default')
    parser.add_argument('--os-project-domain-name',
                        action=EnvDefault,
                        envvar="OS_PROJECT_DOMAIN_NAME",
                        default='default')
    parser.add_argument('-p', '--pool', default='cinder')
    parser.add_argument('-c', '--conf', metavar='FILE',
                        default='/etc/ceph/ceph.conf',
                        help='Ceph configuration file. '
                        'Default: %(default)s')
    parser.add_argument('-u', '--user',
                        default='cinder',
                        help='Ceph user to use to connect. '
                        'Default: %(default)s')
    parser.add_argument('-o', '--output',  default='plot-rbd.dot', help='Output file')
    parser.add_argument('volumes', nargs="*")
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='Increase verbosity')
    cfg = parser.parse_args()
    verbosity = max(0, 3-cfg.verbose) * 10
    log.setLevel(verbosity)

    ioctx = cluster_connect(cfg.pool, cfg.conf, cfg.user)
    rbd_inst = rbd.RBD()
    sess = make_session(cfg)
    cclient = cinder_client.Client('2', session=sess)
    nclient = nova_client.Client('2', session=sess)
    gclient = glance_client.Client('2', session=sess)

    graph = pgv.AGraph(directed=True)
    if not cfg.volumes:
        cfg.volumes = [vol for vol in rbd_inst.list(ioctx) if not vol.endswith('_disk') and not vol.startswith('volume-')]

    for vol in cfg.volumes:
        if vol not in graph:
            image = rbd.Image(ioctx, vol, read_only=True)
            images[vol] = image
            fill_graph(cfg, graph, image, ioctx, vol)
    graph.write(cfg.output)
    if to_delete:
        print "This is the list of commnads you should issue"
        print str.join('\n', to_delete)
    else:
        print "there is no image that can be removed"

