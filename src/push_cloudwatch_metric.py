#!/usr/bin/env python
# encoding: utf-8

import argparse
from datetime import datetime, timedelta
import logging
import re
import sys
import time

from boto.ec2 import cloudwatch
from boto.utils import get_instance_metadata


def parser():
    arg_parser = argparse.ArgumentParser(
        description='Post the given metric to CloudWatch',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    arg_parser.add_argument(
        'namespace',
        help='the namespace the metric will be posted to'
    )
    arg_parser.add_argument(
        'name',
        help='the name of the metric'
    )
    arg_parser.add_argument(
        'value',
        type=float,
        help='the value of the metric'
    )
    arg_parser.add_argument(
        '-u', '--unit',
        help='the unit for metric'
    )
    arg_parser.add_argument(
        '-d', '--dimensions',
        help=('extra name value pairs to associate with the metric\n'
              'i.e.: name1=value1,name2=value2&value3')
    )
    arg_parser.add_argument(
        '-i', '--instance_specific',
        action='store_true',
        help=('if specified, instance id will be attached to dimensions to\n'
              'make instance specific metric')
    )
    arg_parser.add_argument(
        '-c', '--credential',
        type=file,
        default='/opt/aws-scripts-mon/linqia-cloudwatch-creds',
        help='the aws credential file path'
    )
    arg_parser.add_argument(
        '-v', '--verify',
        action='store_true',
        help='list metric info to be posted but will not actually push to CloudWatch'
    )

    return arg_parser.parse_args()


class CloudWatch(object):

    logger = logging.getLogger(__name__)
    AWSAccessKeyId = None
    AWSSecretKey = None
    metadata = {}

    def __init__(self, credential_path):
        self._populate_metadata()
        self._populate_credential(credential_path)

    def _populate_credential(self, credential):
        for line in credential:
            if '=' in line:
                parts = line.split('=')
                if parts[0] in ('AWSAccessKeyId', 'AWSSecretKey'):
                    setattr(self, parts[0].strip("\n"), parts[1].strip("\n"))
        if not (self.AWSAccessKeyId and self.AWSSecretKey):
            raise ValueError("AWSAccessKeyId or AWSSecretKey is empty")

    def _populate_metadata(self):
        metadata = get_instance_metadata(timeout=5)
        if metadata:
            self.metadata['instance_id'] = metadata['instance-id']
            region_name = metadata['placement']['availability-zone'][0:-1]
            for curr_region in cloudwatch.regions():
                if curr_region.name == region_name:
                    self.metadata['region'] = curr_region

    def put_cloudwatch_metric(self,
                              namespace,
                              name,
                              value,
                              dimensions=None,
                              unit=None,
                              instance_specific=False):
        cw = cloudwatch.CloudWatchConnection(
            region=self.metadata.get('region'),
            aws_access_key_id=self.AWSAccessKeyId,
            aws_secret_access_key=self.AWSSecretKey
        )
        if instance_specific:
            instance_id = self.metadata.get('instance_id')
            if not instance_id:
                raise ValueError('Instance id is empty')
            if not dimensions:
                dimensions = {}
            dimensions.update(dict(InstanceId=instance_id))
        cw.put_metric_data(
            namespace,
            name,
            value,
            unit=unit,
            dimensions=dimensions
        )


def main():
    unit_choices = ('Seconds', 'Microseconds', 'Milliseconds', 'Bytes',
                    'Kilobytes', 'Megabytes', 'Gigabytes', 'Terabytes', 'Bits',
                    'Kilobits', 'Megabits', 'Gigabits', 'Terabits', 'Percent',
                    'Count', 'Bytes/Second', 'Kilobytes/Second', 'Megabytes/Second',
                    'Gigabytes/Second', 'Terabytes/Second', 'Bits/Second',
                    'Kilobits/Second', 'Megabits/Second', 'Gigabits/Second',
                    'Terabits/Second', 'Count/Second')

    try:
        args = parser()
    except IOError, e:
        print ('Credential file not found at {}, try -c to use'
               ' another path').format(e.filename)
        return

    if args.unit and args.unit not in unit_choices:
        print 'unit must be one of these:'
        print ', '.join(unit_choices)
        return

    if args.dimensions:
        dimensions = {}
        key_val = args.dimensions.split(',')
        for kv in key_val:
            kv = key_val.split('=')
            k = kv[0]
            v = '='.join(kv[1:])
            if '&' in v:
                v = tuple(v.split('&'))
            dimensions[k] = v
    else:
        dimensions = None

    if args.verify:
        print 'namespace: {}'.format(args.namespace)
        print 'name: {}'.format(args.name)
        print 'value: {}'.format(args.value)
        print 'unit: {}'.format(args.unit)
        print 'instance specific: {}'.format(args.instance_specific)
        if dimensions:
            print 'dimensions:'
            for k, v in dimensions.iteritems():
                print '    {}: {}'.format(k, v)
        else:
            print 'dimensions: {}'.format(dimensions)
        return

    watch = CloudWatch(args.credential)
    watch.put_cloudwatch_metric(
        args.namespace,
        args.name,
        args.value,
        dimensions,
        args.unit,
        args.instance_specific
    )


if __name__ == '__main__':
    sys.exit(main())
