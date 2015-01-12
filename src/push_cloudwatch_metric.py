#!/usr/bin/env python
# encoding: utf-8

import argparse
import re
import sys
import time

from boto.ec2 import cloudwatch
from boto.utils import get_instance_metadata
from collections import defaultdict

AWSAccessKeyId = None
AWSSecretKey = None
METADATA = {}

UNIT_CHOICES = ('Seconds', 'Microseconds', 'Milliseconds', 'Bytes',
                'Kilobytes', 'Megabytes', 'Gigabytes', 'Terabytes', 'Bits',
                'Kilobits', 'Megabits', 'Gigabits', 'Terabits', 'Percent',
                'Count', 'Bytes/Second', 'Kilobytes/Second', 'Megabytes/Second',
                'Gigabytes/Second', 'Terabytes/Second', 'Bits/Second',
                'Kilobits/Second', 'Megabits/Second', 'Gigabits/Second',
                'Terabits/Second', 'Count/Second')


def parser():
    arg_parser = argparse.ArgumentParser(
        description='Post the given metric to CloudWatch',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    arg_parser.add_argument(
        'namespace',
        help='the namespace of the metric'
    )
    arg_parser.add_argument(
        'name',
        metavar='name[,name1,name2...]',
        help='the name of the metric'
    )
    arg_parser.add_argument(
        'value',
        metavar='value[,value1,value2...]',
        help='the value of the metric'
    )
    arg_parser.add_argument(
        '-u', '--unit',
        metavar='unit[,unit1,unit2...]',
        help=('the unit for the metric, units must be one of the following\n'
              '    Seconds, Microseconds, Milliseconds, Bytes,\n'
              '    Kilobytes, Megabytes, Gigabytes, Terabytes, Bits,\n'
              '    Kilobits, Megabits, Gigabits, Terabits, Percent,\n'
              '    Count, Bytes/Second, Kilobytes/Second, Megabytes/Second,\n'
              '    Gigabytes/Second, Terabytes/Second, Bits/Second,\n'
              '    Kilobits/Second, Megabits/Second, Gigabits/Second,\n'
              '    Terabits/Second, Count/Second')
    )
    arg_parser.add_argument(
        '-d', '--dimensions',
        metavar='name=value[,name1=value1...]',
        help=('extra name value pairs to associate with the metric, multiple\n'
              'values could be associated with the same name')
    )
    arg_parser.add_argument(
        '-c', '--credential',
        metavar='credential_file_path',
        type=file,
        default='/opt/aws/cloudwatch-creds',
        help=('default: /opt/aws/cloudwatch-creds\n'
              'cloudWatch credential file path, this file should have the\n'
              'following format:\n'
              '    AWSAccessKeyId=\n'
              '    AWSSecretKey=')
    )
    arg_parser.add_argument(
        '-i', '--instance_specific',
        action='store_true',
        help=('if specified, instance id will be attached to dimensions to '
              'make\ninstance specific metric')
    )
    arg_parser.add_argument(
        '-v', '--verify',
        action='store_true',
        help='list the metric to be posted instead of pushing it to CloudWatch'
    )

    return arg_parser


def _populate_credential(credential):
    for line in credential:
        if '=' in line:
            parts = line.split('=')
            if parts[0] == 'AWSAccessKeyId':
                AWSAccessKeyId = parts[1].strip("\n")
            if parts[0] == 'AWSSecretKey':
                AWSSecretKey = parts[1].strip("\n")
    if not (AWSAccessKeyId and AWSSecretKey):
        raise ValueError("AWSAccessKeyId or AWSSecretKey is empty")


def _populate_metadata():
    metadata = get_instance_metadata(timeout=5)
    if metadata:
        METADATA['instance_id'] = metadata['instance-id']
        region_name = metadata['placement']['availability-zone'][0:-1]
        for curr_region in cloudwatch.regions():
            if curr_region.name == region_name:
                METADATA['region'] = curr_region


def put_cloudwatch_metric(namespace,
                          name,
                          value,
                          dimensions=None,
                          unit=None,
                          instance_specific=False):
    cw = cloudwatch.CloudWatchConnection(
        region=METADATA.get('region'),
        aws_access_key_id=AWSAccessKeyId,
        aws_secret_access_key=AWSSecretKey
    )
    if instance_specific:
        instance_id = METADATA.get('instance_id')
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


def _parse_dimension(dimensions):
    if not dimensions:
        return
    dimension_dict = defaultdict(list)
    key_val = dimensions.split(',')
    for kv in key_val:
        kv = kv.split('=')
        k = kv[0]
        v = '='.join(kv[1:])
        dimension_dict[k].extend([v])
    for k, v in dimension_dict.iteritems():
        if len(v) == 1:
            dimension_dict[k] = v.pop()
        else:
            dimension_dict[k] = tuple(v)
    return dimension_dict


def _parse_name(name):
    return name.split(',')


def _parse_value(value):
    values = value.split(',')
    try:
        values = map(float, values)
    except:
        raise ValueError('metric value should be float.')
    return values


def _parse_unit(unit):
    units = unit.split(',') if unit else None
    if not units:
        return []
    for u in units:
        if u not in UNIT_CHOICES:
            raise ValueError('unit must be one of the following:\n'
                             '{}'.format(', '.join(UNIT_CHOICES)))
    return units


def main():
    try:
        arg_parser = parser()
        args = arg_parser.parse_args()
        names = _parse_name(args.name)
        values = _parse_value(args.value)
        units = _parse_unit(args.unit)
        dimensions = _parse_dimension(args.dimensions)
        if len(names) != len(values):
            raise ValueError('names should be as many as values.')
        if len(names) != len(units):
            units = units + ['None'] * (len(names) - len(units))
    except IOError, e:
        arg_parser.error('credential file not found at {},'
                         'try -c to use another path'.format(e.filename))
    except Exception, e:
        arg_parser.error(e.message)

    if args.verify:
        print 'namespace: {}'.format(args.namespace)
        for n, v, u in zip(names, values, units):
            print 'metric: {}={} {}'.format(n, v, '' if u == 'None' else u)
        if dimensions:
            for d, v in dimensions.iteritems():
                print 'dimension: {}={}'.format(d, v)
        print 'instance specific: {}'.format(args.instance_specific)
        return

    _populate_metadata()
    _populate_credential(args.credential_path)

    put_cloudwatch_metric(
        args.namespace,
        names,
        values,
        dimensions,
        units,
        args.instance_specific
    )


if __name__ == '__main__':
    sys.exit(main())
