#!/usr/bin/env python3

import argparse
import os
import sys
from subprocess import run
from typing import Optional, Dict, List, Union

MAIN_CLASS = 'ldbc.finbench.datagen.LdbcDatagen'


class KeyValue(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split('=', maxsplit=1)
            # assign into dictionary
            getattr(namespace, self.dest)[key.strip()] = value


def split_passthrough_args():
    args = sys.argv[1:]
    try:
        sep = args.index('--')
        return args[:sep], args[sep + 1:]
    except ValueError:
        return args, []


def flatten(ls):
    return [i for sl in ls for i in sl]


def run_local(
        jar_file: str,
        main_class: Optional[str] = None,
        cores: Optional[Union[int, str]] = None,
        memory: Optional[str] = None,
        parallelism: Optional[int] = None,
        spark_conf: Optional[Dict] = None,
        passthrough_args: Optional[List[str]] = None
):
    if not cores:
        cores = "*"
    if not spark_conf:
        spark_conf = {}
    if not main_class:
        main_class = MAIN_CLASS

    opt_class = ['--class', main_class]
    opt_master = ['--master', f'local[{cores}]']

    additional_opts = []

    # In local mode execution takes place on the driver
    if memory:
        additional_opts.extend(['--driver-memory', memory])

    final_spark_conf = {
        **({'spark.default.parallelism': str(parallelism)} if parallelism else {}),
        **spark_conf
    }

    # num-threads configuration for hadoop is not support yet in Scala part
    # arg_opts = [
    #     *(['--num-threads', str(parallelism)] if parallelism else []),
    # ]

    conf = flatten([['-c', f'{k}={v}'] for k, v in final_spark_conf.items()])
    cmd = [
        'spark-submit',
        *conf,
        *opt_master,
        *opt_class,
        *additional_opts,
        jar_file,
        # *arg_opts,
        *passthrough_args
    ]

    print(cmd)

    default_env = dict(os.environ)

    run(cmd, env=default_env)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a Datagen job locally')
    parser.add_argument('--jar',
                        type=str,
                        default=os.environ.get('LDBC_FINBENCH_DATAGEN_JAR'),
                        help='LDBC Datagen JAR file [LDBC_FINBENCH_DATAGEN_JAR]')
    parser.add_argument('--main-class',
                        type=str,
                        help='Overrides default main class.')
    parser.add_argument('--cores',
                        type=int,
                        help='number of vcpu cores to use'
                        )
    parser.add_argument('--memory',
                        type=str,
                        help='amount of memory to use. E.g. 512m, 16g, 1t'
                        )
    parser.add_argument('--conf',
                        nargs='+',
                        action=KeyValue,
                        help="Spark conf as a list of key=value pairs")
    parser.add_argument('--parallelism',
                        type=int,
                        help='sets job parallelism. Higher values might reduce chance of OOM.')
    parser.add_argument('-y',
                        action='store_true',
                        help='Assume \'yes\' for prompts')
    parser.add_argument('--', nargs='*', help='Arguments passed to LDBC FinBench Datagen', dest="arg")

    self_args, child_args = split_passthrough_args()

    args = parser.parse_args(self_args)

    if not args.jar:
        raise ValueError('No JAR given. Specify with --jar or LDBC_FINBENCH_DATAGEN_JAR env var')

    run_local(
        args.jar,
        main_class=args.main_class,
        cores=args.cores,
        memory=args.memory,
        parallelism=args.parallelism,
        spark_conf=args.conf,
        passthrough_args=child_args
    )
