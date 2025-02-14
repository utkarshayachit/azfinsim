#! /usr/bin/env python3

r"""Common arg parser. It support parsing command line arguments or reading the same
from an optionally specified config file.
"""
import argparse
import json
import logging

log = logging.getLogger(__name__)

class ArgumentsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        prefix = "azfinsim-"
        prefix_length = len(prefix)
        with values as f:
            secrets = json.load(f)
            for key, value in secrets.items():
                key = key.lower()
                if key.startswith(prefix):
                    parser.parse_known_args(
                        ["--" + key[prefix_length:], value], namespace=namespace
                    )
                elif key.startswith("-"):
                    parser.parse_known_args([key, value], namespace=namespace)
                else:
                    parser.parse_known_args(["--" + key, value], namespace=namespace)


class ParseTagsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        tags = {}
        for tag in values.split(","):
            key, value = tag.split("=")
            tags[key.strip()] = value.strip()
        setattr(namespace, self.dest, tags)


def getargs(progname):
    if progname not in ["azfinsim", "generator", "split", "concat"]:
        raise ValueError(f"Invalid program name: {progname}")

    parser = argparse.ArgumentParser(progname)

    # -- cli parsing
    parser.add_argument(
        "--config",
        type=open,
        action=ArgumentsAction,
        help="read extra arguments from the config file (json)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="generate verbose output",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="do not colorize output",
    )

    # -- Cache parameters
    if progname in ["azfinsim", "generator"]:
        cacheParser = parser.add_argument_group("Cache", "Cache-specific options")
        cacheParser.add_argument(
            "--cache-type",
            choices=["redis", "filesystem"],
            help="cache type (default: auto-detected)",
        )

        redisParser = parser.add_argument_group(
            "Redis Cache", "Redis Cache-specific options (when --cache-type=redis)"
        )
        redisParser.add_argument("--cache-name", help="redis hostname/ip address")
        redisParser.add_argument(
            "--cache-port",
            default=6380,
            type=int,
            help="redis port number (default=6380 [SSL])",
        )
        redisParser.add_argument("--cache-key", help="cache access key")
        redisParser.add_argument(
            "--cache-ssl",
            default="yes",
            choices=["yes", "no"],
            help="use SSL for redis cache access (default: yes)",
        )

    fsParser = parser.add_argument_group(
        "Filesystem Cache",
        "Filesystem Cache-specific options (when --cache-type=filesystem)",
    )
    fsParser.add_argument("--cache-path", help="filesystem path for cache")
    if progname in ["azfinsim", "split"]:
        fsParser.add_argument(
            "--output-path",
            default=None,
            help="filesystem directory for results."
        )
    if progname == "concat":
        fsParser.add_argument(
            "--output-path", help="merged file name.", type=str
        )

    # -- algorithm/work per thread
    if progname in ["azfinsim", "generator", "split"]:
        workParser = parser.add_argument_group("Trades", "Trade-specific options")
        if progname in ["azfinsim", "generator"]:
            workParser.add_argument(
                "-s",
                "--start-trade",
                type=int,
                help="trade range to process: starting trade number",
            )
            workParser.add_argument(
                "-w", "--trade-window", type=int, help="number of trades to process"
            )
        if progname == "split":
            workParser.add_argument(
                "-w", "--trade-window", type=int, help="number of trades per file"
            )

    if progname == "azfinsim":
        algoParser = parser.add_argument_group(
            "Algorithm", "Algorithm-specific options"
        )
        algoParser.add_argument(
            "--algorithm",
            default="deltavega",
            choices=["deltavega", "pvonly", "synthetic"],
            help="pricing algorithm (default: deltavega)",
        )

        # -- synthetic workload options
        algoParser.add_argument(
            "--delay-start",
            type=int,
            default=0,
            help="delay startup time in seconds (default: 0)",
        )
        algoParser.add_argument(
            "--mem-usage",
            type=int,
            default=16,
            help="memory usage for task in MB (default: 16)",
        )
        algoParser.add_argument(
            "--task-duration",
            type=int,
            default=20,
            help="task duration in milliseconds (default: 20)",
        )
        algoParser.add_argument(
            "--failure",
            type=float,
            default=0.0,
            help="inject random task failure with this probability (default: 0.0)",
        )

    # -- logs & metrics
    insightsParser = parser.add_argument_group(
        "Azure Application Insights", "Azure Application Insights-specific options"
    )
    insightsParser.add_argument(
        "-i",
        "--app-insights",
        help="Azure Application Insights Connection String",
        type=str,
        default=None,
    )
    insightsParser.add_argument(
        "-t",
        "--tags",
        help="tags to add to metrics; a comma-separated key=value pairs are expected",
        action=ParseTagsAction,
        default={},
    )

    import sys
    log.debug(f"parsing arguments: {sys.argv}")
    args = parser.parse_args()

    from . import process_args
    process_args(progname, args)
    return args
