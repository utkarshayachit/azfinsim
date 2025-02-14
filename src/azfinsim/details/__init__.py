import colorlog
import logging
import os
import sys

from . import metrics

# helper setup azure log handler
def _az_log_handler(connection_string: str):
    # register log handler
    from opencensus.ext.azure.log_exporter import AzureLogHandler

    logger = logging.getLogger(__name__)
    logger.addHandler(AzureLogHandler(connection_string=connection_string))

    # register metrics exporter
    from opencensus.ext.azure import metrics_exporter
    from opencensus.stats import stats as stats_module

    stats_module.stats.view_manager.register_exporter(
        metrics_exporter.new_metrics_exporter(connection_string=connection_string)
    )


def _initialize_logging():
    import sys
    no_color = "--no-color" in sys.argv
    verbose = "--verbose" in sys.argv

    # setup logging for this package
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO if not verbose else logging.DEBUG)

    # setup console logger to output to terminal by default
    ch = logging.StreamHandler()
    if no_color:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    else:
        formatter = colorlog.ColoredFormatter(
            "%(purple)s%(asctime)s - %(name)s - %(levelname)s - %(log_color)s%(message)s"
        )

    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # setup azure log handler from env var
    if os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") is not None:
        logger.info("adding AzureLogHandler from env var")
        _az_log_handler(os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING"))

    # setup unhandled exception logging
    def _log_unhandled_exception(exc_type, exc_value, exc_traceback):
        logger.exception(
            "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
        )

    sys.excepthook = _log_unhandled_exception


def process_args(progname, args):
    """call this function to process command line arguments to update package logging settings"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    # setup azure log handler
    if args.app_insights is not None:
        logger.info("adding AzureLogHandler from args")
        _az_log_handler(args.app_insights)

    # setup default tags for metrics
    tags = {
        "app": progname,
        "package": "azfinsim",
    }
    if args.tags is not None:
        tags.update(args.tags)
    metrics.initialize_tags(tags)

    logger = logging.getLogger(__name__)
    for key in dir(args):
        if key.startswith("_"):
            continue
        logger.debug(f"args.{key} = {getattr(args, key)}")

# initialize logging
_initialize_logging()
