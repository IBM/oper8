"""
This is the main entrypoint command for running the operator
"""
# Standard
from datetime import datetime, timedelta
from pathlib import Path
import argparse

# First Party
import alog

# Local
from .. import config
from ..watch_manager.python_watch_manager.threads.heartbeat import HeartbeatThread
from .base import CmdBase

log = alog.use_channel("MAIN")


class CheckHeartbeatCmd(CmdBase):
    __doc__ = __doc__

    ## Interface ##

    def add_subparser(
        self,
        subparsers: argparse._SubParsersAction,
    ) -> argparse.ArgumentParser:
        parser = subparsers.add_parser("check-heartbeat", help=__doc__)
        runtime_args = parser.add_argument_group("Check Heartbeat Configuration")
        runtime_args.add_argument(
            "--delta",
            "-d",
            required=True,
            type=int,
            help="Max time allowed since last check",
        )
        runtime_args.add_argument(
            "--file",
            "-f",
            default=config.python_watch_manager.heartbeat_file,
            help="Location of health check file. Defaults to config based.",
        )
        return parser

    def cmd(self, args: argparse.Namespace):
        """Run command to validate a health check file"""

        # Validate args
        assert args.delta is not None
        assert args.file is not None

        # Ensure file exists
        file_path = Path(args.file)
        if not file_path.exists():
            log.error(f"Health Check failed: {file_path} does not exist")
            raise FileNotFoundError()

        # Read and the most recent time from the health check
        last_log_time = file_path.read_text().strip()
        last_time = datetime.strptime(last_log_time, HeartbeatThread._DATE_FORMAT)

        if last_time + timedelta(seconds=args.delta) < datetime.now():
            msg = f"Health Check failed: {last_log_time} is to old"
            log.error(msg)
            raise KeyError(msg)
