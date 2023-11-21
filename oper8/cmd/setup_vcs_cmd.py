"""
CLI command for setting up a VCS version repo
"""
# Standard
import argparse

# First Party
import alog

# Local
from ..setup_vcs import DEFAULT_DEST, DEFAULT_TAG_EXPR, setup_vcs
from .base import CmdBase

log = alog.use_channel("CMD-VCS")


class SetupVCSCmd(CmdBase):
    __doc__ = __doc__

    def add_subparser(
        self,
        subparsers: argparse._SubParsersAction,
    ) -> argparse.ArgumentParser:
        """Add the subparser for this command"""
        parser = subparsers.add_parser(
            "setup-vcs",
            help="Initialize a clean git repo to use with VCS versioning",
        )
        command_args = parser.add_argument_group("Command Arguments")
        command_args.add_argument(
            "--source",
            "-s",
            required=True,
            help="Source repo to seed the clean git history",
        )
        command_args.add_argument(
            "--destination",
            "-d",
            default=DEFAULT_DEST,
            help="Destination directory in which to place the clean git history",
        )
        command_args.add_argument(
            "--branch-expr",
            "-b",
            nargs="*",
            default=None,
            help="Regular expression(s) to use to identify branches",
        )
        command_args.add_argument(
            "--tag-expr",
            "-te",
            nargs="*",
            default=DEFAULT_TAG_EXPR,
            help="Regular expression(s) to use to identify tags",
        )
        command_args.add_argument(
            "--force",
            "-f",
            action="store_true",
            default=False,
            help="Force overwrite existing destination",
        )
        return parser

    def cmd(self, args: argparse.Namespace):
        setup_vcs(
            source=args.source,
            destination=args.destination,
            branch_expr=args.branch_expr,
            tag_expr=args.tag_expr,
            force=args.force,
        )
