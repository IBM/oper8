"""
Base class for all oper8 commands
"""

# Standard
import abc
import argparse


class CmdBase(abc.ABC):
    __doc__ = __doc__

    @abc.abstractmethod
    def add_subparser(
        self,
        subparsers: argparse._SubParsersAction,
    ) -> argparse.ArgumentParser:
        """Add this command's argument parser subcommand

        Args:
            subparsers (argparse._SubParsersAction): The subparser section for
                the central main parser

        Returns:
            subparser (argparse.ArgumentParser): The configured parser for this
                command
        """

    @abc.abstractmethod
    def cmd(self, args: argparse.Namespace):
        """Execute the command with the parsed arguments

        Args:
            args (argparse.Namespace): The parsed command line arguments
        """
