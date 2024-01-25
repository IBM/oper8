#!/usr/bin/env python
"""
The main module provides the executable entrypoint for oper8
"""

# Standard
from typing import Dict, Tuple
import argparse

# First Party
import aconfig
import alog

# Local
from .cmd import CheckHeartbeatCmd, CmdBase, RunOperatorCmd, SetupVCSCmd
from .component import config
from .config import library_config
from .log_format import Oper8JsonFormatter

## Constants ###################################################################

log = alog.use_channel("MAIN")

## Helpers #####################################################################


def add_library_config_args(parser, config_obj=None, path=None):
    """Automatically add args for all elements of the library config"""
    path = path or []
    setters = {}
    config_obj = config_obj or config
    for key, val in config_obj.items():
        sub_path = path + [key]

        # If this is a nested arg, recurse
        if isinstance(val, aconfig.AttributeAccessDict):
            sub_setters = add_library_config_args(parser, config_obj=val, path=sub_path)
            for dest_name, nested_path in sub_setters.items():
                setters[dest_name] = nested_path

        # Otherwise, add an argument explicitly
        else:
            arg_name = ".".join(sub_path)
            dest_name = "_".join(sub_path)
            kwargs = {
                "default": val,
                "dest": dest_name,
                "help": f"Library config override for {arg_name} (see oper8.config)",
            }
            if isinstance(val, list):
                kwargs["nargs"] = "*"
            elif isinstance(val, bool):
                kwargs["action"] = "store_true"
            else:
                type_name = None
                if val is not None:
                    type_name = type(val)
                kwargs["type"] = type_name

            if (
                f"--{arg_name}"
                not in parser._option_string_actions  # pylint: disable=protected-access
            ):
                parser.add_argument(f"--{arg_name}", **kwargs)
                setters[dest_name] = sub_path
    return setters


def update_library_config(args, setters):
    """Update the library config values based on the parsed arguments"""
    for dest_name, config_path in setters.items():
        config_obj = library_config
        while len(config_path) > 1:
            config_obj = config_obj[config_path[0]]
            config_path = config_path[1:]
        config_obj[config_path[0]] = getattr(args, dest_name)


def add_command(
    subparsers: argparse._SubParsersAction,
    cmd: CmdBase,
) -> Tuple[argparse.ArgumentParser, Dict[str, str]]:
    """Add the subparser and set up the default fun call"""
    parser = cmd.add_subparser(subparsers)
    parser.set_defaults(func=cmd.cmd)
    library_args = parser.add_argument_group("Library Configuration")
    library_config_setters = add_library_config_args(library_args)
    return parser, library_config_setters


## Main ########################################################################


def main():
    """The main module provides the executable entrypoint for oper8"""
    parser = argparse.ArgumentParser(description=__doc__)

    # Add the subcommands
    subparsers = parser.add_subparsers(help="Available commands", dest="command")
    run_operator_cmd = RunOperatorCmd()
    run_operator_parser, library_config_setters = add_command(
        subparsers, run_operator_cmd
    )
    run_health_check_cmd = CheckHeartbeatCmd()
    add_command(subparsers, run_health_check_cmd)
    setup_vcs_cmd = SetupVCSCmd()
    add_command(subparsers, setup_vcs_cmd)

    # Use a preliminary parser to check for the presence of a command and fall
    # back to the default command if not found
    check_parser = argparse.ArgumentParser(add_help=False)
    check_parser.add_argument("command", nargs="?")
    check_args, _ = check_parser.parse_known_args()
    if check_args.command not in subparsers.choices:
        args = run_operator_parser.parse_args()
    else:
        args = parser.parse_args()

    # Provide overrides to the library configs
    update_library_config(args, library_config_setters)

    # Reconfigure logging
    alog.configure(
        default_level=config.log_level,
        filters=config.log_filters,
        formatter=Oper8JsonFormatter() if config.log_json else "pretty",
        thread_id=config.log_thread_id,
    )

    # Run the command's function
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
