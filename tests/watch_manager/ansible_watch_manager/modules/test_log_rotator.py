"""
Test the implementation of the log_rotator open_iteration function
"""

# Standard
import json
import os
import shlex
import subprocess
import sys
import tempfile

# First Party
import alog

# Local
from oper8.test_helpers.helpers import configure_logging

configure_logging()
log = alog.use_channel("TEST")


def strip_indentation(script_string):
    # Strip off "extra" indentation. This allows the tests to not look TERRIBLE
    lines = script_string.split("\n")
    first_line = [l for l in lines if l.strip()][0]
    indentation = first_line[: len(first_line) - len(first_line.lstrip())]
    if all([l.startswith(indentation) or not l for l in lines]):
        lines = [l[len(indentation) :] for l in lines]
    return "\n".join(lines)


def run_test_script(script_string):
    """Since the goal of the rotator is to perform log rotation over a series of
    successive python process executions, we use this helper to launch
    subprocesses
    """

    script = strip_indentation(script_string)
    with tempfile.NamedTemporaryFile(mode="w") as f_handle:
        # Save the script
        f_handle.write(script)
        f_handle.flush()

        # Run it
        assert os.path.exists(f_handle.name)
        cmd = f"{sys.executable} {f_handle.name}"
        with alog.ContextTimer(log.info, "Subprocess runtime: "):
            subprocess.run(shlex.split(cmd))


def parse_log_files(workdir):
    out = {}
    for fname in os.listdir(workdir):
        fpath = os.path.join(workdir, fname)
        with open(fpath, "r") as log_handle:
            out[fname] = json.load(log_handle)
    return out


def test_alog_configure():
    """Test that a log file is created when using the AutoRotatingFileHandler
    with alog.configure
    """
    with tempfile.TemporaryDirectory() as workdir:
        log_file_name = "foo.log"

        # Define the "doit" script which will open the log file for rotation
        def get_script(i):
            return f"""
                import alog
                from oper8.watch_manager.ansible_watch_manager.modules.log_rotator import AutoRotatingFileHandler
                alog.configure(
                    "info",
                    formatter="json",
                    handler_generator=lambda: AutoRotatingFileHandler(
                        '{os.path.join(workdir, log_file_name)}',
                        backupCount=1,
                    ),
                )
                ch = alog.use_channel("TEST")
                ch.info("TEST {i}")
                """

        # Run the script the first time
        run_test_script(get_script(1))
        all_files = parse_log_files(workdir)
        assert len(all_files) == 1
        assert log_file_name in all_files
        assert all_files[log_file_name]["message"] == "TEST 1"

        # Run it a second time and make sure it keeps one backup
        run_test_script(get_script(2))
        all_files = parse_log_files(workdir)
        assert len(all_files) == 2
        assert log_file_name in all_files
        assert all_files[log_file_name]["message"] == "TEST 2"
        assert all_files[f"{log_file_name}.1"]["message"] == "TEST 1"

        # Run it a third time and make sure it keeps only one backup
        run_test_script(get_script(3))
        all_files = parse_log_files(workdir)
        assert len(all_files) == 2
        assert log_file_name in all_files
        assert all_files[log_file_name]["message"] == "TEST 3"
        assert all_files[f"{log_file_name}.1"]["message"] == "TEST 2"
