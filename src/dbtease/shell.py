"""Methods for running shell commands."""

import subprocess
import logging
from typing import List


logger = logging.getLogger("dbtease.shell")

cmd_logger = logging.getLogger("dbtease.shell.cmd")


def _process_line(line):
    # Turn from bytes to unicode for unicode chars
    # Strip trailing newlines and whitespace
    return line.decode("utf8").rstrip()


def _log_from(iterator):
    for raw_line in iterator:
        line = _process_line(raw_line)
        # Log as we go
        cmd_logger.info(line)
        yield line


def run_shell_command(cmd: List[str]):
    """Run a shell command, logging the output."""
    logger.info("Command: %r", cmd)
    # Start the process
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    # Output stdout as we go...
    stdoutlines = []  # Cache for stdout lines
    with process.stdout:
        stdoutlines = list(_log_from(process.stdout))

    # Wait for command to finish
    retcode = process.wait()
    # Don't check for success, just return with the relevant code
    return retcode, stdoutlines
