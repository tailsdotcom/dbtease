"""Methods for running shell commands."""

import subprocess
import logging
from typing import List


logger = logging.getLogger("dbtease.shell")

cmd_logger = logging.getLogger("dbtease.shell.cmd")


def _process_line(line):
    # Turn from bytes to unicode for unicode chars
    # Strip trailing newlines and whitespace
    return line.rstrip()  # .decode("utf8")


def _log_from(iterator, echo=None):
    for raw_line in iterator:
        # Echo the raw line if we have an echo function.
        if echo:
            echo(raw_line.rstrip())
        line = _process_line(raw_line)
        # Log as we go
        cmd_logger.debug(line)
        yield line


def run_shell_command(cmd: List[str], echo=None):
    """Run a shell command, logging the output."""
    logger.debug("Command: %r", cmd)
    # Start the process
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        # Force line level buffering so things arrive in order.
        universal_newlines=True,
        bufsize=1,
    )
    # Cache for stdout lines, outputting as we go...
    stdoutlines = []
    if process.stdout:
        stdoutlines = list(_log_from(process.stdout, echo=echo))

    # Wait for command to finish
    retcode = process.wait()

    # Process stderr
    stderrlines = []
    if process.stderr:
        stderrlines = [_process_line(line) for line in process.stderr]
    # Don't check for success, just return with the relevant code
    return retcode, stdoutlines, stderrlines
