# Ozone checks

This directory contains a collection of easy-to-use helper scripts to execute various type of tests on the ozone/hdds codebase.

The contract of the scripts are very simple:

 1. Executing the scripts without any parameter will check the hdds/ozone project
 2. Shell exit code represents the result of the check (if failed, exits with non-zero code)
 3. Detailed information may be saved to the $OUTPUT_DIR (if it's not set, root level ./target will be used).
 4. The standard output should contain all the log about the build AND the results.
 5. The content of the $OUTPUT_DIR can be:
    * `summary.html`/`summary.md`/`summary.txt`: contains a human readable overview about the failed tests (used by reporting)
    * `failures`: contains a simple number (used by reporting)
