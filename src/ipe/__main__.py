"""Entry point for `python -m ipe`."""

import sys

from ipe.runtime.cli import main

if __name__ == "__main__":
    sys.exit(main())
