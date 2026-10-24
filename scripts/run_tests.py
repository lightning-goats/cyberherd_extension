"""Run the extension suite with isolated imports and disposable database fixtures.

Use an LNbits virtualenv. For PostgreSQL coverage, configure LNBITS_DATABASE_URL
and set CYBERHERD_TEST_POSTGRES=1. An explicit --env-file may supply those settings.
Remaining arguments are passed to pytest. External effects stay mocked by tests.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import sys
import tempfile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path)
    args, pytest_args = parser.parse_known_args()
    if args.env_file:
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
    source = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory(prefix='cyberherd-tests-') as temp:
        root = Path(temp)
        cases = root / 'cases'
        cases.mkdir()
        for test in (source / 'tests').glob('test_*.py'):
            shutil.copy2(test, cases / test.name)
        (root / 'cyberherd').symlink_to(source, target_is_directory=True)
        import lnbits.extensions
        lnbits.extensions.__path__ = [str(root), *lnbits.extensions.__path__]
        from lnbits.extensions.cyberherd import crud

        class UnscopedDatabase:
            def __getattr__(self, name):
                raise RuntimeError('Tests must install a disposable database fixture')

        crud.db = UnscopedDatabase()
        sys.path.insert(0, str(cases))
        import pytest
        return pytest.main([str(cases), '--no-cov', *pytest_args])


if __name__ == '__main__':
    raise SystemExit(main())
