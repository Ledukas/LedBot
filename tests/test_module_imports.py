"""Confirms LedBotCode.py/Functions.py import cleanly and don't try to connect
to Discord as a side effect of import -- a regression pin for the
`if __name__ == "__main__":` guard around bot.run(TOKEN).

Relies on the real local .env/service_account.json already present in this
working tree (the same precondition the bot itself needs to run) -- these
tests aren't trying to achieve zero-secrets hermeticity, just to pin the
import-time guard.
"""

import importlib
import sys


def test_ledbotcode_imports_without_calling_bot_run(no_bot_run):
    sys.modules.pop("LedBotCode", None)
    import LedBotCode  # noqa: F401

    no_bot_run.assert_not_called()


def test_functions_imports_cleanly(no_bot_run):
    sys.modules.pop("Functions", None)
    import Functions  # noqa: F401


def test_logic_reachable_from_ledbotcode(no_bot_run):
    sys.modules.pop("LedBotCode", None)
    import LedBotCode

    assert LedBotCode.logic.compute_gp_rank_role(1000, [(1000, 'Knight')]) == 'Knight'


def test_logic_reachable_from_functions(no_bot_run):
    sys.modules.pop("Functions", None)
    import Functions

    assert Functions.logic.filter_top_average is not None
