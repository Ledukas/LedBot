"""Confirms LedBotCode.py/Functions.py import cleanly and don't try to connect
to Discord as a side effect of import -- a regression pin for the
`if __name__ == "__main__":` guard around bot.run(TOKEN).

Relies on the real local .env/service_account.json already present in this
working tree (the same precondition the bot itself needs to run) -- these
tests aren't trying to achieve zero-secrets hermeticity, just to pin the
import-time guard.

Modules are removed with monkeypatch rather than sys.modules.pop, so the
originals are put back afterwards. A bare pop left a fresh module in their
place for every later test: a fixture importing Functions then patched the new
module while a test file still held the old one -- whose connection is the
real DatabaseLedBot.db -- and the weekly-marker tests wrote to it.
"""

import sys

import Functions as COLLECTED_FUNCTIONS


def fresh_import(monkeypatch, name):
    monkeypatch.delitem(sys.modules, name, raising=False)
    return __import__(name)


def test_ledbotcode_imports_without_calling_bot_run(no_bot_run, monkeypatch):
    fresh_import(monkeypatch, "LedBotCode")

    no_bot_run.assert_not_called()


def test_functions_imports_cleanly(no_bot_run, monkeypatch):
    fresh_import(monkeypatch, "Functions")


def test_logic_reachable_from_ledbotcode(no_bot_run, monkeypatch):
    LedBotCode = fresh_import(monkeypatch, "LedBotCode")

    assert LedBotCode.logic.compute_gp_rank_role(1000, [(1000, 'Knight')]) == 'Knight'


def test_logic_reachable_from_functions(no_bot_run, monkeypatch):
    Functions = fresh_import(monkeypatch, "Functions")

    assert Functions.logic.filter_top_average is not None


def test_the_original_modules_are_restored():
    """Pins the fix above. Runs after the fresh imports in this file: the
    module bound at collection must be the one in sys.modules again."""
    assert sys.modules["Functions"] is COLLECTED_FUNCTIONS
