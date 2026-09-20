from __future__ import annotations

from captain_hook import (
    Allow,
    BaseHookEvent,
    Block,
    CommandLine,
    CustomCommandLineCondition,
    Event,
    Input,
    Tool,
    hook,
)


class UnpipedGrep(CustomCommandLineCondition):
    """True when a `grep` call does not consume piped input.

    Matches on the call's name — the unwrapped head word's dequoted, casefolded basename — so
    `/usr/bin/grep`, `'grep'`, `LC_ALL=C grep`, `sudo grep`, and an `sh -c` payload's grep all
    count, while `grep-tool` and `git log --grep` do not. Allows the stream-filter idiom
    (`… | grep`) while still blocking grep used for file searching, whether standalone, heading
    a pipe, or in a `&&`/`;` chain. `xargs` is the exception among wrappers: it consumes the pipe
    itself and hands grep filenames, so `… | xargs grep` is file searching.
    """

    def check_command_line(self, evt: BaseHookEvent, cl: CommandLine) -> bool:
        return any(call.occurrence.prev_op != "|" or "xargs" in call.wrappers for call in evt.cmd.calls("grep"))


hook(
    Event.PreToolUse,
    only_if=[Tool("Bash"), UnpipedGrep()],
    message="BLOCKED: Use ripgrep (rg) instead of grep. Replace grep with rg, or use the built-in Grep tool.",
    block=True,
    tests={
        Input(command="grep -rn foo src/"): Block(),
        Input(command="ls | grep foo"): Allow(),
        Input(command="cat x | grep foo | sort"): Allow(),
        Input(command="grep foo file.py | wc -l"): Block(),
        Input(command="grep foo a && echo done"): Block(),
        Input(command="git log --grep=fix"): Allow(),
        Input(command='git log --grep "fix bug"'): Allow(),
        Input(command="/usr/bin/grep -rn foo src/"): Block(),
        Input(command="'grep' -rn foo src/"): Block(),
        Input(command="LC_ALL=C grep -rn foo src/"): Block(),
        Input(command="xargs grep foo"): Block(),
        Input(command="rg -l foo | xargs grep -n bar"): Block(),
        Input(command="sh -c 'grep -rn foo src/'"): Block(),
        Input(command="sh -c 'ls | grep foo'"): Allow(),
        Input(command="grep-tool foo"): Allow(),
    },
)
