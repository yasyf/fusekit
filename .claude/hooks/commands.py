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
    """True when a `grep` command does not consume piped input.

    Allows the stream-filter idiom (`… | grep`) while still blocking grep used
    for file searching, whether standalone, heading a pipe, or in a `&&`/`;` chain.
    """

    def check_command_line(self, evt: BaseHookEvent, cl: CommandLine) -> bool:
        return any(
            cmd.matches(r"^grep\b") and (i == 0 or cl.parts[i - 1][1] != "|") for i, (cmd, _) in enumerate(cl.parts)
        )


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
    },
)
