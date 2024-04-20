from pathlib import Path
import re
from typing import List, Union

from jztools.colorama import colored_text


class StackLine:
    _compiled_pattern = re.compile(
        '^  File "(.*/site-packages/(?P<package>[^/]+$)|.*/(?P<user_file>[^/]+?))", '
        r"line (?P<line>\d+), "
        "in (?P<function>.*)\n    "
        "(?P<code>.*)\n",
        re.MULTILINE,
    )

    def __init__(self, line):
        self.raw = line
        members = re.match(self._compiled_pattern, line).groupdict()
        self.anon_path = members.get("package") or members.get(
            "user_file"
        )  # TODO: Anonymize the path
        self.is_frozen_call = Path(self.anon_path).name == Path(__file__).name
        self.line_no = int(members["line"])
        self.function = members["function"]
        self.code = members["code"]

    def __eq__(self, stack_line):
        if not isinstance(stack_line, StackLine):
            return NotImplemented
        else:
            return self.raw == stack_line.raw or (
                self.is_frozen_call and stack_line.is_frozen_call
            )


class Stack:
    def __init__(self, formatted_stack):
        self.stack_lines = [StackLine(x) for x in formatted_stack]

    def __eq__(self, stack: Union["Stack", List[str]]):
        if isinstance(stack, list):
            stack = Stack(stack)
        elif not isinstance(stack, Stack):
            return NotImplemented
        return all(x == y for x, y in zip(self.stack_lines, stack.stack_lines))

    def comparison_string(self, stack: "Stack"):
        diff = [x != y for x, y in zip(self.stack_lines, stack.stack_lines)]
        diff_lines = [
            (
                colored_text(self_line.raw, "green")
                if not is_diff
                else (
                    colored_text(self_line.raw, "yellow")
                    + colored_text(test_line.raw, "red")
                )
            )
            for self_line, test_line, is_diff in zip(
                self.stack_lines, stack.stack_lines, diff
            )
        ]
        return "".join(diff_lines)
