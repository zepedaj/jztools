from jztools import autonamed_pattern as mdl
from unittest import TestCase
import re


class TestAutonamedPattern(TestCase):
    def test_doc(self):
        # Simple auto-name pattern
        sp = mdl.AutonamedPattern("(?P<htag>Hello)", names=["htag"])

        # Composed auto-name pattern
        cp = mdl.AutonamedPattern(
            "{x} {x} {x} (?P<wtag>World)", {"x": sp}, names=["wtag"]
        )

        # Match only the first pattern
        assert sp.view() == (sp0_expected := "(?P<htag_0>Hello)")
        assert re.match(sp0_actual := str(sp), "Hello")
        assert sp0_actual == sp0_expected

        # Match composed pattern
        assert cp.view() == (
            cp0_expected := "(?P<htag_1>Hello) (?P<htag_2>Hello) (?P<htag_3>Hello) (?P<wtag_0>World)"
        )
        assert re.match(cp0_actual := str(cp), "Hello Hello Hello World")
        assert cp0_actual == cp0_expected
        assert sp.view() == "(?P<htag_4>Hello)"
        assert (
            cp.view()
            == "(?P<htag_4>Hello) (?P<htag_5>Hello) (?P<htag_6>Hello) (?P<wtag_1>World)"
        )

    def test_identifier_guarantee(self):
        NESTED = mdl.AutonamedPattern("(?P<addend>[0-9])")
        str(NESTED)  # Advance the counter for illustration purposes.

        # 'my_letter' and 'my_value' are at the same nesting level; 'addend' is one level down.
        ap = mdl.AutonamedPattern(
            r"(?P<my_letter>[a-z]) \= (?P<my_value>[0-9]) \+ {NESTED}", vars()
        )
        match = re.match(str(ap), "a = 1 + 2")

        # Tags at the same nesting level have the same suffix identifier
        assert match.groupdict() == {
            "my_letter_0": "a",
            "my_value_0": "1",
            "addend_1": "2",
        }

    def test_doc__no_names(self):
        # Simple auto-name pattern
        sp = mdl.AutonamedPattern("(?P<htag>Hello)")

        # Composed auto-name pattern
        cp = mdl.AutonamedPattern("{x} {x} {x} (?P<wtag>World)", {"x": sp})

        # Match only the first pattern
        assert sp.view() == (sp0_expected := "(?P<htag_0>Hello)")
        assert re.match(sp0_actual := str(sp), "Hello")
        assert sp0_actual == sp0_expected

        # Match composed pattern
        assert cp.view() == (
            cp0_expected := "(?P<htag_1>Hello) (?P<htag_2>Hello) (?P<htag_3>Hello) (?P<wtag_0>World)"
        )
        assert re.match(cp0_actual := str(cp), "Hello Hello Hello World")
        assert cp0_actual == cp0_expected
        assert sp.view() == "(?P<htag_4>Hello)"
        assert (
            cp.view()
            == "(?P<htag_4>Hello) (?P<htag_5>Hello) (?P<htag_6>Hello) (?P<wtag_1>World)"
        )


class TestPxs(TestCase):
    def test_all(self):
        for name, matches, non_matches in [
            ("VARNAME", vnm := ("abc0", "_abc0", "_"), vnnm := ("0abc",)),
            (
                "NS_VARNAME",
                vnm + ("abc0.def1",),
                vnnm + ("abc.0abc", "abc.", "abc.def1."),
            ),
            # Any string will match zero-slashes not preceded by slash
            # at the start of the string, including odd slashes. Skipping
            # Non-matching tests for even slashes.
            (
                "EVEN_SLASHES",
                _even := ("", r"\\", r"\\\\", r"\\\\\\"),
                _odd := ("\\", "\\" * 3),
            ),
            ("ODD_SLASHES", _odd, _even),
            (
                "ATTR",
                ("$abc", r"\\$abc", r"\\\\$abc.def", "$abc"),
                (r"\$abc", r"\\\$abc.def0", "$0abc"),
            ),
            (
                "UNQUOTED_LITERAL",
                (
                    "abc",
                    r"a\$",
                    r"\$b",
                    r"a\#",
                    r"\#b",
                    r"\'a",
                    r"a\"bc",
                    r"a\,",
                    r"a\"",
                ),
                (
                    "'abc'",
                    '"abc"',
                    "a'bc",
                    'a"bc',
                    r"a\\$",
                    r"a b",
                    r"a,",
                    ",",
                    ",,",
                    "",
                ),
            ),
            (
                "QUOTED_LITERAL",
                ("'abc'", '"a$"', "'#b'", "'\\#'", '"abc"', '"ab"c"', r'"a\""', "'()'"),
                ("'abc", 'abc"', "a\\$" "'a'bc'", ""),
            ),
            (
                "ARG_LIST",
                (
                    "abc,def,xyz",
                    "1,234,abc",
                    "1,'abc', xyz",
                    "1,'()'",
                    "1,'()',c,5a3,'$f()'",
                ),
                (",", " , " "a,,b", "a=1", "a=1,b", ""),
            ),
            (
                "KWARG_LIST",
                (
                    "abc=1, def=2,ghi=abc",
                    "abc=1",
                ),
                ("abc, def", "abc 1, def=2,ghi=abc", ""),
            ),
        ]:
            [
                self.assertTrue(
                    re.match("^" + str(getattr(mdl.pxs, name)) + "$", match)
                )
                for match in matches
            ]
            [
                self.assertFalse(
                    re.match("^" + str(getattr(mdl.pxs, name)) + "$", non_match)
                )
                for non_match in non_matches
            ]
