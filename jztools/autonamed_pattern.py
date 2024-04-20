import re
import jztools.validation as pgval
from contextlib import ExitStack
from dataclasses import dataclass, field
from threading import RLock
from typing import Optional, Tuple, Dict


def _n(name, expr):
    return f"(?P<{name}>{expr})"


@dataclass
class AutonamedPattern:
    """
    Represents a pattern with named groups that have sequential numbers automatically attached to them as suffixes. The numbers are guaranteed to be the same for tags that appear at the same nesting level. Other than that, no guarantees are provided about their order, except that it should be sequential. **Example:**

    .. testcode:: autonamed_pattern

        from soleil.solconf.autonamed_pattern import AutonamedPattern
        import re

        NESTED = AutonamedPattern('(?P<addend>[0-9])')
        str(NESTED)  # Advance the counter for illustration purposes.

        # 'my_letter' and 'my_value' are at the same nesting level; 'addend' is one level down.
        ap = AutonamedPattern(
            r'(?P<my_letter>[a-z]) \\= (?P<my_value>[0-9]) \\+ {NESTED}', vars())
        match = re.match(str(ap), 'a = 1 + 2')

        # Tags at the same nesting level have the same suffix identifier
        assert match.groupdict() == {'my_letter_0': 'a', 'my_value_0': '1', 'addend_1': '2'}

    The pattern can also contain placeholders for other :class:`AutonamedPattern` patterns using a syntax similar to the ``str.format`` syntax.

    .. warning::

      Calling the :meth:`AutonamedPattern.__str__` method (even implicitly through ``print(obj)``) will modify the object by advancing the counter. This enables support for situations where the same nested pattern is used more than once in the same expression, e.g., ``'{pattern}{pattern}'``.

      Use :meth:`AutonamedPattern.view` instead if you want to view the rendered auto-named pattern string without modifying the object.

    .. testcode:: autonamed_pattern

        # Simple auto-named pattern
        sp = AutonamedPattern('(?P<htag>Hello)')

        # Match simple pattern
        assert sp.view() == '(?P<htag_0>Hello)' # View the pattern w/o modifying the object
        assert re.match(str(sp), 'Hello') # Modifies the object
        assert sp.view() == '(?P<htag_1>Hello)'

        # Composite auto-named pattern
        cp = AutonamedPattern('{x} {x} {x} (?P<wtag>World)', {'x': sp})

        # Match composite pattern
        assert(
            cp.view() ==
            '(?P<htag_1>Hello) (?P<htag_2>Hello) (?P<htag_3>Hello) (?P<wtag_0>World)')
        assert re.match(
            str(cp), 'Hello Hello Hello World')
        assert (
            cp.view() ==
            '(?P<htag_4>Hello) (?P<htag_5>Hello) (?P<htag_6>Hello) (?P<wtag_1>World)')

    """

    pattern: str
    nested_patterns: Dict[str, "AutonamedPattern"] = field(default_factory=dict)
    names: Optional[Tuple[str]] = None
    _k: int = 0
    _lock: RLock = field(default_factory=RLock)
    _frozen: bool = False

    def next_name(self, name):
        """
        Generates the next auto-numbered name derived from ``name``.
        """
        return self.name_builder(name, self._k)

    @classmethod
    def name_builder(cls, name, identifier):
        """
        Generates the name derived from ``name`` for the given ``identifier``. Derived classes wishing to modify the auto-named string format should overload this method.
        """
        return f"{name}_{identifier}"

    @classmethod
    def get_derived_tags(cls, base_tag: str, match: re.Match):
        tag_pattern = re.compile(cls.name_builder(base_tag, r"\d+"))
        return [x for x in match.groupdict().keys() if re.fullmatch(tag_pattern, x)]

    @classmethod
    def get_single(cls, base_tag: str, match: re.Match):
        """
        Returns the value of id-suffixed version of ``base_tag``, checking first that a single such tag exists in ``match``.
        """
        return match[cls.get_single_tag(base_tag, match)]

    @classmethod
    def get_single_tag(cls, base_tag: str, match: re.Match):
        """
        Returns the id-suffixed version of ``base_tag``, and checks that a single such tag exists in ``match``.
        """
        return pgval.checked_get_single(
            AutonamedPattern.get_derived_tags(base_tag, match)
        )

    def view(self):
        """
        Compiles the pattern to a string and without advancing the counters.
        """
        with ExitStack() as stack:
            all_patterns = [self] + list(self.nested_patterns.values())
            [
                stack.enter_context(x._lock)
                for x in all_patterns
                if not isinstance(x, str)
            ]
            all_ks = [(x, x._k) for x in all_patterns]
            try:
                return str(self)
            finally:
                for x, _k in all_ks:
                    x._k = _k

    def __str__(self):
        """
        Compiles the pattern to a string and advances the counter.
        """

        with self._lock:

            # Replace names in pattern
            NAMES = (
                "|".join(re.escape(x) for x in self.names)
                if self.names
                else r"[a-zA-Z]\w*"
            )

            # Substitute groups
            out = self.pattern
            for GROUP_PREFIX, GROUP_SUFFIX in [
                ("(?P<", ">"),  # Replaces (?P<name> by (?P<name_k>
                ("(?P=", ")"),  # Replaces (?P=name) by (?P=name_k)
                ("(?(", ")"),  # Replaces (?P=name) by (?P=name_k)
            ]:
                out = re.sub(
                    f"(?P<prefix>({pxs.EVEN_SLASHES}|^|[^{pxs.SLASH}]))"
                    f"{re.escape(GROUP_PREFIX)}(?P<name>{NAMES}){re.escape(GROUP_SUFFIX)}",
                    lambda x: f"{x['prefix']}{GROUP_PREFIX}{self.next_name(x['name'])}{GROUP_SUFFIX}",
                    out,
                )

            # Replace nested patterns
            out = out.format(**self.nested_patterns)

            #
            if not self._frozen:
                self._k += 1

            return out


class pxs:
    """
    Contains base regex patterns.
    """

    # abc0, _abc0 (not 0abc)
    VARNAME = r"[a-zA-Z_]+\w*"

    # Matches VARNAME's and fully qualified versions
    # abc0.def1 (not abc.0abc, abc., abc.def1. )
    NS_VARNAME = AutonamedPattern(r"{VARNAME}(\.{VARNAME})*(?!\.)(?!\w)", vars())

    # \\, \\\\, \\\\\\, not \, \\\
    SLASH = r"\\"
    EVEN_SLASHES = r"(?<!\\)(\\\\)*"
    ODD_SLASHES = r"\\(\\\\)*"

    # $abc, \\$abc, \\\\$abc.def
    ATTR = AutonamedPattern(
        r"(?P<slash>{EVEN_SLASHES})\$(?P<name>{NS_VARNAME})", vars()
    )

    # abc, a\$, \#b, \'a, a\"bc, a\, not 'abc', "abc", a'bc, a"bc, a\\, a,$
    UNQUOTED_LITERAL = (
        "("
        # No un-escaped spaces, $, #, =, ', ", (, ), \
        "[^" + (_qchars := r"\s\$\#\=\'\"\,\(\)\\" + "]|") +
        # A sequence of escape sequences
        f"({ODD_SLASHES}[{_qchars}])+"
        ")+"
    )

    # 'abc', "a$", '#b', '\#', "abc", not 'abc, abc", a\\$ 'a'bc'
    # r'(?P<q>(?P<sq>\')|(?P<dq>\"))((?(sq)\"|\')|[^\\])*(?P=q)'
    QUOTED_LITERAL = AutonamedPattern(
        r"(?P<q>(?P<sq>\')|(?P<dq>\"))("
        # Non-quote
        + (non_quote := r"(?(sq)\"|\')" "|") +
        # Non-slash
        r"[^\\]" "|"
        # Odd-slashes-escaped non-slash char (including quote)
        r"{ODD_SLASHES}[^\\]" "|"
        # Even number of slashes followed by non-quote
        "{EVEN_SLASHES}" + non_quote + ")*?(?P=q)",
        vars(),
    )
    # $fxnname(arg), $fxnname(arg0, arg1), $fxnname(arg0, ... kwarg0=val0, ...)
    #

    # A string literal
    # "abc, def, '123' "
    LITERAL = AutonamedPattern("({UNQUOTED_LITERAL}|{QUOTED_LITERAL})", vars())

    # Arguments in argument lists are interpreted as YAML strings.
    LITERAL_ARG = LITERAL
    ARG_LIST = AutonamedPattern(r"({LITERAL_ARG}(\s*,\s*{LITERAL_ARG})*)", vars())

    LITERAL_KWARG = AutonamedPattern(
        r"(?P<kw_name>{VARNAME})\s*=\s*(?P<kw_val>{LITERAL})", vars()
    )
    KWARG_LIST = AutonamedPattern(
        ARG_LIST.pattern.format(LITERAL_ARG="{LITERAL_KWARG}"), vars()
    )

    # Matches a function call where all arguments have been resolved (i.e., a non-nested function call).
    FXN_CALL = AutonamedPattern(
        r"\$(?P<fxn>{NS_VARNAME})\("
        r"\s*(?P<arg_list>{ARG_LIST})?\s*((?(arg_list),\s*)(?P<kwarg_list>{KWARG_LIST}))?\s*"
        r"\)",
        vars(),
    )
