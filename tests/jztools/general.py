from jztools import general as mdl


def test_debouncer():
    out = []

    def fxn():
        out.append("Executed")

    debouncer = mdl.Debouncer(5)

    debouncer(fxn)
    debouncer(fxn)

    assert out == ["Executed"]
