from jztools.parallelization.threading import outsourced_callable as mdl


class TestThreadOutsourcedCallable:
    def test_docs(self, capsys):

        #### Docs code
        from jztools.parallelization.threading import ThreadOutsourcedCallable
        from concurrent.futures import ThreadPoolExecutor

        def my_callable(value):
            print("Output of `my_callable`:", value)

        with ThreadPoolExecutor() as executor:

            wrapped_callable = ThreadOutsourcedCallable(executor, my_callable)
            wrapped_callable(5)

            wrapped_callable.wait()
        ###############

        captured = capsys.readouterr()

        assert captured.out == "Output of `my_callable`: 5\n"
