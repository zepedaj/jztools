import jztools.shared_memory as pgsm
import multiprocessing as mp
import climax as clx
import jztools.profiling as pgprof
import jztools.parallelization as pgpar
from contextlib import nullcontext
import numpy as np
from tqdm import tqdm


@clx.group()
def main():
    pass


def worker(arr, power, output=None, manager=None):

    if isinstance(arr, tuple):
        arr = np.ones(arr)

    # arr = arr.view(np.ndarray)

    if output == "none":
        out = None
    elif output == "xndarray":
        out = manager.empty(arr.shape)
    elif output == "ndarray":
        out = np.empty(arr.shape)
    else:
        raise Exception("Invalid input value.")

    for k in range(power):
        np.log(arr, out=out)

    return out


@main.command()
@clx.argument("--par-type", choices=["MOCK", "THREAD", "PROCESS"], default="MOCK")
@clx.argument("--num-workers", type=int, default=mp.cpu_count())
@clx.argument("--num-jobs", type=int, default=20)
@clx.argument("--shape", type=int, default=5000)
@clx.argument("--power", type=int, default=1)
@clx.argument(
    "--send", choices=(send_opts := ["ndarray", "xndarray", "none"]), default="ndarray"
)
@clx.argument("--receive", choices=send_opts, default="ndarray")
def compute(par_type, num_workers, num_jobs, shape, power, send, receive):

    with pgprof.Time() as total_time:
        try:
            with (
                pgsm.xSharedMemoryManager()
                if (send == "xndarray" or receive == "xndarray")
                else nullcontext(None)
            ) as manager:

                # Prepare arrays to send
                with pgprof.Time() as create_timer:
                    if send in ["xndarray", "ndarray"]:
                        _empty = np.empty if send == "ndarray" else manager.empty
                        arrays = [_empty((shape, shape)) for k in range(num_jobs)]
                    else:
                        arrays = [(shape, shape)] * num_jobs
                print(f"Array creation: {create_timer}.")

                # Set array data.
                ref_array = np.ones((shape, shape))
                with pgprof.Time() as to_shared_timer:
                    for arr in arrays:
                        arr[:] = ref_array[:]
                print(f"Writing arrays: {to_shared_timer}.")

                with pgprof.Time() as execute_timer:
                    par = pgpar.Parallelizer(par_type, max_workers=num_workers)
                    for args, out in tqdm(
                        par.run(worker, pgpar.ParArgs(arrays), power, receive, manager)
                    ):
                        if isinstance(out, pgpar.WorkerException):
                            raise out.error
                    print(f"Execution: {execute_timer}.")
        except KeyboardInterrupt:
            pass

    print("Total time: ", total_time)


@main.command()
@clx.argument("--num-arrays", type=int, default=20)
@clx.argument("--shape", type=int, default=5000)
def create(num_arrays, shape):

    ref_arr = np.empty((shape, shape))

    with pgprof.Time() as tmr:
        nd_arrays = [np.empty((shape, shape)) for k in range(num_arrays)]
    print(f"NDArray creation: {tmr}.")

    with pgprof.Time() as tmr:
        for arr in nd_arrays:
            arr[:] = ref_arr[:]
    print(f"NDArray write: {tmr}.")

    with pgsm.xSharedMemoryManager() as smm:
        with pgprof.Time() as tmr:
            xnd_arrays = [
                smm.empty((shape, shape), dtype="f") for k in range(num_arrays)
            ]
        print(f"xNDArray creation: {tmr}.")

        with pgprof.Time() as tmr:
            for arr in xnd_arrays:
                arr[:] = ref_arr[:]
        print(f"xNDArray write: {tmr}.")


if __name__ == "__main__":
    main()
