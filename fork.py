import atexit
import contextlib
import os
import signal

class Jobserver:
    """
    A GNU Make-ish jobserver using a POSIX pipe to track available job slots.

    All forked children will inherit the same collection of available slots.
    See https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html
    """
    def __init__(self, slots: int):
        assert slots >= 0
        self.r, self.w = os.pipe2(os.O_CLOEXEC)  # Blocking!
        os.set_inheritable(self.r, True)  # Per PEP-446
        os.set_inheritable(self.w, True)  # Per PEP-446
        os.write(self.w, slots * b'x')  # Mark all slots available

    @contextlib.contextmanager
    def slot(self) -> None:
        bs = []
        while not bs:
            bs.extend(bytes((b,)) for b in os.read(self.r, 1))
        def cleanup():
            while bs:
                os.write(self.w, bs.pop())
        atexit.register(cleanup)
        try:
            yield
        finally:
            cleanup()
            atexit.unregister(cleanup)


def foo(js):
    with js.slot():
        print('Hola')


if __name__ == '__main__':
    print('Hello')
    js = Jobserver(3)
    import multiprocessing as mp
    ctx = mp.get_context('fork')
    p = ctx.Process(target=foo, args=(js,))
    p.start()

