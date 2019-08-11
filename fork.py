import atexit
import contextlib
import multiprocessing
import os
import signal

class Jobserver:
    """
    A GNU Make-ish jobserver using a POSIX pipe to track available job slots.

    All forked children will inherit the same collection of available slots.
    See https://www.gnu.org/software/make/manual/html_node/POSIX-Jobserver.html
    """
    def __init__(self, context, slots: int):
        assert slots >= 0
        self.r, self.w = context.Pipe()  # Cannot be garbage collected
        os.write(self.w.fileno(), slots * b'x')  # Mark all slots available

    @contextlib.contextmanager
    def slot(self) -> None:
        rfd, wfd = self.r.fileno(), self.w.fileno()
        bs = []
        while not bs:
            bs.extend(bytes((b,)) for b in os.read(rfd, 1))
        def cleanup():
            while bs:
                os.write(wfd, bs.pop())
        atexit.register(cleanup)
        try:
            yield
        finally:
            cleanup()
            atexit.unregister(cleanup)


def foo(js):
    with js.slot():
        print('Hello!')


if __name__ == '__main__':
    for context_type in ('fork', 'forkserver'):
        print('Hi {}?'.format(context_type))
        context = multiprocessing.get_context(context_type)
        js = Jobserver(context, 3)
        p = context.Process(target=foo, args=(js,))
        p.start()
        p.join()

