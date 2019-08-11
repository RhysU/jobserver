import contextlib
import enum
import itertools
import multiprocessing
import os
import queue
import sys

class Jobserver:
    def __init__(self, context, slots: int):
        # Prepare required resources
        assert slots >= 0
        self.context = context
        self.slots = self.context.Queue(maxsize=slots)

        # Issue one token for each requested slot
        for i in range(slots):
            self.slots.put(block=False)

    def submit(self, fn, *args, **kwargs) -> 'Jobserver.Future':
        # Consume one slot prior to acquiring any resources
        token = self.slots.get(block=True)
        try:
            # Prepare required resources
            queue = self.context.Queue()
            args += (slots, token, queue, fn)
            process = self.context.Process(target=Jobserver._run,
                                           args=args,
                                           kwargs=kwargs,
                                           daemon=False)
            # A process that successfully starts now responsible for token
            process.start()
        except:
            self.slots.put(token, block=True)
            raise

        # Wrap up necessary details for the caller
        return self.Future(process, queue)

    class Future:
        def __init__(self, process, queue):
            assert self.process is not None
            assert self.queue is not None
            self.process = process
            self.queue = queue
            self.result = None

        def done() -> bool:
            if self.queue is not None:
                try:
                    self.result = self.queue.get(block=False)
                    self.queue = None
                    self.process.join()
                    self.process = None
                except queue.Empty:
                    return False

            return True

        def result(block=True, timeout=None):
            # May throw queue.Empty when non-blocking requested
            if self.queue is not None:
                self.result = self.queue.get(block=block, timeout=timeout)
                self.queue = None
                self.process.join()
                self.process = None

            # Raise any exception that occurred
            if isinstance(self.result, Exception):
                raise self.result

            # Return
            return result

    # TODO preexec_hook?
    @staticmethod
    def _run(slots, token, queue, fn, *args, **kwargs) -> None:
        try:
            result = fn(*args, **kwargs)
            queue.put(result, block=True)
        except queue.Full as e:
            raise RuntimeError('Logic error') from e
        except Exception as e:
            queue.put(e)
        finally:
            slots.put(token, block=True)


# FIXME START HERE
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

