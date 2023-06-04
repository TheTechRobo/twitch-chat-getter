"""
Creates a `signal_fence` context manager that will delay SIGINT
until after the context manager closes.

Shamelessly stolen from https://stackoverflow.com/a/71330357/9654083
"""

import os
import signal
from contextlib import contextmanager
from types import FrameType
from typing import Callable, Iterator, Optional, Tuple
from typing_extensions import assert_never


@contextmanager
def signal_fence(
    signum: signal.Signals,
    *,
    on_deferred_signal: Callable[[int, Optional[FrameType]], None] = None,
) -> Iterator[None]:
    """
    A `signal_fence` creates an uninterruptible "fence" around a block of code. The
    fence defers a specific signal received inside of the fence until the fence is
    destroyed, at which point the original signal handler is called with the deferred
    signal. Multiple deferred signals will result in a single call to the original
    handler. An optional callback `on_deferred_signal` may be specified which will be
    called each time a signal is handled while the fence is active, and can be used
    to print a message or record the signal.

    A `signal_fence` guarantees the following with regards to exception-safety:

    1. If an exception occurs prior to creating the fence (installing a custom signal
    handler), the exception will bubble up as normal. The code inside of the fence will
    not run.
    2. If an exception occurs after creating the fence, including in the fenced code,
    the original signal handler will always be restored before the exception bubbles up.
    3. If an exception occurs while the fence is calling the original signal handler on
    destruction, the original handler may not be called, but the original handler will
    be restored. The exception will bubble up and can be detected by calling code.
    4. If an exception occurs while the fence is restoring the original signal handler
    (exceedingly rare), the original signal handler will be restored regardless.
    5. No guarantees about the fence's behavior are made if exceptions occur while
    exceptions are being handled.

    A `signal_fence` can only be used on the main thread, or else a `ValueError` will
    raise when entering the fence.
    """
    handled: Optional[Tuple[int, Optional[FrameType]]] = None

    def handler(signum: int, frame: Optional[FrameType]) -> None:
        nonlocal handled
        if handled is None:
            handled = (signum, frame)
        if on_deferred_signal is not None:
            try:
                on_deferred_signal(signum, frame)
            except:
                pass

    # https://docs.python.org/3/library/signal.html#signal.getsignal
    original_handler = signal.getsignal(signum)
    if original_handler is None:
        raise TypeError(
            "signal_fence cannot be used with signal handlers that were not installed"
            " from Python"
        )
    if isinstance(original_handler, int) and not isinstance(
        original_handler, signal.Handlers
    ):
        raise NotImplementedError(
            "Your Python interpreter's signal module is using raw integers to"
            " represent SIG_IGN and SIG_DFL, which shouldn't be possible!"
        )

    # N.B. to best guarantee the original handler is restored, the @contextmanager
    #      decorator is used rather than a class with __enter__/__exit__ methods so
    #      that the installation of the new handler can be done inside of a try block,
    #      whereas per [PEP 343](https://www.python.org/dev/peps/pep-0343/) the
    #      __enter__ call is not guaranteed to have a corresponding __exit__ call if an
    #      exception interleaves
    try:
        try:
            signal.signal(signum, handler)
            yield
        finally:
            if handled is not None:
                if isinstance(original_handler, signal.Handlers):
                    if original_handler is signal.Handlers.SIG_IGN:
                        pass
                    elif original_handler is signal.Handlers.SIG_DFL:
                        signal.signal(signum, signal.SIG_DFL)
                        os.kill(os.getpid(), signum)
                    else:
                        assert_never(original_handler)
                elif callable(original_handler):
                    original_handler(*handled)
                else:
                    assert_never(original_handler)
            signal.signal(signum, original_handler)
    except:
        signal.signal(signum, original_handler)
        raise
