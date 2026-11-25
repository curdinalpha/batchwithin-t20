"""batchWithin(K, D): race-free chunking with a fixed (never reset) window deadline."""
from .core import BatchWithin, Emission, State, ProtocolError
from .naive import NaiveBatchWithin
from .stream import batch_within, BatchWithinStream
from .vclock import VirtualTimeLoop, run_virtual

__all__ = ["BatchWithin", "Emission", "State", "ProtocolError", "NaiveBatchWithin",
           "batch_within", "BatchWithinStream", "VirtualTimeLoop", "run_virtual"]
