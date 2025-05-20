"""
Type hints for Native Rust Extension
"""

from abc import ABCMeta
from typing import Any, Final

class ReifyDB:
    def __init__(self) -> None: ...
    def query(sql: str): ...

