from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from ipe.ir import TopicIR


class AdapterBase(ABC):
    @abstractmethod
    def discover(self) -> list[tuple[str, str]]:
        ...

    @abstractmethod
    def bind_topics(self, on_topic_ir: Callable[[TopicIR], None]) -> None:
        ...

    @abstractmethod
    def shutdown(self) -> None:
        ...
