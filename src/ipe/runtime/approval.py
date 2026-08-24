"""Desktop approval prompts for ambiguous ROS command topics."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import shutil
import subprocess
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

log = logging.getLogger(__name__)

ApprovalDecision = Literal["approve", "reject", "unavailable"]


@dataclass(frozen=True)
class ApprovalRequest:
    proposal_id: str
    robot_id: str
    interface: str
    msg_type: str
    preview: str


def control_approval_id(robot_id: str, interface: str, msg_type: str) -> str:
    """Return a stable identifier that invalidates approval on type changes."""
    identity = "\0".join((robot_id, interface, msg_type))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return f"control_{digest}"


def command_preview(payload: dict[str, Any], limit: int = 800) -> str:
    """Serialize a bounded payload preview for the approval dialog and CSE event."""
    rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if len(rendered) <= limit:
        return rendered
    return rendered[: limit - 3] + "..."


class DesktopApprovalPrompt:
    """Show one non-blocking desktop question per ambiguous command topic."""

    def __init__(
        self,
        on_result: Callable[[ApprovalRequest, ApprovalDecision], None],
        *,
        runner: Callable[[ApprovalRequest], ApprovalDecision] | None = None,
    ) -> None:
        self._on_result = on_result
        self._runner = runner or self._show_dialog
        self._queue: queue.Queue[ApprovalRequest | None] = queue.Queue()
        self._pending: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._process: subprocess.Popen[Any] | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="ipe-control-approval",
            daemon=True,
        )
        self._thread.start()

    def request(self, request: ApprovalRequest) -> bool:
        """Queue a prompt unless the same topic already has one pending."""
        with self._lock:
            if self._stop.is_set() or request.proposal_id in self._pending:
                return False
            self._pending.add(request.proposal_id)
        self._queue.put(request)
        return True

    def close(self) -> None:
        self._stop.set()
        self._queue.put(None)
        with self._lock:
            process = self._process
        if process is not None and process.poll() is None:
            process.terminate()
        self._thread.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            request = self._queue.get()
            if request is None or self._stop.is_set():
                return
            try:
                decision = self._runner(request)
            except Exception:
                log.exception("control approval dialog failed")
                decision = "unavailable"
            with self._lock:
                self._pending.discard(request.proposal_id)
            self._on_result(request, decision)

    def _show_dialog(self, request: ApprovalRequest) -> ApprovalDecision:
        if not os.environ.get("DISPLAY"):
            return "unavailable"
        text = (
            "oneM2M requested access to a new ROS 2 command topic.\n\n"
            f"Robot: {request.robot_id}\n"
            f"Topic: {request.interface}\n"
            f"Type: {request.msg_type}\n\n"
            f"First command: {request.preview}\n\n"
            "Allow the command path for this topic?\n"
            "The first command was discarded. Send a new command after approval."
        )
        command = self._dialog_command("ROS 2 Command Approval", text)
        if command is None:
            return "unavailable"
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with self._lock:
            self._process = process
        try:
            while process.poll() is None:
                if self._stop.wait(0.2):
                    process.terminate()
                    return "unavailable"
            if process.returncode == 0:
                return "approve"
            if process.returncode == 1:
                return "reject"
            return "unavailable"
        finally:
            with self._lock:
                if self._process is process:
                    self._process = None

    @staticmethod
    def _dialog_command(title: str, message: str) -> list[str] | None:
        """Build a desktop dialog command for the available GUI toolkit."""
        executable = shutil.which("zenity")
        if executable is not None:
            return [
                executable,
                "--question",
                f"--title={title}",
                f"--text={message}",
                "--ok-label=Allow",
                "--cancel-label=Deny",
                "--width=560",
            ]

        tkinter_script = (
            "import sys\n"
            "try:\n"
            " import tkinter as tk\n"
            " root = tk.Tk()\n"
            " root.title(sys.argv[1])\n"
            " root.resizable(False, False)\n"
            " root.attributes('-topmost', True)\n"
            " result = {'code': 1}\n"
            " def finish(code):\n"
            "  result['code'] = code\n"
            "  root.destroy()\n"
            " tk.Label(root, text=sys.argv[2], justify='left', wraplength=540, padx=24, pady=20).pack()\n"
            " buttons = tk.Frame(root)\n"
            " buttons.pack(pady=(0, 18))\n"
            " tk.Button(buttons, text='Allow', width=12, command=lambda: finish(0)).pack(side='left', padx=8)\n"
            " deny = tk.Button(buttons, text='Deny', width=12, command=lambda: finish(1))\n"
            " deny.pack(side='left', padx=8)\n"
            " deny.focus_set()\n"
            " root.protocol('WM_DELETE_WINDOW', lambda: finish(1))\n"
            " root.bind('<Escape>', lambda _event: finish(1))\n"
            " root.update_idletasks()\n"
            " x = (root.winfo_screenwidth() - root.winfo_width()) // 2\n"
            " y = (root.winfo_screenheight() - root.winfo_height()) // 2\n"
            " root.geometry(f'+{x}+{y}')\n"
            " root.mainloop()\n"
            "except Exception:\n"
            " raise SystemExit(2)\n"
            "raise SystemExit(result['code'])\n"
        )
        return [sys.executable, "-c", tkinter_script, title, message]


__all__ = [
    "ApprovalDecision",
    "ApprovalRequest",
    "DesktopApprovalPrompt",
    "command_preview",
    "control_approval_id",
]
