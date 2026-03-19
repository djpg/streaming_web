import asyncio
import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "whisperlivekit" / "meeting_manager.py"
SPEC = importlib.util.spec_from_file_location("meeting_manager_for_tests", MODULE_PATH)
meeting_manager_module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(meeting_manager_module)
MeetingManager = meeting_manager_module.MeetingManager


class DummySocket:
    def __init__(self) -> None:
        self.messages = []

    async def send_json(self, payload):
        self.messages.append(payload)


def test_meeting_manager_merges_lines_and_buffers():
    async def run_test():
        manager = MeetingManager()
        socket_a = DummySocket()
        socket_b = DummySocket()

        await manager.register("room-1", socket_a, "antonio", "Antonio", "Seller", "Acme")
        await manager.register("room-1", socket_b, "pepe", "Pepe", "Buyer", "Beta")

        meeting = manager._meetings["room-1"]
        meeting.created_at = 100.0
        meeting.participants["antonio"].session_started_at = 100.0
        meeting.participants["pepe"].session_started_at = 102.0

        await manager.update_participant(
            "room-1",
            "antonio",
            {
                "lines": [{"speaker": 1, "text": "Hola Pepe", "start": "0:00:01.00", "end": "0:00:02.00"}],
                "buffer_transcription": "¿Me escuchas?",
                "remaining_time_transcription": 0.2,
            },
        )
        await manager.update_participant(
            "room-1",
            "pepe",
            {
                "lines": [{"speaker": 1, "text": "Sí, claro", "start": "0:00:00.50", "end": "0:00:01.40"}],
                "buffer_transcription": "",
                "remaining_time_transcription": 0.0,
            },
        )

        payload = socket_a.messages[-1]

        assert payload["type"] == "meeting_state"
        assert [line["speaker_name"] for line in payload["lines"]] == ["Antonio", "Pepe"]
        assert payload["lines"][0]["start"] == "0:00:01.00"
        assert payload["lines"][1]["start"] == "0:00:02.50"
        assert payload["active_buffers"] == [
            {
                "participant_id": "antonio",
                "speaker_name": "Antonio",
                "speaker_role": "Seller",
                "speaker_company": "Acme",
                "buffer_transcription": "¿Me escuchas?",
                "buffer_translation": "",
                "remaining_time_transcription": 0.2,
                "remaining_time_diarization": 0,
            }
        ]

    asyncio.run(run_test())


def test_unregister_marks_participant_offline_but_keeps_transcript():
    async def run_test():
        manager = MeetingManager()
        socket = DummySocket()

        await manager.register("room-2", socket, "antonio", "Antonio", "Seller", "Acme")
        await manager.update_participant(
            "room-2",
            "antonio",
            {"lines": [{"speaker": 1, "text": "Cierre", "start": "0:00:00.00", "end": "0:00:01.00"}]},
        )

        await manager.unregister("room-2", socket)

        meeting = manager._meetings["room-2"]
        assert meeting.participants["antonio"].connected is False

    asyncio.run(run_test())
