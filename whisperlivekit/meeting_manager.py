import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from fastapi import WebSocket
else:
    WebSocket = Any


def parse_time_str(time_str: str) -> float:
    """Parse H:MM:SS.cc strings emitted by websocket payloads."""
    parts = time_str.split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    if len(parts) == 2:
        return int(parts[0]) * 60 + float(parts[1])
    return float(parts[0])


def format_time(seconds: float) -> str:
    """Format seconds as H:MM:SS.cc."""
    total_cs = int(round(seconds * 100))
    cs = total_cs % 100
    total_s = total_cs // 100
    s = total_s % 60
    total_m = total_s // 60
    m = total_m % 60
    h = total_m // 60
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


@dataclass
class ParticipantProfile:
    participant_id: str
    name: str
    role: str
    company: str
    connected: bool = True
    session_started_at: float = field(default_factory=time.time)
    latest_payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "participant_id": self.participant_id,
            "name": self.name,
            "role": self.role,
            "company": self.company,
            "connected": self.connected,
        }


@dataclass
class MeetingState:
    meeting_id: str
    created_at: float = field(default_factory=time.time)
    participants: Dict[str, ParticipantProfile] = field(default_factory=dict)
    sockets: Dict[WebSocket, str] = field(default_factory=dict)


class MeetingManager:
    """Coordinates multi-participant meeting transcripts in memory."""

    def __init__(self) -> None:
        self._meetings: Dict[str, MeetingState] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        meeting_id: str,
        websocket: WebSocket,
        participant_id: str,
        name: str,
        role: str,
        company: str,
    ) -> None:
        async with self._lock:
            meeting = self._meetings.setdefault(meeting_id, MeetingState(meeting_id=meeting_id))
            participant = meeting.participants.get(participant_id)
            if participant is None:
                participant = ParticipantProfile(
                    participant_id=participant_id,
                    name=name,
                    role=role,
                    company=company,
                )
                meeting.participants[participant_id] = participant
            else:
                participant.name = name
                participant.role = role
                participant.company = company
                participant.connected = True
                participant.session_started_at = time.time()

            meeting.sockets[websocket] = participant_id

        await self.broadcast(meeting_id)

    async def unregister(self, meeting_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            meeting = self._meetings.get(meeting_id)
            if meeting is None:
                return

            participant_id = meeting.sockets.pop(websocket, None)
            if participant_id and participant_id in meeting.participants:
                participant = meeting.participants[participant_id]
                participant.connected = participant_id in meeting.sockets.values()

            has_connected_sockets = bool(meeting.sockets)
            has_transcript = any(
                participant.latest_payload.get("lines") or participant.latest_payload.get("buffer_transcription")
                for participant in meeting.participants.values()
            )

            if not has_connected_sockets and not has_transcript:
                self._meetings.pop(meeting_id, None)
                return

        await self.broadcast(meeting_id)

    async def update_participant(self, meeting_id: str, participant_id: str, payload: Dict[str, Any]) -> None:
        async with self._lock:
            meeting = self._meetings.get(meeting_id)
            if meeting is None:
                return

            participant = meeting.participants.get(participant_id)
            if participant is None:
                return

            participant.latest_payload = payload

        await self.broadcast(meeting_id)

    async def broadcast(self, meeting_id: str) -> None:
        async with self._lock:
            meeting = self._meetings.get(meeting_id)
            if meeting is None:
                return
            payload = self._build_payload(meeting)
            sockets = list(meeting.sockets.keys())

        disconnected: List[WebSocket] = []
        for websocket in sockets:
            try:
                await websocket.send_json(payload)
            except Exception:
                disconnected.append(websocket)

        if disconnected:
            async with self._lock:
                meeting = self._meetings.get(meeting_id)
                if meeting is None:
                    return
                for websocket in disconnected:
                    participant_id = meeting.sockets.pop(websocket, None)
                    if participant_id and participant_id in meeting.participants:
                        participant = meeting.participants[participant_id]
                        participant.connected = participant_id in meeting.sockets.values()

    def _build_payload(self, meeting: MeetingState) -> Dict[str, Any]:
        meeting_started_at = min(
            [meeting.created_at] + [participant.session_started_at for participant in meeting.participants.values()]
        )
        lines = self._build_lines(meeting, meeting_started_at)
        active_buffers = self._build_active_buffers(meeting)
        return {
            "type": "meeting_state",
            "meeting_id": meeting.meeting_id,
            "participants": [participant.to_dict() for participant in meeting.participants.values()],
            "lines": lines,
            "active_buffers": active_buffers,
            "status": "active_transcription" if lines or active_buffers else "no_audio_detected",
        }

    def _build_lines(self, meeting: MeetingState, meeting_started_at: float) -> List[Dict[str, Any]]:
        merged: List[tuple[float, float, Dict[str, Any]]] = []
        for participant in meeting.participants.values():
            for line in participant.latest_payload.get("lines", []):
                start_seconds = parse_time_str(line.get("start", "0:00:00.00"))
                end_seconds = parse_time_str(line.get("end", "0:00:00.00"))
                absolute_start = participant.session_started_at + start_seconds
                absolute_end = participant.session_started_at + end_seconds
                merged.append(
                    (
                        absolute_start,
                        absolute_end,
                        {
                            **line,
                            "participant_id": participant.participant_id,
                            "speaker_name": participant.name,
                            "speaker_role": participant.role,
                            "speaker_company": participant.company,
                            "start": format_time(max(0.0, absolute_start - meeting_started_at)),
                            "end": format_time(max(0.0, absolute_end - meeting_started_at)),
                        },
                    )
                )

        merged.sort(key=lambda item: (item[0], item[1], item[2]["speaker_name"]))
        return [item[2] for item in merged]

    def _build_active_buffers(self, meeting: MeetingState) -> List[Dict[str, Any]]:
        buffers: List[Dict[str, Any]] = []
        for participant in meeting.participants.values():
            payload = participant.latest_payload
            buffer_text = payload.get("buffer_transcription", "")
            buffer_translation = payload.get("buffer_translation", "")
            if not buffer_text and not buffer_translation:
                continue

            buffers.append(
                {
                    "participant_id": participant.participant_id,
                    "speaker_name": participant.name,
                    "speaker_role": participant.role,
                    "speaker_company": participant.company,
                    "buffer_transcription": buffer_text,
                    "buffer_translation": buffer_translation,
                    "remaining_time_transcription": payload.get("remaining_time_transcription", 0),
                    "remaining_time_diarization": payload.get("remaining_time_diarization", 0),
                }
            )

        return buffers
