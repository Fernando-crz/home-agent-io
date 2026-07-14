import queue
import pyaudio
import numpy as np
from typing import Dict, Optional, Tuple, Any, TypedDict, Literal, cast
from redis.asyncio import Redis
import sys
import asyncio
import logging

from src.audio_utils import apply_volume, clip_buffer_16_bits
from src.config import settings

logger = logging.getLogger(__name__)

class AudioMixerEvent(TypedDict):
    event: Literal["started", "finished"]
    channel: str

class AudioMixerEventNotifier:
    def __init__(self, redis_provider: Redis, stream_name: str, loop: asyncio.AbstractEventLoop):
        self.redis_provider = redis_provider
        self.stream_name = stream_name
        self.loop = loop

    async def _send_to_redis(self, event_data: AudioMixerEvent):
        try:
            await self.redis_provider.xadd(
                self.stream_name,
                cast(Dict, event_data),
                maxlen=1000,
                approximate=True,
            )
        except Exception as e:
            logger.error(
                f"[Mixer Error] Failed to publish event to Redis: {e}"
            )

    def _notify(self, event_data: AudioMixerEvent):
        asyncio.run_coroutine_threadsafe(
            self._send_to_redis(event_data), 
            self.loop
        )

    def notify_audio_started(self, channel_name: str):
        self._notify({"event": "started", "channel": channel_name})

    def notify_audio_finished(self, channel_name: str):
        self._notify({"event": "finished", "channel": channel_name})
    

class ChannelConfig(TypedDict):
    queue: queue.Queue
    volume: float
    paused: bool
    active_playback: bool

class AudioMixerChannelsConfig(TypedDict):
    music: ChannelConfig
    agent: ChannelConfig
    notification: ChannelConfig

class AudioMixerMasterConfig(TypedDict):
    volume: float
    paused: bool

class AudioMixer:
    def __init__(self, event_notifier: AudioMixerEventNotifier):
        self.event_notifier = event_notifier

        self.channels: AudioMixerChannelsConfig = self._get_default_channels_config()
        self.master: AudioMixerMasterConfig = self._get_default_master_config()

        self._setup_pyaudio()

    def _get_default_channels_config(self) -> AudioMixerChannelsConfig:
        return {
            "music": {
                "queue": queue.Queue(),
                "volume": settings.AUDIO_MIXER.MUSIC_DEFAULT_VOLUME,
                "paused": False,
                "active_playback": False,
            },
            "agent": {
                "queue": queue.Queue(),
                "volume": settings.AUDIO_MIXER.AGENT_DEFAULT_VOLUME,
                "paused": False,
                "active_playback": False,
            },
            "notification": {
                "queue": queue.Queue(),
                "volume": settings.AUDIO_MIXER.NOTIFICATION_DEFAULT_VOLUME,
                "paused": False,
                "active_playback": False,
            },
        }

    def _get_default_master_config(self) -> AudioMixerMasterConfig:
        return {
            "volume": settings.AUDIO_MIXER.MASTER_DEFAULT_VOLUME,
            "paused": False
        }

    def _setup_pyaudio(self):
        self.p = pyaudio.PyAudio()
        self.stream = self.p.open(
            format=pyaudio.paInt16,
            channels=1,
            rate=settings.SPEAKER.RATE,
            output=True,
            frames_per_buffer=settings.SPEAKER.CHUNK_SIZE,
            stream_callback=cast(Any, self._audio_callback),
        )

    def _audio_callback(
            self,
            in_data: Optional[bytes], 
            frame_count: int, 
            time_info: Dict[str, float], 
            status: int
        ) -> Tuple[Optional[bytes], int]:
        mixed_buffer = np.zeros(frame_count, dtype=np.int32)

        if self.master["paused"]:
            # no sound is played or queue updated, just return empty array
            return (mixed_buffer.astype(np.int16).tobytes(), pyaudio.paContinue)

        for name, ch in self.channels.items():
            ch = cast(ChannelConfig, ch)
            
            if ch["paused"]:
                continue

            try:
                chunk_array = ch["queue"].get_nowait() # falls into exception if queue empty.

                if not ch["active_playback"]:
                    ch["active_playback"] = True
                    self.event_notifier.notify_audio_started(name)

                mixed_buffer += apply_volume(chunk_array, ch["volume"])

            except queue.Empty:
                if ch["active_playback"]:
                    ch["active_playback"] = False
                    self.event_notifier.notify_audio_finished(name)

        final_buffer = clip_buffer_16_bits(mixed_buffer)
        return (final_buffer.tobytes(), pyaudio.paContinue)


    def submit_audio_chunk(self, channel_name: str, raw_bytes: bytes):
        if channel_name not in self.channels:
            return
        
        array_data = np.frombuffer(raw_bytes, dtype=np.int16)
        self.channels[channel_name]["queue"].put(array_data)

    def stop_channel(self, channel_name: str):
        if channel_name not in self.channels:
            return
        
        ch = cast(ChannelConfig, self.channels[channel_name])
        q = ch["queue"]
        with q.mutex:
            q.queue.clear()
    
    def set_paused_channel(self, channel_name: str, is_paused: bool):
        if channel_name not in self.channels:
            return
        
        ch = cast(ChannelConfig, self.channels[channel_name])
        ch["paused"] = is_paused
    
    def pause_channel(self, channel_name: str):
        self.set_paused_channel(channel_name, True)
    
    def resume_channel(self, channel_name: str):
        self.set_paused_channel(channel_name, False)
    
    def set_volume_channel(self, channel_name: str, volume: float):
        if channel_name in self.channels:
            self.channels[channel_name]["volume"] = max(0.0, min(volume, 1.0))
    
    def close(self):
        self.stream.stop_stream()
        self.stream.close()
        self.p.terminate()


def main():
    pass

if __name__ == "__main__":
    main()