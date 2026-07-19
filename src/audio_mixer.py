import queue
import pyaudio
import numpy as np
from typing import Dict, Optional, Tuple, Any, TypedDict, Literal, cast
from redis.asyncio import Redis
import asyncio
import logging
from dataclasses import dataclass

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
    
    async def close(self):
        await self.redis_provider.close()

@dataclass
class ChannelConfig:
    name: Literal["music", "agent", "notification"]
    queue: queue.Queue
    volume: float
    paused: bool
    active_playback: bool

@dataclass
class AudioMixerChannelsConfig:
    music: ChannelConfig
    agent: ChannelConfig
    notification: ChannelConfig


    def __contains__(self, channel_name: str) -> bool:
        return channel_name in ["music", "agent", "notification"]
    
    def __iter__(self):
        for channel in [self.music, self.agent, self.notification]:
            yield channel

    def get_channel(self, channel_name: str) -> Optional[ChannelConfig]:
        if channel_name not in self:
            return None

        if channel_name == "music": 
            return self.music
        if channel_name == "agent": 
            return self.agent
        if channel_name == "notification": 
            return self.notification

@dataclass
class AudioMixerMasterConfig:
    volume: float
    paused: bool

class AudioMixer:
    def __init__(self, event_notifier: AudioMixerEventNotifier, pyaudio_instance: pyaudio.PyAudio):
        self.event_notifier = event_notifier
        self.pyaudio_instance = pyaudio_instance

        self.channels: AudioMixerChannelsConfig = self._get_default_channels_config()
        self.master: AudioMixerMasterConfig = self._get_default_master_config()

        self._setup_pyaudio()

    def _get_default_channels_config(self) -> AudioMixerChannelsConfig:
        music = ChannelConfig(
            name="music",
            queue=queue.Queue(),
            volume=settings.AUDIO_MIXER.MUSIC_DEFAULT_VOLUME,
            paused=False,
            active_playback=False,
        )
        agent = ChannelConfig(
            name="agent",
            queue=queue.Queue(),
            volume=settings.AUDIO_MIXER.AGENT_DEFAULT_VOLUME,
            paused=False,
            active_playback=False,
        )
        notification = ChannelConfig(
            name="notification",
            queue=queue.Queue(),
            volume=settings.AUDIO_MIXER.NOTIFICATION_DEFAULT_VOLUME,
            paused=False,
            active_playback=False,
        )
        
        return AudioMixerChannelsConfig(
            music=music,
            agent=agent,
            notification=notification
        )

    def _get_default_master_config(self) -> AudioMixerMasterConfig:
        return AudioMixerMasterConfig(
            volume=settings.AUDIO_MIXER.MASTER_DEFAULT_VOLUME,
            paused=False,
        )

    def _setup_pyaudio(self):
        self.stream = self.pyaudio_instance.open(
            format=pyaudio.paInt16,
            channels=settings.SPEAKER.CHANNELS,
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

        if self.master.paused:
            # no sound is played or queue updated, just return empty array
            return (mixed_buffer.astype(np.int16).tobytes(), pyaudio.paContinue)

        for channel in self.channels:
            
            if channel.paused:
                continue

            if not channel.active_playback:
                if channel.queue.qsize() < settings.AUDIO_MIXER.PRE_BUFFER_COUNT:
                    continue 
                else:
                    channel.active_playback = True
                    self.event_notifier.notify_audio_started(channel.name)

            try:
                chunk_array = channel.queue.get_nowait() # falls into exception if queue empty.
                mixed_buffer += apply_volume(chunk_array, channel.volume)

            except queue.Empty:
                if channel.active_playback:
                    channel.active_playback = False
                    self.event_notifier.notify_audio_finished(channel.name)

        final_buffer = clip_buffer_16_bits(mixed_buffer)
        return (final_buffer.tobytes(), pyaudio.paContinue)


    def submit_audio_chunk(self, channel_name: str, raw_bytes: bytes):
        channel = self.channels.get_channel(channel_name)
        if channel is None:
            return 
        
        array_data = np.frombuffer(raw_bytes, dtype=np.int16)
        channel.queue.put(array_data)

    def stop_channel(self, channel_name: str):
        channel = self.channels.get_channel(channel_name)
        if channel is None:
            return
        
        channel.active_playback = False
        self.event_notifier.notify_audio_finished(channel_name)
        
        q = channel.queue
        with q.mutex:
            q.queue.clear()
    
    def set_paused_channel(self, channel_name: str, is_paused: bool):
        channel = self.channels.get_channel(channel_name)
        if channel is None:
            return
        
        channel.paused = is_paused
    
    def pause_channel(self, channel_name: str):
        self.set_paused_channel(channel_name, True)
    
    def resume_channel(self, channel_name: str):
        self.set_paused_channel(channel_name, False)
    
    def set_volume_channel(self, channel_name: str, volume: float):
        channel = self.channels.get_channel(channel_name)
        if channel is None:
            return
        
        channel.volume = max(0.0, min(volume, 1.0))
    
    async def close(self):
        await self.event_notifier.close()
        self.stream.stop_stream()
        self.stream.close()
        self.pyaudio_instance.terminate()


def main():
    pass

if __name__ == "__main__":
    main()