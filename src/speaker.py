from redis.asyncio import Redis
import asyncio
import logging
from typing import Any, cast
from dataclasses import dataclass

from src.audio_mixer import AudioMixer

logger = logging.getLogger(__name__)

@dataclass
class SpreakerChannel:
    name: str
    stream_name: str
    last_id: str

@dataclass
class SpeakerChannelConfig:
    music: SpreakerChannel
    agent: SpreakerChannel
    notification: SpreakerChannel

    def __iter__(self):
        for channel in [self.music, self.agent, self.notification]:
            yield channel

class Speaker:
    def __init__(self, channel_config: SpeakerChannelConfig, audio_mixer: AudioMixer, redis_provider: Redis):
        self.channel_config = channel_config
        self.audio_mixer = audio_mixer
        self.redis_provider = redis_provider
        
    async def run(self):
        tasks = []
        for channel in self.channel_config:
            tasks.append(self._run_channel(channel))
        
        await asyncio.gather(*tasks)
    
    async def _run_channel(self, channel: SpreakerChannel):
        stream_name = channel.stream_name
        
        while True:
            try:
                payload = {stream_name: channel.last_id}
                
                response = await self.redis_provider.xread(
                    cast(Any, payload),
                    count=10,
                    block=2_000
                )

                for _, messages in response:
                    messages = cast(Any, messages)
                    for message_id, payload in messages:
                        channel.last_id = message_id

                        raw_audio_bytes = payload.get(b"audio_data")
                        if not raw_audio_bytes:
                            continue

                        await asyncio.to_thread(self.audio_mixer.submit_audio_chunk, channel.name, raw_audio_bytes)
            
            except asyncio.CancelledError:
                logger.info(f"Safely stopping ingestion for channel: {channel}")
                break
            except Exception as e:
                logger.error(f"Connection failure on channel '{channel}': {e}")
                await asyncio.sleep(1)
    
    async def close(self):
        await self.audio_mixer.close()
        await self.redis_provider.close()
