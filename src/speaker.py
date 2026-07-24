from redis.asyncio import Redis
import asyncio
import logging
from typing import Any, TypedDict, NotRequired, Dict, cast
from dataclasses import dataclass

from src.audio_mixer import AudioMixer
from src.audio_utils import process_and_slice_file
from src.config import settings

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

class ControlListenerCommand(TypedDict):
    command: str
    channel: str
    volume: NotRequired[float]

class Speaker:
    def __init__(self, channel_config: SpeakerChannelConfig, audio_mixer: AudioMixer, redis_provider: Redis):
        self.channel_config = channel_config
        self.audio_mixer = audio_mixer
        self.redis_provider = redis_provider

        self.last_control_listener_id = "$"
        
    async def run(self):
        tasks = []
        for channel in self.channel_config:
            tasks.append(self._run_channel(channel))
        
        tasks.append(self._run_control_listener())

        await asyncio.gather(*tasks)
    
    
    async def _run_control_listener(self):
        while True:
            try:
                payload = {
                    settings.SPEAKER.CONTROLLER_STREAM_NAME: self.last_control_listener_id
                }
                
                response = await self.redis_provider.xread(
                    cast(Any, payload),
                    count=10,
                    block=2_000,
                )

                for _, messages in response:
                    messages = cast(Any, messages)
                    for message_id, payload in messages:
                        self.last_control_listener_id = message_id
                        
                        control_listener_command = self._parse_control_listener_message(payload)
                        self._execute_control_listener_message(control_listener_command)
            
            except asyncio.CancelledError:
                logger.info("Safely stopping speaker control listener")
                break
            except Exception as e:
                logger.error(f"Connection failure on speaker control listener: {e}")
                await asyncio.sleep(1)
    
    def _parse_control_listener_message(self, payload: Dict[bytes, bytes]) -> ControlListenerCommand:
        control_listener_command: ControlListenerCommand = {
            "channel": payload[b"channel"].decode("utf-8"),
            "command": payload[b"command"].decode("utf-8"),
        }
        if payload.get(b"volume"):
            control_listener_command["volume"] = float(payload[b"volume"].decode("utf-8"))
        
        return control_listener_command

    def _execute_control_listener_message(self, control_listener_command: ControlListenerCommand):
        if control_listener_command["channel"] == "master":
            if control_listener_command["command"] == "pause":
                self.audio_mixer.pause_master()
            elif control_listener_command["command"] == "resume":
                self.audio_mixer.resume_master()
            elif control_listener_command["command"] == "stop":
                self.audio_mixer.stop_master()
            elif control_listener_command["command"] == "volume" and "volume" in control_listener_command:
                self.audio_mixer.set_volume_master(control_listener_command["volume"])
            
            return
        
        if control_listener_command["command"] == "pause":
            self.audio_mixer.pause_channel(control_listener_command["channel"])
        elif control_listener_command["command"] == "resume":
            self.audio_mixer.resume_channel(control_listener_command["channel"])
        elif control_listener_command["command"] == "stop":
            self.audio_mixer.stop_channel(control_listener_command["channel"])
        elif control_listener_command["command"] == "volume" and "volume" in control_listener_command:
            self.audio_mixer.set_volume_channel(control_listener_command["channel"], control_listener_command["volume"])
        

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
                        audio_type = payload.get(b"audio_type", b"raw").decode("utf-8")
                        
                        if not raw_audio_bytes:
                            continue
                        
                        processed_audio_bytes = process_and_slice_file(
                            raw_audio_bytes, 
                            audio_type, 
                            settings.SPEAKER.RATE, 
                            settings.SPEAKER.CHANNELS, 
                            settings.SPEAKER.SAMPLE_WIDTH,
                            settings.SPEAKER.CHUNK_SIZE
                        )
                        for chunk in processed_audio_bytes:
                            await asyncio.to_thread(self.audio_mixer.submit_audio_chunk, channel.name, chunk)
            
            except asyncio.CancelledError:
                logger.info(f"Safely stopping ingestion for channel: {channel}")
                break
            except Exception as e:
                logger.error(f"Connection failure on channel '{channel}': {e}")
                await asyncio.sleep(1)
    
    async def close(self):
        await self.audio_mixer.close()
        await self.redis_provider.close()
