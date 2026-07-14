from pydantic import BaseModel, Field, SecretStr, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated

class RedisSettings(BaseModel):
    HOST: str = "127.0.0.1"
    PORT: int = 6379
    PASSWORD: SecretStr | None = None
    MAX_STREAM_LEN: PositiveInt = 2_000

class StreamNameSettings(BaseModel):
    LIVE_AUDIO: str = "live_audio_broadcast"
    PLAYBACK: str = "playback"

class MicrophoneSettings(BaseModel):
    CAPTURE_RATE: PositiveInt = 16_000
    CHUNK_SIZE: PositiveInt = 1_280

class SpeakerSettings(BaseModel):
    RATE: PositiveInt = 16_000
    CHUNK_SIZE: PositiveInt = 1_280

VolumeSetting = Annotated[float, Field(ge=0.0, le=1.0)]

class AudioMixerSettings(BaseModel):
    MASTER_DEFAULT_VOLUME: VolumeSetting = 1.0
    MUSIC_DEFAULT_VOLUME: VolumeSetting = 1.0
    AGENT_DEFAULT_VOLUME: VolumeSetting = 1.0
    NOTIFICATION_DEFAULT_VOLUME: VolumeSetting = 1.0


class Settings(BaseSettings):
    REDIS: RedisSettings = Field(default_factory=RedisSettings)
    STREAM_NAME: StreamNameSettings = Field(default_factory=StreamNameSettings)
    MICROPHONE: MicrophoneSettings = Field(default_factory=MicrophoneSettings)
    SPEAKER: SpeakerSettings = Field(default_factory=SpeakerSettings)
    AUDIO_MIXER: AudioMixerSettings = Field(default_factory=AudioMixerSettings)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

settings = Settings()