import numpy as np
import numpy.typing as npt
import io
from pydub import AudioSegment
from typing import Literal

MIN_VAL_INT_16 = np.iinfo(np.int16).min
MAX_VAL_INT_16 = np.iinfo(np.int16).max

def apply_volume(chunk: npt.NDArray[np.int16] | npt.NDArray[np.int32], volume: float) -> npt.NDArray[np.int32]:
    # Humans perceive volume in a logarithmic scale;
    # that means we must bring the volume metric to a log scale to make it sound right.
    volume = volume ** 3

    return (chunk * volume).astype(np.int32)

def clip_buffer_16_bits(chunk: npt.NDArray[np.int32]) -> npt.NDArray[np.int16]:
    return np.clip(chunk, MIN_VAL_INT_16, MAX_VAL_INT_16).astype(np.int16)

def process_and_slice_file(
        raw_audio_bytes: bytes, 
        audio_type: Literal["wav", "mp3", "raw"],
        frame_rate: int,
        channels: int,
        sample_width: int,
        speaker_chunk_size: int
    ) -> list[bytes]:
    if audio_type == "raw":
        return [raw_audio_bytes]
    
    chunk_size = speaker_chunk_size * channels * sample_width
    
    audio_file = io.BytesIO(raw_audio_bytes)
    audio = AudioSegment.from_file(audio_file, format=audio_type)
    
    audio = audio.set_frame_rate(frame_rate).set_channels(channels).set_sample_width(sample_width)
    
    raw_pcm_data = audio.raw_data
    chunks = []

    for i in range(0, len(raw_pcm_data), chunk_size):
        chunk = raw_pcm_data[i : i + chunk_size] 
        padding_needed = chunk_size - len(chunk)
        if padding_needed > 0:
            chunk += b"\x00" * padding_needed
        
        chunks.append(chunk)
    
    return chunks