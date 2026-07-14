import numpy as np
import numpy.typing as npt

MIN_VAL_INT_16 = np.iinfo(np.int16).min
MAX_VAL_INT_16 = np.iinfo(np.int16).max

def apply_volume(chunk: npt.NDArray[np.int16], volume: float) -> npt.NDArray[np.int32]:
    # Humans perceive volume in a logarithmic scale;
    # that means we must bring the volume metric to a log scale to make it sound right.
    volume = volume ** 3

    return (chunk * volume).astype(np.int32)

def clip_buffer_16_bits(chunk: npt.NDArray[np.int32]) -> npt.NDArray[np.int16]:
    return np.clip(chunk, MIN_VAL_INT_16, MAX_VAL_INT_16).astype(np.int16)
