import soundfile as sf
import torchaudio as ta
from .common import check_start_end_time

__all__ = ['load_audio']


def load_audio(fname, start_sec=None, end_sec=None, backend='soundfile'):
    if backend == 'soundfile':
        return _sf_load_audio(fname, start_sec, end_sec)
    elif backend == 'torchaudio':
        return _ta_load_audio(fname, start_sec, end_sec)
    else:
        raise ValueError('unknown backend {}'.format(backend))


def _sf_load_audio(fname, start_sec, end_sec):
    with sf.SoundFile(fname) as f:
        sampling_rate = f.samplerate
        duration_secs = f.frames / sampling_rate
        start, end = check_start_end_time(start_sec, end_sec, duration_secs)
        start, end = int(start * sampling_rate), int(end * sampling_rate)
        f.seek(start)
        data = f.read(frames=end - start, dtype='float32', always_2d=False)
        data = data.T
    return data, sampling_rate


def _ta_load_audio(fname, start_sec, end_sec):
    meta = ta.info(fname)[0]
    sampling_rate = meta.rate
    duration_secs = meta.length / sampling_rate
    start, end = check_start_end_time(start_sec, end_sec, duration_secs)
    start, end = int(start * sampling_rate), int(end * sampling_rate)
    data, sr = ta.load(fname, offset=start, num_frames=end - start)
    data = data.numpy()
    return data, int(sampling_rate)
