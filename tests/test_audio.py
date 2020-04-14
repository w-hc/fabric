import os.path as osp
import unittest
from fvcore.common.benchmark import benchmark
from fabric.io.audio import load_audio

fname = '/projects/haochenw/av/audio_visual/j_utah/_hptbEVx5eM.wav'


def consistency_compare(fname, start, end):
    sf_data, sr = load_audio(fname, start, end, backend='soundfile')
    ta_data, sr = load_audio(fname, start, end, backend='torchaudio')
    assert sf_data.shape == ta_data.shape
    assert sf_data.dtype == ta_data.dtype
    is_same = (sf_data == ta_data)
    return is_same


def audio_benchmark(backend, start, end):
    def func():
        return load_audio(fname, start, end, backend)
    return func


@unittest.skipIf(not osp.isfile(fname), 'file does not exist')
class LoadingConsistency(unittest.TestCase):
    def test_torchaudio_soundfile_consistency(self):
        start, end = None, None
        for start, end in (
            [None, None],
            [100, 201.5],
            [17.123, 19.435]
        ):
            is_same = consistency_compare(fname, start, end)
            self.assertTrue(is_same.all())

    def test_sf_read_speed(self):
        args = [
            {'backend': 'soundfile', 'start': None,   'end': None},
            {'backend': 'soundfile', 'start': 100,    'end': 201.5},
            {'backend': 'soundfile', 'start': 17.123, 'end': 19.435},
            {'backend': 'torchaudio', 'start': None,   'end': None},
            {'backend': 'torchaudio', 'start': 100,    'end': 201.5},
            {'backend': 'torchaudio', 'start': 17.123, 'end': 19.435},
        ]
        benchmark(audio_benchmark, 'bm', args, num_iters=4, warmup_iters=1)


if __name__ == "__main__":
    unittest.main()
