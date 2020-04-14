import unittest
import os.path as osp
import numpy as np

from fabric.io.video import Video
from torchvision.io import read_video
from torchvision.io.video import _read_from_stream as tv_read_stream

'''
1. repeated reading artifact due to forward seek
Is reading from a dumped mp3 faster than decoding on the fly? Test it.
2. test whether pyav can read from an audio file properly vs soundfile
3. test whether pyav can read audio from video file properly vs dumped wav file
4. understand the boundary frame drop situation


What I've discovered:
1. less than expected amount of audio data may be returned. Focus on the final
consumption shape. The data semantics should be fine.

2. pyav does not load audio properly.
Define [repeated loading]: use pyav to load the same audio segment repeatedly.
Repeated loading returns inconsistent results. For some files it works. For
others it doesn't. Complete mess of a function.

I have abandoned the plan to use pyav for audio loading.
I will manually dump the audio tracks into sound files and load them using
established audio libraries. This is such a painful lesson.


'''

fname = '/projects/haochenw/av/audio_visual/j_utah/_hptbEVx5eM.mp4'


@unittest.skipIf(not osp.isfile(fname), 'file does not exist')
class VideoLoading(unittest.TestCase):
    def test_metadata_probing(self):
        v = Video(fname)
        with v.open_video():
            self.assertTrue(v.meta.has_video())
            self.assertTrue(v.meta.has_audio())

    def test_repeated_video_load_consistency(self):
        num_trials = 3
        v = Video(fname)
        with v.open_video():
            for start, end in (
                [73.5, 77.7],
                [1000, 1010.3]
            ):
                deposits = []
                for _ in range(num_trials):
                    data = v.load_image_frames(start, end)
                    deposits.append(data)
                data = np.stack(deposits, axis=0)
                self.assertTrue((data == data[0]).all())

    def test_repeated_audio_load_consistency(self):
        num_trials = 3
        v = Video(fname)
        with v.open_video():
            for start, end in (
                [73.5, 77.7],
                [1000, 1010.3]
            ):
                deposits = []
                for _ in range(num_trials):
                    data = v.load_audio_frames(start, end)
                    deposits.append(data)
                data = np.stack(deposits, axis=0)
                self.assertTrue((data == data[0]).all())


if __name__ == "__main__":
    unittest.main()


"""
Below are some scratch functions. I won't delete it for now
"""


def test_repeated_audio_read_consistent():
    # fname = '/home/haochenw/tv_host.mp4'
    # start, end = 1.0, 2.0

    fname = '/projects/haochenw/av/audio_visual/j_utah/_hptbEVx5eM.mp4'
    v = Video(fname)
    deposits = []
    num_trials = 5
    with v.open_video():
        for _ in range(num_trials):
            sdata = v.load_audio_frames(start, end)
            deposits.append(sdata)
    data = np.stack(deposits, axis=0)
    assert (data == data[0]).all()


def test_repeated_video_read_consistent():
    # fname = '/projects/haochenw/av/audio_visual/j_utah/_hptbEVx5eM.mp4'
    # start, end = 73.5, 77.7

    fname = '/home/haochenw/tv_host.mp4'
    start, end = None, None
    v = Video(fname)
    deposits = []
    num_trials = 5
    with v.open_video():
        for _ in range(num_trials):
            sdata = v.load_image_frames(start, end)
            deposits.append(sdata)
    data = np.stack(deposits, axis=0)
    assert (data == data[0]).all()


def test_repeated_audio_read_consistent2():
    fname = '/projects/haochenw/av/audio_visual/j_utah/_hptbEVx5eM.aac'
    deposits = []
    num_trials = 3
    offset, num_frames = 0, 10000
    for _ in range(num_trials):
        # sdata, _ = sf.read(fname, start=0, stop=None)
        sf_sdata, _ = sf_load_to_float(fname, offset, num_frames)
        ta_sdata, _ = ta_load(fname, offset, num_frames)
        assert (ta_sdata == sf_sdata).all()
        deposits.append(ta_sdata)
    data = np.stack(deposits, axis=0)
    assert (data == data[0]).all()


def expose_tv_repeated_audio_loading_artifacts():
    # fname = '/home/haochenw/tv_host.mp4'
    fname = '/projects/haochenw/av/audio_visual/j_utah/_hptbEVx5eM.mp4'
    container = av.open(fname)
    deposits = []
    num_trials = 3
    start, end = 73.5, 77.7
    for _ in range(num_trials):
        aframes = tv_read_stream(
            container, start, end, pts_unit='sec',
            stream=container.streams.audio[0], stream_name={'audio': 0}
        )
        sdata = np.concatenate(
            [frame.to_ndarray() for frame in aframes], axis=1
        )
        deposits.append(sdata)
    data = np.stack(deposits, axis=0)
    assert (data == data[0]).all()


def bug_due_to_unit_in_audio_align():
    fname = osp.expanduser('~/tv_host.mp4')
    vframes, aframes, info = read_video(
        fname, start_pts=0, end_pts=1, pts_unit='sec'
    )
    return vframes


# main()
# bug_due_to_audio_align()
# expose_tv_repeated_audio_loading_artifacts()
# test_repeated_audio_read_consistent()
# test_repeated_video_read_consistent()
# test_repeated_audio_read_consistent2()
