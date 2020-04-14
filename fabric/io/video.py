import os.path as osp
import contextlib
import gc
import math
import re
import warnings

import numpy as np
import av

from .common import check_start_end_time
from .audio import load_audio

# PyAV has some reference cycles
_CALLED_TIMES = 0
_GC_COLLECTION_INTERVAL = 10

# decide the audio loading method; pyav audio loading is inconsisten and slow
# for now, load the transcoded wav file as a walkaround
_AUDIO_LOAD_FROM_TRANSCODE = True


__all__ = ['Video']


class VideoMeta():
    '''This is to ensure that has_video and has_audio are tied to meta'''
    def __init__(self, container):
        v_meta, a_meta = self.probe_meta(container)
        self.video = v_meta
        self.audio = a_meta

    def has_video(self):
        return self.video is not None

    def has_audio(self):
        return self.audio is not None

    @staticmethod
    def probe_meta(container):
        v_meta, a_meta = None, None
        if container.streams.video:
            vstream = container.streams.video[0]
            timebase = vstream.time_base
            v_meta = {
                'stream_type': 'video',
                'pts_timebase': [timebase.numerator, timebase.denominator],
                'num_pts': vstream.duration,
                'length_in_secs': float(timebase * vstream.duration),
                'num_frames': vstream.frames,
                'average_fps': float(vstream.average_rate),
                'spatial_size': [vstream.codec_context.height, vstream.codec_context.width]
            }

        if container.streams.audio:
            astream = container.streams.audio[0]
            timebase = astream.time_base
            a_meta = {
                'stream_type': 'audio',
                'pts_timebase': [timebase.numerator, timebase.denominator],
                'num_pts': astream.duration,
                'length_in_secs': float(timebase * astream.duration),
                'num_frames': astream.frames,
                'sampling_rate': astream.sample_rate,
                'num_channels': astream.codec_context.channels,
                'frame_size': astream.codec_context.frame_size,
            }
            # assert sampling rate is the second digit of timebase
            assert a_meta['sampling_rate'] == a_meta['pts_timebase'][1]

        return v_meta, a_meta


class Video():
    def __init__(self, fname):
        self.fname = fname
        self.container = None
        self._meta = None

    @contextlib.contextmanager
    def open_video(self):
        '''this method is robust to nested or repeated calls'''
        if self.container is not None:
            # if the container is already created, do nothing
            yield
        else:
            self.container = av.open(self.fname)
            try:
                yield
            finally:  # bubble up (don't catch) exceptions; but please clean up
                self.container.close()
                self.container = None

    def _confirm_container_opened(self):
        if self.container is None:
            raise ValueError("the video container is not opened")

    @property
    def meta(self):
        if self._meta is None:
            self._confirm_container_opened()
            self._meta = VideoMeta(self.container)
        return self._meta

    def load_image_frames(self, start_sec=None, end_sec=None):
        self._confirm_container_opened()
        if not self.meta.has_video():
            raise ValueError("no visual for this file")
        start_sec, end_sec = check_start_end_time(
            start_sec, end_sec, self.meta.video['length_in_secs']
        )
        container = self.container

        vframes = read_from_stream(
            container, container.streams.video[0], {'video': 0},
            start_sec, end_sec
        )
        vdata = np.stack(
            [frame.to_rgb().to_ndarray() for frame in vframes], axis=0
        )
        return vdata

    def load_audio_frames(self, start_sec=None, end_sec=None):
        self._confirm_container_opened()
        if not self.meta.has_audio():
            raise ValueError("no audio for this file")
        start_sec, end_sec = check_start_end_time(
            start_sec, end_sec, self.meta.audio['length_in_secs']
        )

        if _AUDIO_LOAD_FROM_TRANSCODE:
            transcode_audio_fname = osp.splitext(self.fname)[0] + '.wav'
            if not osp.isfile(transcode_audio_fname):
                raise ValueError("the transcoded audio file does not exist")
            adata, sr = load_audio(
                transcode_audio_fname, start_sec, end_sec, backend='soundfile'
            )
            return adata
        else:
            container = self.container

            aframes = read_from_stream(
                container, container.streams.audio[0], {'audio': 0},
                start_sec, end_sec
            )
            adata = np.concatenate(
                [frame.to_ndarray() for frame in aframes], axis=1
            )
            return adata


def read_from_stream(container, stream, stream_info, start_secs, end_secs):
    return _read_from_stream(
        container, start_secs, end_secs,
        pts_unit='sec', stream=stream, stream_name=stream_info
    )


def _read_from_stream(
    container, start_offset, end_offset, pts_unit, stream, stream_name
):
    global _CALLED_TIMES, _GC_COLLECTION_INTERVAL
    _CALLED_TIMES += 1
    if _CALLED_TIMES % _GC_COLLECTION_INTERVAL == _GC_COLLECTION_INTERVAL - 1:
        gc.collect()

    if pts_unit == "sec":
        start_offset = int(math.floor(start_offset * (1 / stream.time_base)))
        if end_offset != float("inf"):
            end_offset = int(math.ceil(end_offset * (1 / stream.time_base)))
    else:
        warnings.warn(
            "The pts_unit 'pts' gives wrong results and will be removed in a "
            + "follow-up version. Please use pts_unit 'sec'."
        )  # It is because the pts is stream specific and not a standard unit

    frames = {}
    should_buffer = False
    max_buffer_size = 5
    if stream.type == "video":
        # DivX-style packed B-frames can have out-of-order pts (2 frames in a single pkt)
        # so need to buffer some extra frames to sort everything
        # properly
        extradata = stream.codec_context.extradata
        # overly complicated way of finding if `divx_packed` is set, following
        # https://github.com/FFmpeg/FFmpeg/commit/d5a21172283572af587b3d939eba0091484d3263
        if extradata and b"DivX" in extradata:
            # can't use regex directly because of some weird characters sometimes...
            pos = extradata.find(b"DivX")
            d = extradata[pos:]
            o = re.search(br"DivX(\d+)Build(\d+)(\w)", d)
            if o is None:
                o = re.search(br"DivX(\d+)b(\d+)(\w)", d)
            if o is not None:
                should_buffer = o.group(3) == b"p"
    seek_offset = start_offset
    # some files don't seek to the right location, so better be safe here
    # WHC edit 1: change from seek_offset - 1 to seek_offset - 0
    seek_offset = max(seek_offset - 0, 0)
    if should_buffer:
        # FIXME this is kind of a hack, but we will jump to the previous keyframe
        # so this will be safe
        seek_offset = max(seek_offset - max_buffer_size, 0)
    try:
        # TODO check if stream needs to always be the video stream here or not
        container.seek(seek_offset, any_frame=False, backward=True, stream=stream)
    except av.AVError:
        # TODO add some warnings in this case
        # print("Corrupted file?", container.name)
        return []
    buffer_count = 0
    try:
        for _idx, frame in enumerate(container.decode(**stream_name)):
            frames[frame.pts] = frame
            if frame.pts >= end_offset:
                if should_buffer and buffer_count < max_buffer_size:
                    buffer_count += 1
                    continue
                break
    except av.AVError:
        # TODO add a warning
        pass
    # ensure that the results are sorted wrt the pts
    result = [
        frames[i] for i in sorted(frames) if start_offset <= frames[i].pts <= end_offset
    ]
    # WHC edit 2: remove front seeking
    # if len(frames) > 0 and start_offset > 0 and start_offset not in frames:
    #     # if there is no frame that exactly matches the pts of start_offset
    #     # add the last frame smaller than start_offset, to guarantee that
    #     # we will have all the necessary data. This is most useful for audio
    #     preceding_frames = [i for i in frames if i < start_offset]
    #     if len(preceding_frames) > 0:
    #         first_frame_pts = max(preceding_frames)
    #         print('adding in frame {}'.format(first_frame_pts))
    #         result.insert(0, frames[first_frame_pts])
    return result


def _align_audio_frames(aframes, audio_frames, ref_start, ref_end):
    '''
    This is logically wrong cuz it assumes that ref_start and end
    are in the units of stream pts
    '''
    start, end = audio_frames[0].pts, audio_frames[-1].pts
    total_aframes = aframes.shape[1]
    step_per_aframe = (end - start + 1) / total_aframes
    s_idx = 0
    e_idx = total_aframes
    if start < ref_start:
        s_idx = int((ref_start - start) / step_per_aframe)
    if end > ref_end:
        e_idx = int((ref_end - end) / step_per_aframe)
    return aframes[:, s_idx:e_idx]
