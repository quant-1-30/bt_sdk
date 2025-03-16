from textwrap import dedent
from typing import Any, List
import numpy as np
from numpy.random import default_rng # type: ignore
# from six import with_metaclass
from meta import ParamBase


class Downsample(ParamBase):
        
    @staticmethod
    def on_align(downsamples, snapshot):
        """
            align downsamples by snapshot 
            start / end 
            min / max
        """
        downsamples[0] = snapshot[0]
        downsamples[-1] = snapshot[-1]
        max_idx = np.argmax(downsamples)
        downsamples[max_idx] = np.max(snapshot)
        min_idx = np.argmin(downsamples)
        downsamples[min_idx] = np.min(snapshot)
        return downsamples
    
    def sample(self, datas):
        raise NotImplementedError("240/m ticker to 4800 3/s ticker")
    

class Beta(Downsample):
    """
        level1  stock  3/s
        level1  future 250/ms
    """

    params = (
        ("alias", "beta"),
        ("size", 20),
        ("prior", {"a":1, "b": 2})
    )

    def sample(self, snapshot):
        """
            snapshot: np.array[t-1,t]
            beta distribution
        """
        delta = np.max(snapshot) - np.min(snapshot)
        # downsample
        rng = default_rng()
        probs = rng.beta(a=self.p.prior["a"], b=self.p.prior["b"], size=self.p.size)
        samples = delta * np.array(probs) + np.min(snapshot)
        # align
        align_samples = self.on_align(samples, snapshot=snapshot)
        return align_samples


class Smooth(Downsample):

    params = (("size", 20),)

    def sample(self, snapshot):
        """
            linear sample
        """
        delta = snapshot[-1] - snapshot[0]
        probs = range(1, self.p.size+1) / self.p.size
        samples = delta * np.array(probs) + snapshot[0]
        # align
        align_samples = self.on_align(samples, snapshot=snapshot)
        return align_samples
