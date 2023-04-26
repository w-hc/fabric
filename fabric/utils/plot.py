import numpy as np
import matplotlib.pyplot as plt


def mpl_fig_to_buffer(fig):
    fig.canvas.draw()
    plot = np.array(fig.canvas.renderer.buffer_rgba())
    plt.close(fig)
    return plot


def plot_heatmap(xs, ys, data, ax=None, cmap="Oranges", **kwargs):
    if ax is None:
        fig, ax = plt.subplots()

    extent = [xs[0], xs[-1], ys[0], ys[-1]]
    ax.imshow(
        data, extent=extent, cmap=cmap,
        aspect='auto', origin="lower",
        **kwargs
    )

    """
    pcolormesh is more flexible but a lot slower vs imshow.
    - it can plot non-regular 2D quadmesh, (e.g. grid of trapezoid). hence the
    the name pcolormesh.
    - Cost is speed. It doesn't interactively display the value under cursor,
    unless explictly enabled as done below. Reason is its interpolation algo
    is expensive. https://github.com/matplotlib/matplotlib/issues/24406

    pcolormesh also has stronger defaults intended for heatmap
    - y-axis that grows from bottom,
    - flexible aspect ratio.
    - axis labeling based on coordinates rather than pixel integer indices

    the imshow code above incorporates incorporates these defaults
    """
    # h = plt.pcolormesh(
    #     xs, ys, data, cmap=cmap,
    #     shading="nearest"
    # )
    # h.set_mouseover(True)
