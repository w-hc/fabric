from pathlib import Path
import yaml
import torch
import fire
import tabulate


def compute_stats(ckpt_path):
    ckpt_path = Path(ckpt_path)
    ckpt = torch.load(ckpt_path)
    stats = []
    for k, tsr in ckpt.items():
        tsr = tsr.cuda()
        data = {
            "name": k,
            "shape": list(tsr.shape),
            "mean": tsr.mean().item(),
            "std": tsr.std().item(),
            "max": tsr.max().item(),
            "min": tsr.min().item()
        }
        stats.append(data)
        # free the cuda mem by deleting the tsr
        tsr.cpu()
        del tsr
    
    # store the stats as yaml, add prepend the name with "stats_", and change suffix to ".yaml"
    yaml_path = ckpt_path.parent / f"stats_{ckpt_path.stem}.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(stats, f, default_flow_style=False, sort_keys=False)


def display(stats_fname):
    stats_fname = Path(stats_fname)
    with stats_fname.open('r') as f:
        stats = yaml.safe_load(f)
    print(tabulate.tabulate(stats, headers="keys"))


if __name__ == '__main__':
    fire.Fire()
