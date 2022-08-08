from fabric.cluster.smit2 import period_watch
from fabric.cluster.watch import survey
import click


@click.command()
@click.option("-a", "--action")
@click.option("-i", "--interval", default=5)
@click.option("--drop_done", default=False)
def main(action, interval, drop_done):
    if action == "survey":
        survey(interval, drop_done)
    elif action == "update":
        period_watch(interval)
    else:
        raise ValueError(action)


if __name__ == "__main__":
    main()
