from fabric.cluster.smit2 import period_watch
from fabric.cluster.watch import survey
import click


@click.command()
@click.option("-a", "--action")
@click.option("-i", "--interval", default=5)
def main(action, interval):
    if action == "survey":
        survey(interval)
    elif action == "update":
        period_watch(interval)
    else:
        raise ValueError(action)


if __name__ == "__main__":
    main()
