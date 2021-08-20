from __future__ import annotations
import click
from pathlib import Path
from qslib.common import Experiment, Machine


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "-o",
    "--output",
    help="output file name (defaults to experiment base filename + extension)",
    type=click.Path(),
)
@click.option(
    "-f",
    "--format",
    "format",
    help="Specify the output format (matplotlib formats are supported, eg, pdf, svg, png); defaults to pdf.",
    default="pdf",
)
@click.option(
    "-a",
    "--actual/--no-actual",
    default=False,
    help="NOT FINISHED Include actual temperature readings for finished experiments (not included by default).",
)
@click.option("-n", "--open/--no-open", help="Open the file after creating it.")
@click.argument("experiment", type=click.Path(exists=True))
def protocol_plot(experiment, output, format, actual, open) -> None:
    """Plot the temperature protocol in an experiment."""
    import matplotlib.pyplot as plt

    experiment = Path(experiment)

    exp = Experiment.from_file(experiment)

    if output is None:
        output = experiment.with_suffix("." + format)

    fig, ax = plt.subplots(figsize=(21.0 / 2.54, 15.0 / 2.54))

    exp.protocol.tcplot(ax)

    fig.savefig(output, format=format)

    if open:
        click.launch(str(output))

@cli.command()
@click.option(
    "-o",
    "--output",
    help="output file name (defaults to experiment base filename + extension)",
    type=click.Path(),
)

@click.option("-n", "--open/--no-open", "openfile", help="Open the file after creating it.", default=True)
@click.argument("experiment", type=click.Path(exists=True))
def info_html(experiment, output, openfile) -> None:
    """Plot the temperature protocol in an experiment."""
    import matplotlib.pyplot as plt

    experiment = Path(experiment)

    exp = Experiment.from_file(experiment)

    if output is None:
        output = experiment.with_suffix(".html")

    with open(output, 'w') as f:
        f.write(exp.info_html())    

    if openfile:
        click.launch(str(output))


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
def protocol_desc(experiment: str | Path) -> None:
    """Print a description of the protocol in an experiment."""
    experiment = Path(experiment)

    exp = Experiment.from_file(experiment)

    click.echo(str(exp.protocol))


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
def export_temperatures(experiment: str | Path) -> None:
    """Export temperature readings from an experiment, as a CSV file."""

    experiment = Path(experiment)

    exp = Experiment.from_file(experiment)

    exp.temperatures.to_csv(click.get_text_stream("stdout"))


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
def export_data(experiment: str | Path) -> None:
    """Export fluorescence reading data from an experiment, as a CSV file."""
    experiment = Path(experiment)

    exp = Experiment.from_file(experiment)

    exp.welldata.to_csv(click.get_text_stream("stdout"))


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
def info(experiment: str) -> None:
    """Output information on the experiment."""

    exp = Experiment.from_file(experiment)

    click.echo(exp.summary())


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
@click.argument("machine")
@click.option("-t", "--tunnel-host")
@click.option("-u", "--tunnel-user")
def run(experiment: str, machine: str, tunnel_host: str, tunnel_user: str) -> None:
    """Run an experiment."""
    exp = Experiment.from_file(experiment)

    import logging

    logging.basicConfig(level=logging.INFO)

    m = Machine(
        machine,
        max_access_level="Controller",
        tunnel_host=tunnel_host,
        tunnel_user=tunnel_user,
    )

    with m:
        exp.run(m)


@cli.command()
@click.argument("machine")
@click.argument("state", type=click.Choice(["on", "off"]))
@click.option("-t", "--tunnel-host")
@click.option("-u", "--tunnel-user")
def machine_power(machine: str, tunnel_host: str, tunnel_user: str, state: str) -> None:
    """Turn the lamp/heat-block on or off (if idle)."""
    m = Machine(
        machine,
        max_access_level="Controller",
        tunnel_host=tunnel_host,
        tunnel_user=tunnel_user,
    )

    with m:
        rs = m.run_status()
        ms = m.machine_status()
        mn = m.run_command("SYST:SETT:NICK?")
        
        if rs.state != "Idle":
            raise click.UsageError(f"Machine {mn} is currently running {rs.name}, not changing power state during run.")
        else:
            with m.at_access("Controller"):
                m.power = {"on": True, "off": False}[state]


@cli.command()
@click.argument("machine")
@click.option("-t", "--tunnel-host")
@click.option("-u", "--tunnel-user")
def machine_status(machine: str, tunnel_host: str, tunnel_user: str) -> None:
    """Print the current status of a machine."""
    m = Machine(
        machine,
        max_access_level="Observer",
        tunnel_host=tunnel_host,
        tunnel_user=tunnel_user,
    )

    m.connect()
    rs = m.run_status()
    ms = m.machine_status()
    mn = m.run_command("SYST:SETT:NICK?")
    m.disconnect()

    drawer = ms.drawer
    if drawer == "Closed":
        drawer = click.style(drawer, fg="green")
    if drawer == "Open":
        drawer = click.style(drawer, fg="yellow")
    if drawer == "Unknown":
        drawer = click.style(drawer, fg="red")

    cover = ms.cover
    if cover == "Down":
        cover = click.style(cover, fg="green")
    if cover == "Up":
        cover = click.style(cover, fg="blue")
    if cover == "Unknown":
        cover = click.style(cover, fg="red")

    lamp = ms.lamp_status.split()[0]
    if lamp == "Off":
        lamp = click.style(lamp, fg="yellow")
    if lamp == "On":
        lamp = click.style(lamp, fg="green")

    state = rs.state
    if state == "Running":
        state = click.style(state, fg="green")
    if state == "Idle":
        state = click.style(state, fg="blue")
    if state == "Error":
        state = click.style(state, fg="red")

    click.echo(f"Machine {mn} is {state}.")
    click.echo(f"Drawer is {drawer}, cover is {cover}, and lamp is {lamp}.")
    if rs.state != "Idle":
        click.echo(
            f"Run {rs.name}. Stage {rs.stage}/{rs.num_stages},"
            f" cycle {rs.cycle}/{rs.num_cycles}, and step {rs.step}."
        )

    del m


@cli.command()
@click.argument("machine")
@click.option("-t", "--tunnel-host")
@click.option("-u", "--tunnel-user")
def list_stored(machine: str, tunnel_host: str, tunnel_user: str) -> None:
    """List experiments stored on a machine."""
    m = Machine(
        machine,
        max_access_level="Observer",
        tunnel_host=tunnel_host,
        tunnel_user=tunnel_user,
    )

    with m:
        for f in m.list_runs_in_storage():
            click.echo(f)


@cli.command()
@click.argument("machine")
@click.option("-t", "--tunnel-host")
@click.option("-u", "--tunnel-user")
@click.argument("experiment")
@click.option("-o", "--output", type=click.Path())
def copy_stored(
    machine: str,
    tunnel_host: str,
    tunnel_user: str,
    experiment: str,
    output: str | None,
) -> None:
    """Copy experiment from machine storage."""
    m = Machine(
        machine,
        max_access_level="Observer",
        tunnel_host=tunnel_host,
        tunnel_user=tunnel_user,
    )

    if output is None:
        output = experiment

    with m:
        exp = Experiment.from_machine_storage(m, experiment)
        exp.save_file_without_changes(output)


if __name__ == "__main__":
    cli()
