# SPDX-FileCopyrightText: 2021 - 2023 Constantine Evans <qslib@mb.costi.net>
#
# SPDX-License-Identifier: EUPL-1.2

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import click
from click_aliases import ClickAliasedGroup

from qslib.common import Experiment, Machine
from qslib.scpi_commands import (
    AccessLevel,
    AccessLevelExceeded,
    AuthError,
    CommandError,
    InsufficientAccess,
)
from .version import __version__


@click.group(cls=ClickAliasedGroup)
@click.version_option(__version__, "--version", "-V")
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

    exp.protocol.plot_protocol(ax)

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
@click.option(
    "-n",
    "--open/--no-open",
    "openfile",
    help="Open the file after creating it.",
    default=True,
)
@click.argument("experiment", type=click.Path(exists=True))
def info_html(experiment, output, openfile) -> None:
    """Create an HTML summary of the experiment, and potentially open it."""

    experiment = Path(experiment)

    exp = Experiment.from_file(experiment)

    if output is None:
        output = experiment.with_suffix(".html")

    with open(output, "w") as f:
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

    if exp.temperatures is None or exp.temperatures.empty:
        raise click.UsageError("Experiment does not contain temperature data.")

    exp.temperatures.to_csv(path_or_buf=click.get_binary_stream("stdout"))  # type: ignore


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
def export_data(experiment: str | Path) -> None:
    """Export fluorescence reading data from an experiment, as a CSV file."""
    experiment = Path(experiment)

    exp = Experiment.from_file(experiment)

    exp.welldata.to_csv(path_or_buf=click.get_binary_stream("stdout"))  # type: ignore


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
def info(experiment: str) -> None:
    """Output information on the experiment."""

    exp = Experiment.from_file(experiment)

    click.echo(exp.info())


@cli.command()
@click.argument("experiment", type=click.Path(exists=True))
@click.argument("machine")
def run(experiment: str, machine: str) -> None:
    """Run an experiment."""
    exp = Experiment.from_file(experiment)

    import logging

    logging.basicConfig(level=logging.INFO)

    m = Machine(
        machine,
        max_access_level="Controller",
    )

    with m:
        exp.run(m)


@cli.command()
@click.argument("machine")
@click.argument("state", type=click.Choice(["on", "off"]))
def machine_power(machine: str, state: str) -> None:
    """Turn the lamp/heat-block on or off (if idle)."""
    m = Machine(
        machine,
        max_access_level="Controller",
    )

    with m:
        rs = m.run_status()
        m.machine_status()
        mn = m.run_command("SYST:SETT:NICK?")

        if rs.state != "Idle":
            raise click.UsageError(
                f"Machine {mn} is currently running {rs.name}, not changing power state during run."
            )
        else:
            with m.at_access("Controller"):
                m.power = {"on": True, "off": False}[state]


@cli.command()
@click.argument("machine")
def machine_status(machine: str) -> None:
    """Print the current status of a machine."""
    m = Machine(
        machine,
        max_access_level="Observer",
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
    click.echo(
        f"Drawer is {drawer}, cover is {cover}, and lamp is {lamp} and {ms.led_temperature:.2f} °C."
    )

    click.echo(f"Cover temperature is {ms.cover_temperature:.2f} °C", nl=False)
    if ms.target_controlled["Cover"]:
        click.echo(
            f" and is controlled with a target of {ms.target_temperatures['Cover']:.2f} °C."
        )
    else:
        click.echo(" and is uncontrolled.")

    click.echo(
        "Block temperatures are "
        + ", ".join("{:.2f}".format(x) for x in ms.block_temperatures)
        + " °C."
    )
    click.echo(
        "Sample temperatures are "
        + ", ".join("{:.2f}".format(x) for x in ms.block_temperatures)
        + " °C."
    )

    if all(ms.target_controlled[f"Zone{i}"] for i in range(1, 7)):
        click.echo(
            "Zone temperatures are controlled with a target of "
            + ", ".join(
                "{:.2f}".format(ms.target_temperatures[f"Zone{i}"]) for i in range(1, 7)
            )
            + " °C."
        )
    else:
        click.echo("Zone temperatures are uncontrolled.")

    if rs.state != "Idle":
        click.echo(
            f"Run {rs.name}. Stage {rs.stage}/{rs.num_stages},"
            f" cycle {rs.cycle}/{rs.num_cycles}, and step {rs.step}."
        )

    del m

@cli.command(name="list-stored")
@click.argument("machine_path", nargs=-1)
@click.option("-v", "--verbose", is_flag=True, help="Show detailed file information")
@click.option("-s", "--sort", type=click.Choice(['name', 'time', 'size', 'created']), default='name',
              help="Sort by name, modification time, creation time, or file size")
@click.option("-r", "--reverse", is_flag=True, help="Reverse the sort order")
@click.option("-U", "--created", is_flag=True, help="Use creation time instead of modification time")
def list_stored(machine_path: tuple[str, ...], verbose: bool, sort: str, reverse: bool, created: bool) -> None:
    """List experiments stored on a machine.
    
    MACHINE_PATH can be either just a machine name/address, or machine:pattern where
    pattern is used to filter results using shell-style wildcards (*, ?, etc)."""
    
    for mp in machine_path:
        if mp.startswith("["):
            # Handle IPv6 address in brackets
            closing_bracket = mp.find("]")
            if closing_bracket == -1:
                raise click.UsageError("Missing closing bracket for IPv6 address")
        
            machine = mp[1:closing_bracket]
            if len(mp) > closing_bracket + 1:
                if mp[closing_bracket + 1] != ":":
                    raise click.UsageError("Expected ':' after IPv6 address")
                pattern = mp[closing_bracket + 2:]
            else:
                pattern = "*"
        elif ":" in mp:
            machine, pattern = mp.split(":", 1)
        else:
            machine = mp
            pattern = "*"

        m = Machine(
            machine,
            max_access_level="Observer",
        )

        with m:
            if len(machine_path) > 1:
                click.echo(f"{machine}:")
            files = m.list_runs_in_storage(pattern, verbose=True)

            if not files:
                click.echo(f"No runs found matching {pattern}")
                continue

            # Sort the files according to the specified criteria
            if sort == 'name':
                files.sort(key=lambda x: x['path'], reverse=reverse)
            elif sort == 'time':
                files.sort(key=lambda x: x['mtime'] if not created else x['ctime'], reverse=reverse)
            elif sort == 'size':
                files.sort(key=lambda x: x['size'], reverse=reverse)

            for f in files:
                if verbose:
                    size_str = f"{f['size']/1000/1000:.1f}M" if f['size'] > 1000*1000 else f"{f['size']/1000:.1f}k"
                    time_str = f['ctime'].strftime("%Y-%m-%d %H:%M") if created else f['mtime'].strftime("%Y-%m-%d %H:%M")
                    click.echo(f"{size_str:>8}  {time_str}  {f['path']}")
                else:
                    click.echo(f['path'])

@cli.command()
@click.argument("machine")
@click.argument("experiment")
@click.option("-o", "--output", type=click.Path())
def copy(
    machine: str,
    experiment: str,
    output: str | None,
) -> None:
    """Copy experiment from machine storage."""
    m = Machine(
        machine,
        max_access_level="Observer",
    )

    if output is None:
        output = experiment

    with m:
        exp = Experiment.from_machine(m, experiment)
        exp.save_file(output, update_files=False)

@cli.command()
@click.argument("path", nargs=-1)
@click.option("-f", "--force/--no-force", default=False, help="Overwrite existing files")
@click.option("-q", "--quiet/--no-quiet", default=False, help="Suppress output messages")
@click.option("-o", "--output-dir", type=click.Path(), help="Output directory for saved files")
def get_eds(path: tuple[str, ...], force: bool, quiet: bool, output_dir: str | None) -> None:
    """Get all experiments matching a name from the machine.
    
    Each argument should be of the form machine:name, where machine is the machine address
    and name is the glob pattern to match experiment names against.
    """
    if output_dir is not None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = Path(".")

    for machine_name in path:
        try:
            machine, name = machine_name.split(":", 1)
        except ValueError:
            click.echo(f"Error: argument '{machine_name}' must be in format 'machine:name'", err=True)
            continue

        m = Machine(machine, max_access_level="Observer")
        runs = m.list_runs_in_storage(name)

        if not runs:
            click.echo(f"No runs found matching {name} on {machine}", err=True)
            continue

        for r in runs:
            click.echo(f"Getting {r} from {machine}: ", nl=False)
            exp = Experiment.from_machine(m, r)

            oname = exp.runtitle_safe

            output_file = output_path / (oname + ".eds")
            if not force and output_file.exists():
                if not quiet:
                    click.echo(f"skipping, {output_file} already exists")
                continue
                
            if not quiet:
                click.echo(f"saving to {output_file}")
            exp.save_file(output_file, update_files=False)

@dataclass
class OutP:
    level: bool

    def verbose(self, s: str):
        if self.level:
            click.secho(s, fg="blue")

    def out(self, s: str):
        click.secho(s, nl=False)

    def good(self, s: str):
        click.secho(s, fg="green")

    def warn(self, s: str):
        click.secho(s, fg="yellow")

    def error(self, s: str):
        click.secho(s, fg="red")


class NoNewAccess(BaseException): ...


class NoAccess(BaseException): ...


def set_default_access(m: Machine, p: OutP):
    limitcontexts = m.run_command("CONF* access accessLimits").split()

    p.verbose("Current access:")
    for lc in limitcontexts:
        al = m.run_command(f"CONF? access accessLimits {lc}")
        p.verbose(f"\t{lc}: {al}")

    p.out("Setting default (passwordless) maximum access level to CONTROLLER: ")
    m.run_command("CONF= access accessLimits default CONTROLLER")
    m.run_command("CONF= access accessLimits eth0 CONTROLLER")
    p.good("done.")


def add_controller_password(m: Machine, p: OutP, newpass: str):
    p.verbose("Current password information:")
    passwords = m.run_command("CONF* access secrets").split()
    for password in passwords:
        pp = m.run_command(f"CONF? access secrets {password}")
        p.verbose(f"\t{password} : {pp}")
    p.out("Adding controller password: ")
    m.run_command(f"CONF= -createMissing access secrets {newpass} CONTROLLER")
    p.good("done.")


def add_administrator_password(m: Machine, p: OutP, newpass: str):
    p.verbose("Current password information:")
    passwords = m.run_command("CONF* access secrets").split()
    for password in passwords:
        pp = m.run_command(f"CONF? access secrets {password}")
        p.verbose(f"\t{password} : {pp}")
    p.out("Adding administrator password: ")
    m.run_command(f"CONF= -createMissing access secrets {newpass} ADMINISTRATOR")
    p.good("done.")


def start_ssh_backup(m: Machine, p: OutP, sshpass: str):
    p.out("Starting SSH in case of failure: ")
    m.run_command("SYST:EXEC 'dropbear -Y appletini &'")
    psout = m.run_command("SYST:EXEC -verbose 'ps'")
    assert "dropbear" in psout
    p.good("done.")
    p.warn(
        f"If procedure fails, SSH is running with user root and password {sshpass}"
        " (but note options required to connect)."
    )


def check_access(
    host: str,
    p: OutP,
    controller_pw: str | None,
    admin_pw: str | None,
    default_controller: bool,
):
    errs: list[CommandError] = []
    if controller_pw is not None:
        p.out("Checking controller password (machine LED should turn yellow): ")
        m = Machine(
            host, max_access_level=AccessLevel.Controller, password=controller_pw
        )
        try:
            with m.ensured_connection(AccessLevel.Controller):
                m.run_command("LED:YELLOWON")
                time.sleep(2)
                m.run_command("LED:BLUEON")
            p.good("succeeded.")
        except AuthError as e:
            p.error("Controller password was not set successfully.")
            errs.append(e)

    if admin_pw is not None:
        p.out("Checking administrator password (machine LED should turn red): ")
        m = Machine(host, max_access_level=AccessLevel.Administrator, password=admin_pw)
        try:
            with m.ensured_connection(AccessLevel.Administrator):
                m.run_command("LED:REDON")
                time.sleep(2)
                m.run_command("LED:BLUEON")
            p.good("succeeded.")
        except AuthError as e:
            p.error("Administrator password was not set successfully.")
            errs.append(e)

    if default_controller:
        p.out(
            "Checking passwordless controller access (machine LED should turn green): "
        )
        m = Machine(host, max_access_level=AccessLevel.Controller)
        try:
            with m.ensured_connection(AccessLevel.Controller):
                m.run_command("LED:GREENON")
                time.sleep(2)
                m.run_command("LED:BLUEON")
            p.good("succeeded.")
        except AccessLevelExceeded as e:
            p.error("Setting passwordless controller access failed.")
            errs.append(e)

    if errs:
        raise NoNewAccess


def stop_ssh_backup(m: Machine, p: OutP):
    p.out("Stopping SSH: ")
    m.run_command("SYST:EXEC 'killall dropbear'")
    psout = m.run_command("SYST:EXEC -verbose 'ps'")
    assert "dropbear" not in psout
    p.good("done.")


def error_ssh(m: Machine, p: OutP):
    raise NotImplementedError


def restart_is(m: Machine, p: OutP):
    p.out("Restarting zygote on machine: ")
    with m:
        with m.at_access(AccessLevel.Controller):
            try:
                m.restart_system()
            except ConnectionError:
                p.good("succeeded.")
            else:
                p.good("command succeeded.")
    p.out("Waiting 30 seconds for restart: ")
    time.sleep(30)
    p.good("done.")
    p.out("Checking connection: ")
    with m:
        pass
    p.good("succeeded.")


@cli.command()
@click.argument("host")
@click.argument("current_password")
@click.option("-c", "--controller-password", default=None)
@click.option("-a", "--admin-password", default=None)
@click.option("-d", "--default-controller/--no-default-controller", default=False)
@click.option("-v", "--verbose/--no-verbose", default=False)
def setup_machine(
    host: str,
    current_password: str,
    controller_password: str | None,
    admin_password: str | None,
    default_controller: bool,
    verbose: bool,
):
    p = OutP(verbose)

    m = Machine(
        host, password=current_password, max_access_level=AccessLevel.Controller
    )

    try:
        with m.ensured_connection(AccessLevel.Controller):
            start_ssh_backup(m, p, "emergencypassword")
            if controller_password:
                add_controller_password(m, p, controller_password)
            if admin_password:
                add_administrator_password(m, p, admin_password)
            if default_controller:
                set_default_access(m, p)
    except AuthError as e:
        p.error("Current password was not valid.")
        if verbose:
            p.error(str(e))
    except InsufficientAccess as e:
        p.error(
            "Current password was valid, but insufficient.  This script needs at least Controller access."
        )
        p.error(str(e))
    restart_is(m, p)

    try:
        check_access(host, p, controller_password, admin_password, default_controller)

        with m:
            with m.at_access(AccessLevel.Controller):
                stop_ssh_backup(m, p)
    except NoNewAccess:
        # Do we have *any* access?
        try:
            with Machine(
                host, password=current_password, max_access_level=AccessLevel.Controller
            ) as m:
                stop_ssh_backup(m, p)
        except Exception as e:
            p.error("Access failed even with old password.")
            p.error(f"Error was {e}")
            p.error("Machine can be accessed directly via SSH using:")
            p.error(
                f"    ssh root@{host} -p 2222 -o KexAlgorithms=diffie-hellman-group1-sha1"
                " -o HostKeyAlgorithms='+ssh-rsa'"
            )
            p.error("Password is 'emergencypassword'.")
            p.error("DO NOT REBOOT THE MACHINE or SSH access will be lost.")
        else:
            p.error("New access failed, but old password still works.")


if __name__ == "__main__":
    cli()
