import csv
import datetime
import logging
import os
import re
import click
import pandas

from pathlib import Path
from typing import Union
from time import sleep

from backend import fetch_accession_csv, filter_accession_csv, create_droplet_farm, stage_names
from backend import Priority as Priority
from backend import Stage as Stage
from backend import RunStatus as RunStatus
from backend import Route as Route
from backend import logger as backend_logger
from backend import log_format as log_format

from backend.server import run_server

logger = logging.getLogger(__file__)

dtypes = {
    'taxon_id': "string",
    'last_checked': "string",
    'last_run_status': "string",
    'needs_attention': "string",
    'stage': "int",
    'stage_name': "string",
    'priority': "int",
    'generated_warning': "string",
    'checkpoint_time': "string"
}
headers = dtypes.keys()
data_directory = os.path.join(os.path.dirname(__file__), '..', 'data')


def load_csv(csv_file: str) -> Union[pandas.DataFrame, bool]:
    if not os.path.exists(csv_file):
        logger.error(f"Cannot find CSV tracker file at {csv_file}")
        raise FileNotFoundError(f"CSV tracker file {csv_file} does not exist.")

    with open(csv_file, 'r') as f:
        logger.info(f"Reading taxon information from {csv_file}")
        logger.debug(f"Checking header integrity of {csv_file}")
        # Check file integrity
        head = next(csv.reader(f))
        logger.debug(f"Headers: {', '.join(head)}")
        # note the date field for pandas parsing
        date_fields = [head.index(x) for x in ['last_checked', 'checkpoint_time']]
        if not all([h in head for h in headers]):
            missing_headers = [h for h in headers if h in head]
            raise KeyError(f"{csv_file} missing headers: {', '.join(missing_headers)}")

    logger.debug(f"Headers okay. Loading content.")
    try:
        data = pandas.read_csv(csv_file, parse_dates=date_fields, dtype=dtypes).fillna('')
    except pandas.errors.EmptyDataError:
        data = pandas.DataFrame(columns=head, dtype=dtypes).fillna('')
        logger.debug(f"File is empty.")

    return data


@click.group()
@click.option(
    '-d', '--data-directory', 'data_dir',
    required=False,
    default=data_directory,
    type=click.Path(),
    help='Program data location.'
)
@click.option(
    '-l', '--log-directory', 'log_dir',
    required=False,
    default=None,
    type=click.Path(),
    help='Logfile location -- if empty will be in <data_dir>/log.'
)
@click.option(
    '-v', '--verbose',
    is_flag=True,
    help='Print debugging information.'
)
@click.pass_context
def taxon_tracker(
        ctx: click.Context,
        data_dir: str = data_directory,
        log_dir: str = None,
        verbose: bool = False
) -> None:
    # ensure that ctx.obj exists and is a dict (in case script is called
    # by means other than the main `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['DATA_DIR'] = os.path.abspath(data_dir)
    ctx.obj['LOG_DIR'] = os.path.abspath(log_dir if log_dir else os.path.join(ctx.obj['DATA_DIR'], 'log'))
    ctx.obj['VERBOSE'] = verbose

    # Set up logging
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_format)
        logger.addHandler(stream_handler)

        log_location = Path(os.path.join(ctx.obj['LOG_DIR'], Route.MAIN_LOG.value))
        # Create the directory skeleton if necessary
        os.makedirs(os.path.dirname(log_location), exist_ok=True)
        log_location.touch(exist_ok=True)
        file_handler = logging.FileHandler(filename=log_location, encoding='utf-8')
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)

        logger.setLevel(logging.DEBUG if ctx.obj['VERBOSE'] else logging.INFO)


@taxon_tracker.command()
@click.pass_context
def serve(ctx: click.Context) -> None:
    run_server(ctx=ctx)


@taxon_tracker.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """
    Create files and directories for tracking taxon_ids.
    """
    # Create directories
    paths = [
        os.path.join(ctx.obj['LOG_DIR']),
        os.path.join(ctx.obj['DATA_DIR'], Route.QUEUE_DIR.value),
        os.path.join(ctx.obj['DATA_DIR'], Route.ACCESSION_DIR.value),
        os.path.join(ctx.obj['DATA_DIR'], Route.METADATA_DIR.value)
    ]
    for path in paths:
        if os.path.exists(path):
            logger.debug(f"Path {path} already exists.")
        else:
            os.makedirs(path, exist_ok=True)
            logger.debug(f"Created {path}")

    csv_file = os.path.join(ctx.obj['DATA_DIR'], Route.CSV.value)
    if not os.path.exists(csv_file):
        with open(csv_file, 'w+') as f:
            logger.debug(f"Creating {csv_file} with headers: {', '.join(headers)}")
            writer = csv.writer(f)
            writer.writerow(headers)
            logger.info(f"Created {csv_file}.")
    else:
        try:
            load_csv(csv_file=csv_file)
        except KeyError as e:
            logger.error(f"Cannot init tracking with invalid CSV file {csv_file}.")
            raise e

        logger.info(f"Suitable CSV file already exists at {csv_file}.")

    logger.info((
        "Tracking now initialized and ready for running process_next."
        "\n\tUse `add` to add new taxon_ids to the tracker."
        "\n\tYou may wish to add `process_next` as a cron job to periodically process taxon_ids."
    ))


def _add_to_queue(command: str, ctx: click.Context) -> None:
    Path(os.path.join(ctx.obj["DATA_DIR"], Route.QUEUE_DIR.value, command)).touch(exist_ok=True)


@taxon_tracker.command()
@click.argument(
    'taxon_id',
    required=True
)
@click.pass_context
def add(ctx: click.Context, taxon_id: str) -> None:
    """
    Add a taxon_id to the tracker.
    When the pipeline processes the taxon_id, it will also fetch subtrees for that taxon_id.

    Adding a taxon_id will not force that taxon_id to be processed next.
    It will be prioritised by process_next above previously-run taxon_ids.
    """
    _add_to_queue(command=f"add {taxon_id}", ctx=ctx)
    logger.info(f"Registered command 'add {taxon_id}' in command queue.")


def _add(ctx: click.Context, taxon_id: str) -> None:
    data = load_csv(csv_file=os.path.join(ctx.obj['DATA_DIR'], Route.CSV.value))
    taxon_id = str(taxon_id)

    if data.loc[data.taxon_id == taxon_id].shape[0] > 0:
        logger.info(f"Already tracking taxon_id {taxon_id} -- doing nothing.")
    else:
        logger.info((
            f"Adding new taxon_id {taxon_id} to taxon list. "
            f"This taxon_id will be processed in due course."
        ))
        new_entry = pandas.DataFrame.from_dict({
            'taxon_id': [taxon_id],
            'last_checked': [''],
            'last_run_status': ['new'],
            'needs_attention': [''],
            'stage': [Stage.FETCH_ACCESSION_CSV.value],
            'stage_name': [stage_names[Stage.FETCH_ACCESSION_CSV]],
            'priority': [Stage.FETCH_ACCESSION_CSV.value],
            'generated_warning': [''],
            'checkpoint_time': [datetime.datetime.utcnow().isoformat()]
        }, dtype="string")
        data = pandas.concat([data, new_entry])
        data.to_csv(os.path.join(ctx.obj['DATA_DIR'], Route.CSV.value), index=False)


def _update_droplet_status(ctx: click.Context, taxon_id: str, droplet_ip: str, new_status: str) -> None:
    """
    Update the status of all accession records assigned to a droplet.
    """
    csv_file = os.path.join(ctx.obj['DATA_DIR'], Route.ACCESSION_DIR.value, taxon_id)
    df = pandas.read_csv(csv_file)
    df[df.droplet_ip == droplet_ip, 'status'] = new_status
    df.to_csv(csv_file)


@taxon_tracker.command()
@click.option(
    '-v', '--verbosity',
    required=False,
    default=0,
    help=(
            'Print detailed information. '
            'Verbosity levels: 0=summary; 1=details for warning/error/needs attention; 2=details for all.'
    )
)
@click.pass_context
def status(ctx: click.Context, verbosity: int) -> None:
    """
    Show the status of currently tracked taxon_ids.
    """
    def summary(key: str) -> str:
        print(f"{key}: {data[(data.last_run_status == key)].shape[0]}")

    def detail(key: str) -> str:
        ids = data[(data.last_run_status == key)].taxon_id
        if len(ids):
            print(f"\t{', '.join(ids)}")

    data = load_csv(csv_file=os.path.join(ctx.obj['DATA_DIR'], Route.CSV.value))

    # Summarise the current status of trackers
    print(f"Taxon tracking status:")
    for s in RunStatus:
        logger.debug(s)
        summary(s.value)
        if s == RunStatus.IN_PROGRESS:
            if verbosity == 1:
                for stage in Stage:
                    print(f"{stage_names[stage]}: {data[(data.stage == stage)].shape[0]}")
        if verbosity > 1 or (verbosity > 0 and s > RunStatus.COMPLETE_WITH_WARNING):
            detail(s.value)

    print(f"Additionally, {data[(data.needs_attention == 'True')].shape[0]} records require attention.")
    if verbosity > 0:
        ids = data[(data.needs_attention == 'True')].taxon_id
        if len(ids):
            print(f"\t{', '.join(ids)}")


def process_queued_queries(ctx: click.Context) -> None:
    """
    Process the actions registered in the data/.queue directory.
    """
    # Commands are queued by creating empty files with the command as the filename
    # in a special directory data/.queue
    # This approach avoids write conflicts in data/microfetch.csv
    d = os.path.join(ctx.obj["DATA_DIR"], Route.QUEUE_DIR.value)
    files = os.listdir(d)
    if len(files) == 0:
        return

    for file in files:
        try:
            command, args = re.match(r"^(?P<command>\S+) (?P<args>.+)$", file).groups()
            if command == "":
                continue
            else:
                command = command.lower()
                args = args.lower()
            if command == "add":
                _add(ctx=ctx, taxon_id=args)
            if command == "update-droplet-status":
                t, d, s = args.split(" ")
                _update_droplet_status(ctx=ctx, taxon_id=t, droplet_ip=d, new_status=s)
            os.remove(os.path.join(d, file))
        except AttributeError:
            continue


def do_next_action(ctx: click.Context, log_to_console: bool = True) -> float:
    """
    Run the next job in the queue and return the amount of time to sleep after completing.
    """
    # Find the next job
    data = load_csv(csv_file=os.path.join(ctx.obj['DATA_DIR'], Route.CSV.value))

    candidates = data.loc[(data.priority > 0)]
    if candidates.shape[0] == 0:
        raise RuntimeWarning(f"There are no taxon ids that require processing.")

    candidates = candidates.sort_values(by=['priority'], ascending=False, na_position='last')
    row = candidates.iloc[0]

    # Set backend logging to file
    log_path = Path(os.path.join(ctx.obj['LOG_DIR'], f'{row.taxon_id}.log'))
    log_path.touch()
    for h in backend_logger.handlers:
        backend_logger.removeHandler(h)
    handler = logging.FileHandler(filename=log_path, encoding='utf-8')
    handler.setFormatter(log_format)
    backend_logger.addHandler(handler)
    backend_logger.setLevel(logging.DEBUG if ctx.obj['VERBOSE'] else logging.INFO)
    backend_logger.propagate = log_to_console

    # Execute the job
    try:
        if row.stage == Stage.FETCH_ACCESSION_CSV.value or row.stage == Stage.UPDATE_ACCESSION_CSV.value:
            logger.info(f"Processing taxon id {row.taxon_id}: Fetch accession CSV")
            result = fetch_accession_csv(row=row, context=ctx.obj)
        elif row.stage == Stage.FILTER_ACCESSION_CSV.value:
            logger.info(f"Processing taxon id {row.taxon_id}: Filter accession numbers")
            result = filter_accession_csv(row=row, context=ctx.obj)
        elif row.stage == Stage.CREATE_DROPLET_FARM.value:
            logger.info(f"Processing taxon id {row.taxon_id}: Create droplet farm")
            result = create_droplet_farm(row=row, context=ctx.obj)
        else:
            raise RuntimeError(f"Unrecognised stage '{row.stage}' for taxon id {row.taxon_id}")
        data.loc[data.taxon_id == row.taxon_id] = result.array
    except Warning as w:
        logger.warning(w)
        data.loc[data.taxon_id == row.taxon_id, ['generated_warning']] = ['True']
    except Exception as e:
        logger.error(e)
        data.loc[data.taxon_id == row.taxon_id, ['last_run_status', 'priority']] = [
            'error',
            Priority.STOPPED.value
        ]

    data.to_csv(os.path.join(ctx.obj['DATA_DIR'], Route.CSV.value), index=False)
    return 60.0  # TODO: This should be a setting or something. In real deploys it can be ~0.0


@taxon_tracker.command()
@click.option(
    '--once', 'stop',
    is_flag=True,
    help='Perform only one heartbeat step.'
)
@click.pass_context
def heartbeat(
        ctx: click.Context,
        sleep_time: float = 1.0,
        max_sleep: float = 3600.0,
        stop: bool = True
) -> None:
    """
    Daemon-like serial processing of jobs.
    """
    _heartbeat(ctx=ctx, sleep_time=sleep_time, max_sleep=max_sleep, stop=stop)


def _heartbeat(
        ctx: click.Context,
        sleep_time: float = 1.0,
        max_sleep: float = 3600.0,
        stop: bool = True
) -> None:
    sleep_s = sleep_time

    while True:
        try:
            process_queued_queries(ctx=ctx)
        except Exception as e:
            logger.error(e)
        try:
            sleep_s = do_next_action(ctx=ctx, log_to_console=stop)
        except Warning as w:
            if not sleep_s:
                sleep_s = sleep_time
            sleep_s = sleep_s * 2
            if sleep_s > max_sleep:
                sleep_s = max_sleep
            logger.warning(w)
        except Exception as e:
            sleep_s = sleep_time * 2
            logger.error(e)

        if stop:
            logger.debug(f"Stopping due to --once flag.")
            break

        logger.debug(f"Sleeping for {sleep_s}s.")
        sleep(sleep_s)


if __name__ == '__main__':
    taxon_tracker(obj={})
