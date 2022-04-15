import csv
import datetime
import logging
import os
import sys
from typing import Union
from time import sleep

from backend import fetch_accession_csv, filter_accession_csv, create_droplet_farm, path, stage_names
from backend import Priority as Priority
from backend import Stage as Stage
from backend import RunStatus as RunStatus

import click
import pandas


def load_csv(csv_file: str) -> Union[pandas.DataFrame, bool]:
    if not os.path.exists(csv_file):
        logging.error(f"Cannot find CSV tracker file at {csv_file}")
        raise FileNotFoundError(f"CSV tracker file {csv_file} does not exist.")

    with open(csv_file, 'r') as f:
        logging.info(f"Reading taxon information from {csv_file}")
        logging.debug(f"Checking header integrity of {csv_file}")
        # Check file integrity
        head = next(csv.reader(f))
        logging.debug(f"Headers: {', '.join(head)}")
        # note the date field for pandas parsing
        date_fields = [head.index(x) for x in ['last_checked', 'checkpoint_time']]
        if not all([h in head for h in headers]):
            missing_headers = [h for h in headers if h in head]
            raise KeyError(f"{csv_file} missing headers: {', '.join(missing_headers)}")

    logging.debug(f"Headers okay. Loading content.")
    try:
        data = pandas.read_csv(csv_file, index_col='index', parse_dates=date_fields, dtype=dtypes).fillna('')
    except pandas.errors.EmptyDataError:
        data = pandas.DataFrame(columns=head, dtype=dtypes).set_index('index').fillna('')
        logging.debug(f"File is empty.")

    return data


dtypes = {
    'index': "int",
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
csv_location = path("data/microfetch.csv")


@click.group()
@click.option(
    '-f', '--file', 'csv_file',
    required=False,
    default=csv_location,
    type=click.Path(),
    help='Tracker CSV file created with `init`.'
)
@click.option(
    '-v', '--verbose',
    is_flag=True,
    help='Print debugging information.'
)
@click.pass_context
def taxon_tracker(ctx: click.Context, csv_file: str, verbose: bool) -> None:
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if verbose else logging.INFO)

    # ensure that ctx.obj exists and is a dict (in case script is called
    # by means other than the main `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['CSV_FILE_PATH'] = csv_file
    ctx.obj['VERBOSE'] = verbose


@taxon_tracker.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """
    Create CSV file for tracking taxon_ids.
    """
    csv_file = ctx.obj['CSV_FILE_PATH']
    if not os.path.exists(csv_file):
        with open(csv_file, 'w+') as f:
            logging.debug(f"Creating {csv_file} with headers: {', '.join(headers)}")
            writer = csv.writer(f)
            writer.writerow(headers)
            logging.info(f"Created {csv_file}.")
    else:
        try:
            load_csv(csv_file=csv_file)
        except KeyError as e:
            logging.error(f"Cannot init tracking with invalid CSV file {csv_file}.")
            raise e

        logging.info(f"Suitable CSV file already exists at {csv_file}.")

    logging.info((
        "Tracking now initialized and ready for running process_next."
        "\n\tUse `add` to add new taxon_ids to the tracker."
        "\n\tYou may wish to add `process_next` as a cron job to periodically process taxon_ids."
    ))


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
    data = load_csv(csv_file=ctx.obj['CSV_FILE_PATH'])

    if data.loc[data.taxon_id == taxon_id].shape[0] > 0:
        logging.info(f"Already tracking taxon_id {taxon_id} -- doing nothing.")
    else:
        logging.info((
            f"Adding new taxon_id {taxon_id} to taxon list. "
            f"This taxon_id will be processed in due course."
        ))
        new_entry = pandas.DataFrame.from_dict({
            'index': [0],
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
        new_entry = new_entry.set_index('index')
        data = pandas.concat([data, new_entry])
        data.to_csv(ctx.obj['CSV_FILE_PATH'])


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

    data = load_csv(csv_file=ctx.obj['CSV_FILE_PATH'])

    # Summarise the current status of trackers
    print(f"Taxon tracking status:")
    for s in RunStatus:
        logging.debug(s)
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


def do_next_action(ctx: click.Context) -> float:
    """
    Run the next job in the queue and return the amount of time to sleep after completing.
    """
    # Find the next job
    data = load_csv(csv_file=ctx.obj['CSV_FILE_PATH'])

    candidates = data.loc[(data.priority > 0)]
    if candidates.shape[0] == 0:
        raise RuntimeWarning(f"There are no taxon ids that require processing.")

    candidates = candidates.sort_values(by=['priority'], ascending=False, na_position='last')
    row = candidates.iloc[0]

    # Execute the job
    logging.basicConfig(
        filename=path(f'log/{row.taxon_id}.log'),
        level=logging.DEBUG if ctx.obj['VERBOSE'] else logging.INFO
    )
    try:
        if row.stage == Stage.FETCH_ACCESSION_CSV or row.stage == Stage.UPDATE_ACCESSION_CSV:
            logging.info(f"Processing taxon id {row.taxon_id}: Fetch accession CSV")
            data.loc[data.taxon_id == row.taxon_id] = fetch_accession_csv(ctx=ctx, row=row)
        elif row.stage == Stage.FILTER_ACCESSION_CSV:
            logging.info(f"Processing taxon id {row.taxon_id}: Filter accession numbers")
            data.loc[data.taxon_id == row.taxon_id] = filter_accession_csv(ctx=ctx, row=row)
        elif row.stage == Stage.CREATE_DROPLET_FARM:
            logging.info(f"Processing taxon id {row.taxon_id}: Create droplet farm")
            data.loc[data.taxon_id == row.taxon_id] = create_droplet_farm(ctx=ctx, row=row)
        else:
            raise RuntimeError(f"Unrecognised stage '{row.stage}' for taxon id {row.taxon_id}")
    except Warning:
        data.loc[data.taxon_id == row.taxon_id, ['generated_warning']] = ['True']
    except Exception:
        data.loc[data.taxon_id == row.taxon_id, ['last_run_status', 'priority']] = [
            'error',
            Priority.STOPPED.value
        ]

    data.to_csv(ctx.obj['CSV_FILE_PATH'])
    return 0.0


@taxon_tracker.command()
@click.option(
    '--once', 'stop',
    is_flag=True,
    help='Perform only one heartbeat step.'
)
@click.pass_context
def heartbeat(ctx: click.Context, sleep_time: float = 1.0, max_sleep: float = 3600.0, stop: bool = True) -> None:
    """
    Daemon-like serial processing of jobs.
    """
    def main_logging():
        logging.basicConfig(
            filename=path('log/main.log'),
            level=logging.DEBUG if ctx.obj['VERBOSE'] else logging.INFO
        )

    try:
        main_logging()
        if stop:
            logging.debug(f"heartbeat called with --once: performing next action and stopping.")
            logging.debug(ctx)
        sleep_time = do_next_action(ctx=ctx)
    except Warning as w:
        main_logging()
        sleep_time = sleep_time * 2
        if sleep_time > max_sleep:
            sleep_time = max_sleep
        logging.warning(w)
    except Exception as e:
        main_logging()
        logging.error(e)

    if not stop:
        sleep(sleep_time)
        heartbeat(sleep_time=sleep_time, max_sleep=max_sleep, stop=stop)


if __name__ == '__main__':
    taxon_tracker(obj={})
