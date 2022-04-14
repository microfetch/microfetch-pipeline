import csv
import datetime
import logging
import os
import sys
from typing import Union

import click
import pandas

headers = ['index', 'taxon_id', 'last_checked', 'droplet_ips', 'last_run_status', 'needs_attention']
csv_location = "microfetch.csv"


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
        date_field = head.index(headers[1])  # note the date field for pandas parsing
        if not all([h in head for h in headers]):
            missing_headers = [h for h in headers if h in head]
            raise KeyError(f"{csv_file} missing headers: {', '.join(missing_headers)}")

    logging.debug(f"Headers okay. Loading content.")
    try:
        data = pandas.read_csv(csv_file, index_col='index', parse_dates=[date_field], dtype="string").fillna('')
    except pandas.errors.EmptyDataError:
        data = pandas.DataFrame(columns=head, dtype="string").set_index('index').fillna('')
        logging.debug(f"File is empty.")

    return data


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
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if verbose else logging.INFO)
    # ensure that ctx.obj exists and is a dict (in case script is called
    # by means other than the main `if` block below)
    ctx.ensure_object(dict)

    ctx.obj['CSV_FILE_PATH'] = csv_file


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
            'droplet_ips': [''],
            'last_run_status': [''],
            'needs_attention': ['']
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
    def summary(nice_name: str, key: str) -> str:
        return f"{nice_name}: {data[(data.last_run_status == key)].shape[0]}"

    def detail(key: str) -> str:
        return '\n\t'.join(data[(data.last_run_status == '')].taxon_id)

    data = load_csv(csv_file=ctx.obj['CSV_FILE_PATH'])

    # Summarise the current status of trackers
    print(f"Taxon tracking status:")
    print(summary('New', ''))
    if verbosity > 1:
        print(detail(''))
    print(summary('In progress', 'in progress'))
    if verbosity > 1:
        print(detail('in progress'))
    print(summary('Awaiting download', 'ready'))
    if verbosity > 0:
        print(detail('ready'))
    print(summary('Completed', 'complete'))
    if verbosity > 1:
        print(detail('complete'))
    print(summary('Completed with warning', 'warning'))
    if verbosity > 0:
        print(detail('warning'))
    print(summary('Errored', 'error'))
    if verbosity > 0:
        print(detail('error'))
    print(f"Additionally, {data[(data.needs_attention == 'True')].shape[0]} records require attention.")
    if verbosity > 0:
        print("\n\t".join(data[(data.needs_attention == 'True')].taxon_id))


@taxon_tracker.command()
@click.pass_context
def process_next(ctx: click.Context) -> Union[tuple[str, str], bool]:
    """
    Run the pipeline for the least recently updated taxon_id in the tracker CSV file.
    """
    output = {
        'taxon_id': None,
        'last_update': None
    }

    data = load_csv(csv_file=ctx.obj['CSV_FILE_PATH'])

    # don't redo in-progress taxons
    candidates = data.loc[((pandas.isna(data['last_run_status']) is False) | (data['last_run_status'] != 'in progress'))]

    if candidates.shape[0] == 0:
        logging.info(f"There are no taxon ids that require processing. Stopping.")
        return False

    candidates = candidates.sort_values(by=[headers[1]], ascending=True, na_position='first')
    row = candidates.iloc[0]

    logging.info(f"Selected {row.taxon_id} for processing.")

    output['last_update'] = row.last_checked
    output['taxon_id'] = row.taxon_id
    if row.last_checked:
        logging.info(f"{row.taxon_id} last updated {row.last_checked}.")
    else:
        logging.info(f"{row.taxon_id} has never been fetched.")

    # Update values in original dataframe
    data.loc[data.taxon_id == row.taxon_id, headers[1:]] = [
        '570', datetime.datetime.utcnow().isoformat(), '', 'in progress', ''
    ]
    data.to_csv(ctx.obj['CSV_FILE_PATH'])

    return output['taxon_id'], output['last_update']


if __name__ == '__main__':
    taxon_tracker(obj={})
