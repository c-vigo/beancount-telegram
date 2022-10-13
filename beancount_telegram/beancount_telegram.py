""" Telegram chat history archiver
"""
import logging

# Imports
from telethon.sync import TelegramClient
from os.path import isfile, expanduser
from pathlib import Path
from os import remove
import csv
from argparse import ArgumentParser, Action, ArgumentError, ArgumentTypeError
from dateutil import parser
from typing import Dict
from fnmatch import fnmatch

# Local packages
from .__project__ import (
    __documentation__ as docs_url,
    __module_name__ as module,
    __description__ as prog_desc,
)


class AttachmentPattern:
    def __init__(self, account: str, pattern: str, skip_init: int, skip_end: int, name: str):
        self.account = account
        self.pattern = pattern
        self.skip_init = skip_init
        self.skip_end = skip_end
        self.name = name

    def __str__(self) -> str:
        return '(Account {}; Pattern {}, Skip {}-{}, Name {})'.format(
            self.account,
            self.pattern,
            self.skip_init,
            self.skip_end,
            self.name
        )


class ParseAttachmentPattern(Action):
    def __call__(self, arg_parser, namespace, values, option_string=None):
        # First pattern?
        if getattr(namespace, self.dest, None) is None:
            setattr(namespace, self.dest, list())

        # Add pattern
        try:
            for value in values:
                account, pattern, skip_init, skip_end, name = value.split(';')
                getattr(namespace, self.dest).append(AttachmentPattern(
                    account, pattern, int(skip_init), int(skip_end), name
                ))
        except:
            raise ArgumentTypeError('Invalid attachment pattern')


class ParseDict(Action):
    def __call__(self, arg_parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value


def build_file_name(account: str, year: str, args) -> str:
    if account not in args.account_map:
        raise KeyError(account)
    account_info = args.account_map[account]
    base_folder = args.root_folder + account_info.replace(':', '/') + '/'
    return base_folder + year + '-12-31-' + account.replace(" ", "") + '_Transactions_TelegramBot.csv'


def check_connection(args):
    print('Connecting to Telegram client')
    client = TelegramClient(args.session_file, args.api_id, args.api_hash)
    with client:
        # Loop over messages
        the_chat = client.get_entity(args.chat_id)
        print('Chat name: {}'.format(the_chat.title))
        for msg in client.iter_messages(args.chat_id, reverse=False, limit=1):
            print('Last message ({}): {}'.format(msg.date, msg.text))


def beancount_telegram():
    """The main routine. It parses the input argument and acts accordingly."""
    fieldnames = ['id', 'sender', 'message_date', 'transaction_date', 'account',
                  'payee', 'description', 'amount', 'currency', 'tag']

    # The argument parser
    ap = ArgumentParser(
        prog=module,
        description=prog_desc,
        add_help=True,
        epilog='Check out the package documentation for more information:\n{}'.format(docs_url)
    )
    # Arguments:
    # API ID
    ap.add_argument(
        'api_id',
        help='The API ID you obtained from https://my.telegram.org',
        type=int,
    )
    # API hash
    ap.add_argument(
        'api_hash',
        help='The API hash you obtained from https://my.telegram.org',
        type=str,
    )
    # Chat ID
    ap.add_argument(
        'chat_id',
        help='The chat ID',
        type=int,
    )
    # Root Folder
    root_arg = ap.add_argument(
        '-r',
        '--root-folder',
        help='The beancount records root folder',
        type=str,
    )
    # Uncategorized attachment path
    tmp_arg = ap.add_argument(
        '-t',
        '--temp-folder',
        help='The beancount temporary records folder',
        type=str,
    )
    # Account map
    acc_arg = ap.add_argument(
        '-acc',
        '--account-map',
        nargs='*',
        action=ParseDict
    )
    # Attachments map
    ap.add_argument(
        '-att',
        '--attachment-map',
        nargs='*',
        action=ParseAttachmentPattern
    )
    # Session file
    ap.add_argument(
        '-s',
        '--session-file',
        help='Session file to store credentials',
        type=str,
        default=expanduser('~/.config/beancount_telegram/session.txt'),
    )
    # Force update
    ap.add_argument(
        '-f',
        '--force',
        action='store_true',
        help='Force re-download of old transactions',
        default=False,
    )
    # Download files
    ap.add_argument(
        '-nd',
        '--no-download',
        action='store_true',
        help='Do not download files',
        default=False,
    )
    # Dry run
    ap.add_argument(
        '-n',
        '--dry-run',
        action='store_true',
        help='Perform a dry run without altering any file',
        default=False,
    )
    # Check connection
    ap.add_argument(
        '-c',
        '--check',
        action='store_true',
        help='Check access to Telegram chat and quit',
        default=False,
    )

    # Parse the arguments
    args = ap.parse_args()

    # Path to session file
    Path(args.session_file).mkdir(parents=True, exist_ok=True)

    # Check connection?
    if args.check:
        check_connection(args)
        return

    # Mandatory arguments
    if args.account_map is None:
        raise ArgumentError(acc_arg, 'Missing account map')
    if not args.no_download:
        if args.root_folder is None:
            raise ArgumentError(root_arg, 'Missing beancount records root folder')
        if args.temp_folder is None:
            raise ArgumentError(tmp_arg, 'Missing beancount records temporary folder')

    lastMessageId = 0
    if args.force and not args.dry_run:
        # Clean files if --force
        print('Cleaning old files')
        for account in args.account_map.keys():
            filename = build_file_name(account, '2022', args)
            if isfile(filename):
                print('Deleting ', filename)
                remove(filename)
    else:
        # Only update files, find the latest message in saved files
        for account in args.account_map.keys():
            filename = build_file_name(account, '2022', args)
            if isfile(filename):
                with open(filename, 'r', encoding='utf8') as csvfile:
                    reader = csv.DictReader(
                        csvfile,
                        ['id', 'sender', 'message_date', 'transaction_date', 'account', 'payee', 'description', 'amount', 'currency', 'tag'],
                        delimiter=";"
                    )
                    rows = list(reader)[1:]
                    for row in rows:
                        messageId = int(row["id"])
                        if messageId > lastMessageId:
                            lastMessageId = messageId
        print('Updating messages with ID > ', lastMessageId)

    # Connect to the Telegram client
    client = TelegramClient(args.session_file, args.api_id, args.api_hash)
    with client:
        # Loop over messages
        for msg in client.iter_messages(args.chat_id, reverse=True, min_id=lastMessageId):
            # Retrieve message info
            entry: Dict = {
                'id': msg.id,
                'sender': msg.sender.first_name.strip(),
                'message_date': msg.date.strftime('%Y-%m-%d')
            }

            # Parse fields of a valid transaction
            try:
                # Parse message text
                fields = msg.text.split(";")
                entry['transaction_date'] = parser.parse(fields[0]).strftime('%Y-%m-%d')
                entry['account'] = fields[1].strip().replace(" ", "")
                entry['payee'] = fields[2].strip()
                entry['description'] = fields[3].strip()
                amount = fields[4].strip().split(" ")
                entry['amount'] = amount[0].strip()
                entry['currency'] = amount[1].strip()
                entry['tag'] = fields[5].strip() if len(fields) > 5 else ''

                # Identify account
                if entry['account'] not in args.account_map:
                    print('Warning: Invalid account <', entry['account'], '> in message <', msg.text, '> from ', entry['message_date'])
                    continue

                # File name associated to transaction
                filename = build_file_name(entry['account'], entry['transaction_date'][0:4], args)

                # Dry run?
                if args.dry_run:
                    print(filename, entry)
                    continue

                # Create new file?
                if not isfile(filename):
                    print('Creating file ', filename)
                    with open(filename, 'w', encoding='UTF8', newline='') as f:
                        # Create the csv writer and write the header
                        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
                        writer.writeheader()

                        # Write the entry
                        writer.writerow(entry)
                # Open file to append entry
                else:
                    with open(filename, 'a', encoding='UTF8', newline='') as f:
                        # Create the csv writer and append the entry
                        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
                        writer.writerow(entry)

                # Parsing successful, get next message
                continue
            except BaseException:
                pass

            # Attachments
            try:
                document = msg.document
                name = document.attributes[0].file_name
                extension = name[-4:]

                # Download?
                if args.no_download:
                    continue

                # Pattern matching
                for pattern in args.attachment_map:
                    if fnmatch(name, pattern.pattern):
                        try:
                            # Parse date
                            date = parser.isoparse(name[pattern.skip_init:pattern.skip_end]).strftime('%Y-%m-%d')

                            # Build filename
                            base_folder = args.root_folder + pattern.account.replace(':', '/') + '/'
                            filename = base_folder + date + '-' + pattern.name + extension
                            if isfile(filename):
                                filename = filename[:-4] + '_2' + filename[-4:]

                            # Download file?
                            if args.dry_run:
                                real_filename = filename
                            else:
                                real_filename = client.download_media(message=msg, file=filename)
                            break

                        except BaseException:
                            pass
                else:
                    # File does not match any pattern
                    filename = args.temp_folder + '/' + name
                    if isfile(filename):
                        filename = filename[:-4] + '_2' + filename[-4:]
                    if args.dry_run:
                        real_filename = filename
                    else:
                        real_filename = client.download_media(message=msg, file=filename)
                print('File downloaded: ', real_filename)
                continue

            except BaseException:
                pass

            print('Warning: Invalid message <', msg.text, '> from ', entry['message_date'])

# TODO: download media
# TODO: arcosana benefit vs premium