#!/usr/bin/env python3

# TODO Check that README.md is accurate in terms of defaults.

# Program Name: zoom-recording-downloader.py
# Description:  Zoom Recording Downloader is a cross-platform Python script
#               that uses Zoom's API (v2) to download and organize all
#               cloud recordings from a Zoom account onto local storage.
#               This Python script uses the OAuth method of accessing the Zoom API
# Created:      2020-04-26
# Author:       Ricardo Rodrigues
# Website:      https://github.com/ricardorodrigues-ca/zoom-recording-downloader
# Forked from:  https://gist.github.com/danaspiegel/c33004e52ffacb60c24215abf8301680

# system libraries
import base64
import datetime
import importlib
import json
import os
import signal
import sys as system

# installed libraries
import dateutil.parser as parser
import pathvalidate as path_validate
import requests
import tqdm as progress_bar

# Local imports
from lib.strategies import MeetingHelperStrategy
from lib.console import Console

CONF_PATH = "zoom-recording-downloader.conf"
with open(CONF_PATH, encoding="utf-8-sig") as json_file:
    CONF = json.loads(json_file.read())


def config(section, key=None, default=''):
    """
    Retrieves a configuration value by section and key.
    If key is None, returns the entire section.
    :param section: the name of a config section.
    :param key: key to the value in that section.
    :param default: value if key is not defined in that section (defaults to empty string)
    :return: the value of the key, or the default value. If key is None then the entire config section.
    """
    try:
        section_data = CONF[section]
        if key is None:
            return section_data
        return section_data[key]
    except KeyError:
        if default == LookupError:
            if key is None:
                Console.error(f"### Config section '{section}' not found in {CONF_PATH}")
            else:
                Console.error(f"### Config key '{key}' not found in section '{section}' in {CONF_PATH}")
            system.exit(1)
        else:
            return default


ACCOUNT_ID = config("OAuth", "account_id", LookupError)
CLIENT_ID = config("OAuth", "client_id", LookupError)
CLIENT_SECRET = config("OAuth", "client_secret", LookupError)
APP_VERSION = "3.1 (Strategy)"
API_ENDPOINT_USER_LIST = "https://api.zoom.us/v2/users"
RECORDING_FILE_INCOMPLETE = "incomplete"


# ----------------------------------------------------
# Script Behaviour: download files or just size things up?
# ----------------------------------------------------
BEHAVIOUR_MODE_DOWNLOAD = "download"
BEHAVIOUR_MODE_SIZE = "size"
BEHAVIOUR_MODES = [BEHAVIOUR_MODE_DOWNLOAD, BEHAVIOUR_MODE_SIZE]
BEHAVIOUR_MODE = config("Behaviour", "mode", BEHAVIOUR_MODES[0])
if BEHAVIOUR_MODE not in BEHAVIOUR_MODES:
    Console.error(f"Unknown Behaviour mode: {BEHAVIOUR_MODE}")
    exit(1)
BEHAVIOUR_MODE_VERB = "Downloading" if BEHAVIOUR_MODE == BEHAVIOUR_MODES[0] else "Sizing"
# ----------------------------------------------------


RECORDING_START_YEAR = config("Recordings", "start_year", datetime.date.today().year)
RECORDING_START_MONTH = config("Recordings", "start_month", 1)
RECORDING_START_DAY = config("Recordings", "start_day", 1)
RECORDING_START_DATE = parser.parse(config("Recordings", "start_date", f"{RECORDING_START_YEAR}-{RECORDING_START_MONTH}-{RECORDING_START_DAY}"))
RECORDING_END_DATE = parser.parse(config("Recordings", "end_date", str(datetime.date.today())))
DOWNLOAD_DIRECTORY = config("Storage", "download_dir", 'downloads')


def load_access_token():
    """
    OAuth function, thanks to https://github.com/freelimiter
    """
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={ACCOUNT_ID}"

    client_cred = f"{CLIENT_ID}:{CLIENT_SECRET}"
    client_cred_base64_string = base64.b64encode(client_cred.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {client_cred_base64_string}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = json.loads(requests.request("POST", url, headers=headers).text)

    global ACCESS_TOKEN
    global AUTHORIZATION_HEADER

    try:
        ACCESS_TOKEN = response["access_token"]
        AUTHORIZATION_HEADER = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
    except KeyError:
        Console.error(f"### The key '{ACCESS_TOKEN}' wasn't found in the response.")
        # system.exit(1)


def get_users():
    """
    Loops through pages and return all users
    """
    response = requests.get(url=API_ENDPOINT_USER_LIST, headers=AUTHORIZATION_HEADER)

    if not response.ok:
        Console.error("### Could not retrieve users. Please check if your access token is still valid")
        Console.log(response)
        system.exit(1)

    page_data = response.json()
    total_pages = int(page_data["page_count"]) + 1

    all_users = []

    Console.log("Fetching user pages", end="", flush=True)
    for page in range(1, total_pages):
        url = f"{API_ENDPOINT_USER_LIST}?page_number={str(page)}"
        user_data = requests.get(url=url, headers=AUTHORIZATION_HEADER).json()
        users = ([
            (
                user["email"],
                user["id"],
                user["first_name"],
                user["last_name"]
            )
            for user in user_data["users"]
        ])

        all_users.extend(users)
        # QU Is the next line really redundant?
        #page += 1
        Console.log(".", end="", flush=True)
    Console.log()  # Move to the next line
    return all_users


def make_download_info_for(meeting: dict) -> list:
    if not meeting.get("recording_files"):
        raise Exception("No recording files specified")

    download_info = []
    for recording_file in meeting["recording_files"]:
        file_type = recording_file.get("file_type", "")
        if file_type == "":
            recording_type = RECORDING_FILE_INCOMPLETE
        elif file_type != "TIMELINE":
            recording_type = recording_file.get("recording_type")
        else:
            recording_type = recording_file.get("file_type")

        download_url = f"{recording_file['download_url']}?access_token={ACCESS_TOKEN}"
        download_info.append({
            "file_type": file_type,
            "file_extension": recording_file.get("file_extension"),
            "download_url": download_url,
            "recording_type": recording_type,
            "id": recording_file.get("id"),
            "file_size": recording_file.get("file_size")
        })
    return download_info


def per_delta(start, end, delta):
    curr = start
    while curr < end:
        yield curr, min(curr + delta, end)
        curr += delta


def get_meetings_for(user_id):
    recordings = []
    Console.log(f"Fetching data for meetings between {RECORDING_START_DATE:%Y-%m-%d} and {RECORDING_END_DATE:%Y-%m-%d}:",
                end="", flush=True)
    for start, end in per_delta(RECORDING_START_DATE, RECORDING_END_DATE, datetime.timedelta(days=30)):
        post_data = {
            "userId": user_id,
            "page_size": 300,
            "from": start.strftime('%Y-%m-%d'),
             "to": end.strftime('%Y-%m-%d')
        }
        response = requests.get(
            f"{API_ENDPOINT_USER_LIST}/{user_id}/recordings",
            headers=AUTHORIZATION_HEADER,
            params=post_data
        )
        recordings_data = response.json()
        recordings.extend(recordings_data.get("meetings", []))
        Console.log(".", end="", flush=True)
    Console.log() # Move to the next line
    return recordings


def download_recording(download_url, email, filename, folder_name, recording_size):
    dl_dir = os.path.join(DOWNLOAD_DIRECTORY, folder_name)
    sanitized_download_dir = path_validate.sanitize_filepath(dl_dir)
    sanitized_filename = path_validate.sanitize_filename(filename)
    full_filename = os.path.join(sanitized_download_dir, sanitized_filename)
    os.makedirs(sanitized_download_dir, exist_ok=True)

    Console.log(f"Destination file: {full_filename} ({recording_size} bytes)")

    # Check to see if we have already downloaded this file, and if it is complete.
    downloaded_file_size = 0
    if os.path.exists(full_filename):
        downloaded_file_size = os.path.getsize(full_filename)
        if downloaded_file_size == recording_size:
            Console.log(f"Recording file has already been downloaded successfully :)")
            return True

    # Download recording file.
    response = requests.get(download_url, stream=True)

    # total size in bytes.
    content_length = int(response.headers.get("content-length", 0))
    block_size = 32 * 1024  # 32 Kilobytes

    if content_length != recording_size:
        Console.warn(
            f"File content length ({content_length}) != reported size ({recording_size})"
        )

    # create TQDM progress bar
    # prog_bar = progress_bar.tqdm(total=content_length, unit="iB", unit_scale=True, desc=filename)
    prog_bar = progress_bar.tqdm(total=content_length, unit="iB", unit_scale=True)
    try:
        with open(full_filename, "wb") as fd:
            for chunk in response.iter_content(32 * 1024):
                prog_bar.update(len(chunk))
                fd.write(chunk)
        prog_bar.close()

        # Check that downloaded file is complete. e.g. Rate limiting may mean we did not actually get all of it.
        downloaded_file_size = os.path.getsize(full_filename)
        if downloaded_file_size != content_length:
            return False

        return True

    except Exception as e:
        Console.error(f"### Download failed for '{filename}': {e}")
        return False


def handle_graceful_shutdown(signal_received, frame):
    Console.info(f"\nSIGINT or CTRL-C detected. Exiting gracefully.")
    system.exit(0)


def validate_user_list(users: list):
    if not isinstance(users, list): raise TypeError("Expected 'users' to be a list.")
    for user in users:
        if not isinstance(user, tuple) or len(user) != 4: raise ValueError(f"Invalid user entry format: {user}")
        if not all(isinstance(item, str) for item in user): raise TypeError(
            f"All items in user tuple must be strings: {user}")


def load_strategy(strategy_config: dict, expected_interface: type) -> MeetingHelperStrategy:
    """
    Dynamically loads and instantiates a strategy class.
    Passes the strategy's own config block to its constructor.
    """
    module_name = strategy_config["module"]
    class_name = strategy_config["class"]
    init_params = strategy_config.get("config", {})

    try:
        strategy_module = importlib.import_module(module_name)
        strategy_class = getattr(strategy_module, class_name)
        if not issubclass(strategy_class, expected_interface):
            raise TypeError(f"Strategy class '{class_name}' does not implement '{expected_interface.__name__}'")
        # Pass the strategy-specific config to its constructor
        return strategy_class(init_params)
    except (ImportError, AttributeError, TypeError) as e:
        Console.error(f"Error loading strategy '{class_name}': {e}")
        raise



def main():
    # clear the screen buffer
    os.system('cls' if os.name == 'nt' else 'clear')

    splash_screen()

    # --- Prompt user to continue ---
    behaviour_message = f"Behaviour mode is '{BEHAVIOUR_MODE}': " + (
        "Meeting files will be downloaded."
        if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD
        else "Total size of download will be calculated."
    )
    Console.bold(behaviour_message)
    try:
        input("Press Enter to continue, or Ctrl+C to abort...")
    except KeyboardInterrupt:
        system.exit(0)

    Console.bold("Loading access token...")
    load_access_token()

    Console.bold("Getting user accounts...")
    users: list = get_users()
    validate_user_list(users)
    Console.log(f"Got {len(users)} user accounts.")

    # --- Load Strategy ---
    Console.bold("Loading Meeting Helper strategy.")
    strategy_conf = config("Strategy", default=LookupError)
    # Load one strategy object that handles all helper logic
    meeting_helper = load_strategy(strategy_conf, MeetingHelperStrategy)
    Console.log(f"Using strategy class: {strategy_conf['class']}")

    Console.dark_cyan("\n>>> Processing list of users in this organisation:", True, True)
    total_bytes = 0

    # for email, user_id, first_name, last_name in users:
    for index_users, (email, user_id, first_name, last_name) in enumerate(users, start=1):
        # --- Use the strategy for filtering users ---
        if meeting_helper.should_ignore_user(email):
            Console.warn(f"\n>>>> Ignoring meetings for user {index_users}/{len(users)}: {first_name} {last_name} ({email})", True)
            continue

        Console.blue(f"\n>>>> Getting meetings for user {index_users}/{len(users)}: {first_name} {last_name} ({email})", True, True)
        meetings: list = get_meetings_for(user_id)
        Console.log(f"Found {len(meetings)} meeting(s)")

        for index_meetings, meeting in enumerate(meetings, start=1):
            meeting_topic = meeting.get("topic", "No Topic")
            meeting_time = meeting.get("start_time", "Unknown Time")
            Console.bold(f"\n{first_name} {last_name} ({email}) meeting {index_meetings}/{len(meetings)}: {meeting_topic} ({meeting_time})")

            # --- Use the strategy for filtering meetings ---
            if meeting_helper.should_ignore_meeting(meeting):
                Console.warn("Ignoring meeting!")
                continue

            try:
                meeting_download_info = make_download_info_for(meeting)
                Console.log(f"Found {len(meeting_download_info)} files for this meeting.")
            except Exception as e:
                Console.error(f"### Could not get recording files: {e}")
                continue

            for file_number, recording_file in enumerate(meeting_download_info, 1):
                if recording_file["recording_type"] == RECORDING_FILE_INCOMPLETE:
                    Console.warn(f"### Recording file is incomplete, skipping.")
                    continue

                # --- Use the strategy for naming files and folders ---
                folder_name, filename = meeting_helper.format_filename(meeting, recording_file)

                Console.log(
                    f"==> {BEHAVIOUR_MODE_VERB} file {file_number}/{len(meeting_download_info)}: type={recording_file['recording_type']}, dest={filename}")

                if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD:
                    download_recording(recording_file["download_url"],
                                       email,
                                       filename,
                                       folder_name,
                                       recording_file["file_size"])
                else:
                    total_bytes += recording_file["file_size"]

    Console.green(f"\n{'*' * 24 } All done! {'*' * 24 }", True, False)
    if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD:
        Console.blue(f"\nRecordings saved to: {os.path.abspath(DOWNLOAD_DIRECTORY)}\n")
    else:
        total_gb = total_bytes / (1024 * 1024 * 1024)
        Console.blue(f"\nTotal disk space required to download these meetings is: {total_gb:.2f} GB", True)


def splash_screen():
    Console.dark_cyan(f"""
                             ,*****************.
                          *************************
                        *****************************
                      *********************************
                     ******               ******* ******
                    *******                .**    ******
                    *******                       ******/
                    *******                       /******
                    ///////                 //    //////
                    ///////*              ./////.//////
                     ////////////////////////////////*
                       /////////////////////////////
                          /////////////////////////
                             ,/////////////////

                        Zoom Recording Downloader

                            Version {APP_VERSION}
        
    """, True)


if __name__ == "__main__":
    # tell Python to shut down gracefully when SIGINT is received
    signal.signal(signal.SIGINT, handle_graceful_shutdown)

    main()
