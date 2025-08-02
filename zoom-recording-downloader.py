#!/usr/bin/env python3

# TODO Check that README.md is accurate in terms of defaults.
# TODO Test that all the defaults work ok if minimal config is given.

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
import fnmatch
import json
import os
import shutil
import signal
import sys as system
import re as regex
from datetime import timezone
from zoneinfo import ZoneInfo


# installed libraries
import dateutil.parser as parser
import pathvalidate as path_validate
import requests
import tqdm as progress_bar

# Local imports
from lib.console import Console
from meeting_metadata import MetadataDB

# ----------------------------------------------------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------------------------------------------------
APP_VERSION = "4.0 (KSP)"
API_ENDPOINT_USER_LIST = "https://api.zoom.us/v2/users"
RECORDING_FILE_INCOMPLETE = "incomplete"
# ----------------------------------------------------
# Load the configuration file
# The configuration file is expected to be in the same directory as this script.
CONF_PATH = "zoom-recording-downloader.conf"
with open(CONF_PATH, encoding="utf-8-sig") as json_file:
    CONF = json.loads(json_file.read())


def config(section:str, key:str, default:str):
    try:
        return CONF[section][key]
    except KeyError:
        if default == LookupError:
            raise LookupError(f"No value found for [{section}][{key}] in {CONF_PATH}")
        else:
            return default


# ----------------------------------------------------
# OAuth
# ----------------------------------------------------
# These values are required for OAuth authentication with Zoom API.
SECTION_KEY_OAUTH = "OAuth"
ACCOUNT_ID = config(section=SECTION_KEY_OAUTH, key="account_id", default=LookupError)
CLIENT_ID = config(section=SECTION_KEY_OAUTH, key="client_id", default=LookupError)
CLIENT_SECRET = config(section=SECTION_KEY_OAUTH, key="client_secret", default=LookupError)
# ----------------------------------------------------
# Storage
# ----------------------------------------------------
DOWNLOAD_DIRECTORY = config("Storage", "download_dir", 'downloads')
# ----------------------------------------------------
# DateInterval - time period for which to download recordings
# ----------------------------------------------------
SECTION_KEY_DATE_INTERVAL = "DateInterval"
START_YEAR = config(section=SECTION_KEY_DATE_INTERVAL, key="start_year", default=datetime.date.today().year)
START_MONTH = config(section=SECTION_KEY_DATE_INTERVAL, key="start_month", default=1)
START_DAY = config(section=SECTION_KEY_DATE_INTERVAL, key="start_day", default=1)
START_DATE = parser.parse(config(section=SECTION_KEY_DATE_INTERVAL, key="start_date", default=f"{START_YEAR}-{START_MONTH}-{START_DAY}"))
END_DATE = parser.parse(config(section=SECTION_KEY_DATE_INTERVAL, key="end_date", default=str(datetime.date.today())))
# ----------------------------------------------------
# Behaviour: download files or just size things up?
# ----------------------------------------------------
BEHAVIOUR_MODE_DOWNLOAD = "download"
BEHAVIOUR_MODE_SIZE = "size"
BEHAVIOUR_MODES = [BEHAVIOUR_MODE_DOWNLOAD, BEHAVIOUR_MODE_SIZE]
BEHAVIOUR_MODE = config("Behaviour", "mode", BEHAVIOUR_MODE_DOWNLOAD)
if BEHAVIOUR_MODE not in BEHAVIOUR_MODES:
    Console.error(f"Unknown Behaviour mode: {BEHAVIOUR_MODE}")
    exit(1)
BEHAVIOUR_MODE_VERB = "Downloading" if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD else "Sizing"
# ----------------------------------------------------
# UserFilter: filtering users based on email addresses
# ----------------------------------------------------
USER_FILTER_INCLUDE = config("UserFilter", "emails_to_include", [])
USER_FILTER_EXCLUDE = config("UserFilter", "emails_to_exclude", [])
# ----------------------------------------------------
# MeetingFilter: filtering meetings based on topics
# ----------------------------------------------------
MEETING_FILTER_INCLUDE = config("MeetingFilter", "topics_to_include", [])
MEETING_FILTER_EXCLUDE = config("MeetingFilter", "topics_to_exclude", [])
# ----------------------------------------------------
# FilepathFormat: formatting the file and folder names
# ----------------------------------------------------
SECTION_KEY_FF = "FilepathFormat"
DEFAULT_TIMEZONE = "UTC"
DEFAULT_TIME_FORMAT = "%Y.%m.%d - %I.%M %p"
DEFAULT_FILENAME_FORMAT = "{meeting_time} - {topic} - {rec_type} - {recording_id}.{file_extension}"
DEFAULT_FOLDERNAME_FORMAT = "{topic} - {meeting_time}"
#
MEETING_TIMEZONE = ZoneInfo(config(section=SECTION_KEY_FF, key="timezone", default=DEFAULT_TIMEZONE))
MEETING_STRFTIME = config(section=SECTION_KEY_FF, key="strftime", default=f'%Y.%m.%d - %I.%M %p {DEFAULT_TIMEZONE}')
MEETING_FILENAME_FORMAT = config(section=SECTION_KEY_FF, key="filename", default=DEFAULT_FILENAME_FORMAT)
MEETING_FOLDERNAME_FORMAT = config(section=SECTION_KEY_FF, key="folder", default=DEFAULT_FOLDERNAME_FORMAT)
MEETING_FILEPATH_REPLACE_OLD = config(section=SECTION_KEY_FF, key="filepath_replace_old", default="")
MEETING_FILEPATH_REPLACE_NEW = config(section=SECTION_KEY_FF, key="filepath_replace_old", default="")

# ----------------------------------------------------
# Metadata
# ----------------------------------------------------
ksp_metadata = MetadataDB("ksp/metadata/David_Wood_Zoom_Recordings-2022-11-06--2024-11-07 - Meetings.csv")


# ----------------------------------------------------------------------------------------------------------------------
# Code
# ----------------------------------------------------------------------------------------------------------------------
# Load the access token for OAuth authentication with Zoom API
ACCESS_TOKEN = None
AUTHORIZATION_HEADER = None


def load_access_token():
    """
    OAuth function, thanks to https://github.com/freelimiter
    """
    Console.bold("Loading access token...")

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
        system.exit(1)
    Console.log(f"Access token loaded successfully.")

def handle_graceful_shutdown(signal_received, frame):
    Console.info(f"\nSIGINT or CTRL-C detected. Exiting gracefully.")
    system.exit(0)


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

    Console.bold("Getting user accounts")
    Console.log("Fetching data.", end="", flush=True)
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

    Console.log(f"Got {len(all_users)} user accounts.")

    return all_users


def make_download_info_for(meeting: dict) -> list:
    recording_files = meeting.get("recording_files")

    if not recording_files:
        return None

    download_info = []
    for recording_file in recording_files:
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
    Console.log(f"Fetching data for meetings between {START_DATE:%Y-%m-%d} and {END_DATE:%Y-%m-%d}",
                end="", flush=True)
    for start, end in per_delta(START_DATE, END_DATE, datetime.timedelta(days=30)):
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


def download_meeting_file(download_url, filename, folder_name, recording_size):
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
            Console.green(f"Meeting file has already been downloaded successfully :)")
            return True

    # Download recording file.
    response = requests.get(download_url, stream=True)
    if not response.ok:
        Console.error(f"Download failed with status code: {response.status_code}")
        if response.status_code == 401:
            Console.error("Unauthorized: Check your access token.")
        elif response.status_code == 404:
            Console.error("File not found: The URL may be incorrect.")
        else:
            Console.error(f"Unexpected error: {response.reason}")
        return False
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


def should_ignore_user(email: str) -> bool:
    if USER_FILTER_INCLUDE and not any(fnmatch.fnmatch(email, pattern) for pattern in USER_FILTER_INCLUDE):
        # Ignore if filter is defined and user email does not match any patterns
        return True
    # Exclude cannot override include, so we check it last
    if USER_FILTER_EXCLUDE and any(fnmatch.fnmatch(email, pattern) for pattern in USER_FILTER_EXCLUDE):
        # Ignore if filter is defined and email matches any of the patterns
        return True
    # Default response is to NOT ignore the user
    return False


def should_ignore_meeting(meeting: dict) -> bool:
    meeting_topic = meeting.get("topic")
    if MEETING_FILTER_INCLUDE and not any(fnmatch.fnmatch(meeting_topic, pattern) for pattern in MEETING_FILTER_INCLUDE):
        # Ignore if filter is defined and topic does not match any patterns
        return True
    # Exclude cannot override include, so we check it last
    if MEETING_FILTER_EXCLUDE and any(fnmatch.fnmatch(meeting_topic, pattern) for pattern in MEETING_FILTER_EXCLUDE):
        # Ignore if filter is defined and topic matches any patterns
        return True
    # Default response is to NOT ignore the meeting
    return False


def format_filename(meeting: dict, recording_file: dict) -> (str, str):
    file_extension = recording_file["file_extension"].lower()
    recording_id = recording_file["id"]
    recording_type = recording_file["recording_type"]

    invalid_chars_pattern = r'[<>:"/\\|?*\x00-\x1F]'
    topic = regex.sub(invalid_chars_pattern, '', meeting["topic"])
    rec_type = recording_type.replace("_", " ").title()
    meeting_time_utc = parser.parse(meeting["start_time"]).replace(tzinfo=timezone.utc)
    meeting_time_local = meeting_time_utc.astimezone(MEETING_TIMEZONE)
    year = meeting_time_local.strftime("%Y")
    month = meeting_time_local.strftime("%m")
    day = meeting_time_local.strftime("%d")
    meeting_time = meeting_time_local.strftime(MEETING_STRFTIME)

    filename = MEETING_FILENAME_FORMAT.format(**locals())
    filename = filename.replace(
        MEETING_FILEPATH_REPLACE_OLD,
        MEETING_FILEPATH_REPLACE_NEW
    )

    folder_name = MEETING_FOLDERNAME_FORMAT.format(**locals())
    folder_name = folder_name.replace(
        MEETING_FILEPATH_REPLACE_OLD,
        MEETING_FILEPATH_REPLACE_NEW
    )

    return folder_name, filename

# ----------------------------------------------------------------------------------------------------------------------
# Alternate Strategies for filtering meetings and formatting file names
# ----------------------------------------------------------------------------------------------------------------------
def should_ignore_meeting_alternate_strategy(meeting: dict) -> bool:
    return ksp_metadata.should_ignore_meeting(zoom_meeting_data=meeting)


def format_filename_alternate_strategy(meeting: dict, recording_file: dict) -> (str, str):

    # Use locally defined variables to format the filename and folder name -- **locals() is used to replace the variables
    # of the same name in the MEETING_FILENAME_FORMAT and MEETING_FOLDERNAME_FORMAT strings.
    # This allows for easy customization of the filename and folder name formats.
    # The variables are defined above, and the format strings are defined in the configuration file.
    # This is a good way to keep the code clean and maintainable.
    # It also allows for easy customization of the filename and folder name formats.
    # If you want to change the format of the filename or folder name, you can do so in the configuration file
    # without having to change the code here too much.

    # Previous variables used in the format strings
    file_extension = recording_file["file_extension"].lower()
    recording_id = recording_file["id"]
    recording_type = recording_file["recording_type"]

    invalid_chars_pattern = r'[<>:"/\\|?*\x00-\x1F]'

    topic = regex.sub(invalid_chars_pattern, '', meeting["topic"])
    rec_type = recording_type.replace("_", " ").title()
    meeting_time_utc = parser.parse(meeting["start_time"]).replace(tzinfo=timezone.utc)
    meeting_time_local = meeting_time_utc.astimezone(MEETING_TIMEZONE)
    year = meeting_time_local.strftime("%Y")
    month = meeting_time_local.strftime("%m")
    day = meeting_time_local.strftime("%d")
    meeting_time = meeting_time_local.strftime(MEETING_STRFTIME)

    # ------------------------------------------------------------------------------------------------------------------
    # New variables used in the format strings from ksp meeting metadata
    #  FIXME check for nan indicating that the metadata is not available for this meeting.
    # ------------------------------------------------------------------------------------------------------------------
    metadata_for_this_meeting:MetadataDB.Row = ksp_metadata.find_csv_metadata_for(zoom_meeting_data=meeting)
    # Custom variables for the format strings
    language = metadata_for_this_meeting.language
    author = metadata_for_this_meeting.author
    book_id = metadata_for_this_meeting.book_id
    book_title = metadata_for_this_meeting.book_title
    chapters = metadata_for_this_meeting.chapters
    group_id = metadata_for_this_meeting.group_id
    start_time = meeting_time

    # ------------------------------------------------------------------------------------------------------------------
    # Format the filename and folder name replacing variables in the format strings with the values of the same name
    # ------------------------------------------------------------------------------------------------------------------
    filename = MEETING_FILENAME_FORMAT.format(**locals())

    filename = filename.replace(
        MEETING_FILEPATH_REPLACE_OLD,
        MEETING_FILEPATH_REPLACE_NEW
    )

    folder_name = MEETING_FOLDERNAME_FORMAT.format(**locals())
    folder_name = folder_name.replace(
        MEETING_FILEPATH_REPLACE_OLD,
        MEETING_FILEPATH_REPLACE_NEW
    )

    return folder_name, filename


# ----------------------------------------------------------------------------------------------------------------------
# Main function
# ----------------------------------------------------------------------------------------------------------------------
def main():
    # clear the screen buffer
    os.system('cls' if os.name == 'nt' else 'clear')

    splash_screen()

    # --- Prompt user to continue ---
    behaviour_message = f"Behaviour mode is '{BEHAVIOUR_MODE}': " + (
        f"Meeting files will be downloaded to: {DOWNLOAD_DIRECTORY}"
        if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD
        else "Total size of download will be calculated."
    )
    Console.bold(behaviour_message)
    try:
        input("Press Enter to continue, or Ctrl+C to abort? ")
    except KeyboardInterrupt:
        system.exit(0)

    load_access_token()

    users: list = get_users()

    Console.dark_cyan("\n>>> Processing list of users in this organisation:", True, True)
    total_bytes = 0

    # for email, user_id, first_name, last_name in users:
    for index_users, (email, user_id, first_name, last_name) in enumerate(users, start=1):
        # --- Use the strategy for filtering users ---
        if should_ignore_user(email):
            Console.warn(f"\n>>>> Ignoring meetings for user {index_users}/{len(users)}: {first_name} {last_name} ({email})", True)
            continue

        Console.blue(f"\n>>>> Getting meetings for user {index_users}/{len(users)}: {first_name} {last_name} ({email})", True, True)
        meetings: list = get_meetings_for(user_id)
        Console.log(f"Found {len(meetings)} meeting(s) for this user in this period.")

        # TODO check that we have enough disk space to download the recordings, report, and proceed if we do.
        for index_meetings, meeting in enumerate(meetings, start=1):
            meeting_topic = meeting.get("topic", "No Topic")
            meeting_time = meeting.get("start_time", "Unknown Time")
            Console.bold(f"\n{first_name} {last_name} ({email}) meeting {index_meetings}/{len(meetings)}: {meeting_topic} ({meeting_time})")

            # --- TODO switch strategy for filtering meetings ---
            if should_ignore_meeting_alternate_strategy(meeting):
                Console.warn("Ignoring meeting!")
                continue

            meeting_download_info = make_download_info_for(meeting)

            if not meeting_download_info:
                Console.warn("No recording files found for this meeting. Skipping.")
                continue

            num_files_to_download = len(meeting_download_info)
            num_files_downloaded = 0
            Console.log(f"Found {num_files_to_download} file(s) for this meeting.")

            for file_number, recording_file in enumerate(meeting_download_info, 1):
                if recording_file["recording_type"] == RECORDING_FILE_INCOMPLETE:
                    Console.warn(f"### Recording file is incomplete, skipping.")
                    continue

                # --- ToDO switch strategy for naming files and folders ---
                folder_name, filename = format_filename_alternate_strategy(meeting=meeting, recording_file=recording_file)

                Console.log(
                    f"==> {BEHAVIOUR_MODE_VERB} file {file_number}/{len(meeting_download_info)}: type={recording_file['recording_type']}, dest={filename}")

                if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD:
                    if download_meeting_file(download_url=recording_file["download_url"],
                                          filename=filename,
                                          folder_name=folder_name,
                                          recording_size=recording_file["file_size"]):
                        Console.green(f"File {file_number}/{num_files_to_download} has been downloaded.")
                        num_files_downloaded += 1
                        if num_files_to_download == num_files_downloaded:
                            Console.green(f"All files for this meeting have been downloaded.", bold=True,
                                          underline=False)
                            # FIXME remove this custom insertion - non-default behaviour
                            ksp_metadata.mark_as_downloaded(meeting)
                    else:
                        Console.error(f"File {file_number}/{num_files_to_download} could not be downloaded.")
                        # TODO? ksp_metadata.mark_as_failed(meeting)
                else:
                    total_bytes += recording_file["file_size"]


    Console.green(f"\n{'*' * 24 } All done! {'*' * 24 }", True, False)
    if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD:
        Console.blue(f"\nRecordings saved to: {os.path.abspath(DOWNLOAD_DIRECTORY)}\n")
        ksp_metadata.save()
    else:
        total_gb = total_bytes / (1024 * 1024 * 1024)
        usage = shutil.disk_usage(DOWNLOAD_DIRECTORY)
        disk_space_gb = usage.free / (1024 * 1024 * 1024)
        Console.blue("\nDisk Space Report:", True, True)
        Console.blue(f"Required : {total_gb:.2f} GB\nAvailable: {disk_space_gb:.2f} GB", True)

        if total_gb >= disk_space_gb:
            Console.error(f"Not enough disk space!")
            system.exit(1)
        Console.green("You have enough disk space to download the recordings.")



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
