#!/usr/bin/env python3

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
import re as regex
import signal
import sys as system
from zoneinfo import ZoneInfo

# installed libraries
import dateutil.parser as parser
import pathvalidate as path_validate
import requests
import tqdm as progress_bar


class Color:
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARK_CYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"

CONF_PATH = "zoom-recording-downloader.conf"
with open(CONF_PATH, encoding="utf-8-sig") as json_file:
    CONF = json.loads(json_file.read())

def config(section, key, default=''):
    try:
        return CONF[section][key]
    except KeyError:
        if default == LookupError:
            print(f"{Color.RED}### No value provided for {section}:{key} in {CONF_PATH}")
            system.exit(1)
        else:
            return default

ACCOUNT_ID = config("OAuth", "account_id", LookupError)
CLIENT_ID = config("OAuth", "client_id", LookupError)
CLIENT_SECRET = config("OAuth", "client_secret", LookupError)

APP_VERSION = "3.1 (OAuth)"

API_ENDPOINT_USER_LIST = "https://api.zoom.us/v2/users"

INCLUDE_EMAILS = config("Include", "emails")
INCLUDE_TOPICS = config("Include", "topics")
EXCLUDE_EMAILS = config("Exclude", "emails")
EXCLUDE_TOPICS = config("Exclude", "topics")

RECORDING_FILE_INCOMPLETE = "incomplete"
BEHAVIOUR_MODES = ["download", "size"]
BEHAVIOUR_MODE = config("Behaviour", "mode", BEHAVIOUR_MODES[0])
if BEHAVIOUR_MODE not in BEHAVIOUR_MODES:
    print(f"{Color.RED}Unknown Behaviour mode: {BEHAVIOUR_MODE}")
    exit(1)
BEHAVIOUR_MODE_ACTION = "Downloading" if BEHAVIOUR_MODE == BEHAVIOUR_MODES[0] else "Sizing"

RECORDING_START_YEAR = config("Recordings", "start_year", datetime.date.today().year)
RECORDING_START_MONTH = config("Recordings", "start_month", 1)
RECORDING_START_DAY = config("Recordings", "start_day", 1)
RECORDING_START_DATE = parser.parse(config("Recordings", "start_date", f"{RECORDING_START_YEAR}-{RECORDING_START_MONTH}-{RECORDING_START_DAY}"))
RECORDING_END_DATE = parser.parse(config("Recordings", "end_date", str(datetime.date.today())))
DOWNLOAD_DIRECTORY = config("Storage", "download_dir", 'downloads')

MEETING_TIMEZONE = ZoneInfo(config("Recordings", "timezone", 'UTC'))
MEETING_STRFTIME = config("Recordings", "strftime", '%Y.%m.%d - %I.%M %p UTC')
MEETING_FILENAME = config("Recordings", "filename", '{meeting_time} - {topic} - {rec_type} - {recording_id}.{file_extension}')
MEETING_FOLDER = config("Recordings", "folder", '{topic} - {meeting_time}')


def load_access_token():
    """ OAuth function, thanks to https://github.com/freelimiter
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
        print(f"{Color.RED}### The key 'access_token' wasn't found.{Color.END}")


def get_users():
    """ loop through pages and return all users
    """
    response = requests.get(url=API_ENDPOINT_USER_LIST, headers=AUTHORIZATION_HEADER)

    if not response.ok:
        print(response)
        print(
            f"{Color.RED}### Could not retrieve users. Please make sure that your access "
            f"token is still valid{Color.END}"
        )

        system.exit(1)

    page_data = response.json()
    total_pages = int(page_data["page_count"]) + 1

    all_users = []

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
        page += 1

    return all_users


def format_filename(params):
    file_extension = params["file_extension"].lower()
    recording = params["recording"]
    recording_id = params["recording_id"]
    recording_type = params["recording_type"]

    invalid_chars_pattern = r'[<>:"/\\|?*\x00-\x1F]'
    topic = regex.sub(invalid_chars_pattern, '', recording["topic"])
    rec_type = recording_type.replace("_", " ").title()
    meeting_time_utc = parser.parse(recording["start_time"]).replace(tzinfo=datetime.timezone.utc)
    meeting_time_local = meeting_time_utc.astimezone(MEETING_TIMEZONE)
    year = meeting_time_local.strftime("%Y")
    month = meeting_time_local.strftime("%m")
    day = meeting_time_local.strftime("%d")
    meeting_time = meeting_time_local.strftime(MEETING_STRFTIME)

    filename = MEETING_FILENAME.format(**locals()).replace(" ", "-")
    folder = MEETING_FOLDER.format(**locals()).replace(" ", "-")
    return (filename, folder)


def make_download_info_for(meeting):
    if not meeting.get("recording_files"):
        raise Exception("No recording files specified")

    download_info = []
    for recording_file in meeting["recording_files"]:
        file_type = recording_file["file_type"]
        file_extension = recording_file["file_extension"]
        recording_id = recording_file["id"]
        recording_size = recording_file["file_size"]

        if file_type == "":
            recording_type = RECORDING_FILE_INCOMPLETE
        elif file_type != "TIMELINE":
            recording_type = recording_file["recording_type"]
        else:
            recording_type = recording_file["file_type"]

        # must append access token to download_url
        download_url = f"{recording_file['download_url']}?access_token={ACCESS_TOKEN}"
        download_info.append((file_type, file_extension, download_url, recording_type, recording_id, recording_size))

    return download_info


def make_postdata_for_recordings(email, page_size, rec_start_date, rec_end_date):
    return {
        "userId": email,
        "page_size": page_size,
        "from": rec_start_date,
        "to": rec_end_date
    }


def per_delta(start, end, delta):
    """ Generator used to create deltas for recording start and end dates
    """
    curr = start
    while curr < end:
        yield curr, min(curr + delta, end)
        curr += delta


def get_meetings_for(user_id):
    """ Start date now split into YEAR, MONTH, and DAY variables (Within 6 month range)
        then get recordings within that range
    """
    recordings = []

    for start, end in per_delta(
        RECORDING_START_DATE,
        RECORDING_END_DATE,
        datetime.timedelta(days=30)
    ):
        post_data = make_postdata_for_recordings(user_id, 300, start, end)
        response = requests.get(
            url=f"https://api.zoom.us/v2/users/{user_id}/recordings",
            headers=AUTHORIZATION_HEADER,
            params=post_data
        )
        recordings_data = response.json()
        recordings.extend(recordings_data["meetings"])

    return recordings


def download_recording(download_url, email, filename, folder_name, recording_size):
    dl_dir = os.sep.join([DOWNLOAD_DIRECTORY, folder_name])
    sanitized_download_dir = path_validate.sanitize_filepath(dl_dir)
    sanitized_filename = path_validate.sanitize_filename(filename)
    full_filename = os.sep.join([sanitized_download_dir, sanitized_filename])

    os.makedirs(sanitized_download_dir, exist_ok=True)

    print(f"Destination file: {full_filename} ({recording_size} bytes)")

    # Check to see if we have already downloaded this file, and if it is complete.
    downloaded_file_size = 0
    if os.path.exists(full_filename):
        # print(f"Download file already exists: {full_filename}")
        downloaded_file_size = os.path.getsize(full_filename)
        if downloaded_file_size == recording_size:
            print(f"Recording file has already been downloaded successfully :)")
            return True

    # Download recording file.
    response = requests.get(download_url, stream=True)

    # total size in bytes.
    content_length = int(response.headers.get("content-length", 0))
    block_size = 32 * 1024  # 32 Kibibytes

    if content_length != recording_size:
        print(f"{Color.RED}Content length of recording file ({content_length}) != reported size ({recording_size}){Color.END}")

    # create TQDM progress bar
    prog_bar = progress_bar.tqdm(total=content_length, unit="iB", unit_scale=True)
    try:
        with open(full_filename, "wb") as fd:
            for chunk in response.iter_content(block_size):
                prog_bar.update(len(chunk))
                fd.write(chunk)  # write video chunk to disk
        prog_bar.close()

        # Check that downloaded file is complete. e.g. Rate limiting may mean we did not actually get all of it.
        downloaded_file_size = os.path.getsize(full_filename)
        if downloaded_file_size != content_length:
            return False

        return True

    except Exception as e:
        print(
            f"{Color.RED}### The video recording with filename '{filename}' for user with email "
            f"'{email}' could not be downloaded because {Color.END}'{e}'"
        )

        return False


def handle_graceful_shutdown(signal_received, frame):
    print(f"\n{Color.DARK_CYAN}SIGINT or CTRL-C detected. system.exiting gracefully.{Color.END}")

    system.exit(0)


def should_include_user(email):
    if len(INCLUDE_EMAILS) == 0:
        # Default == include all
        return True
    for pattern in INCLUDE_EMAILS:
        if fnmatch.fnmatch(email, pattern):
            return True
    return False

def should_exclude_user(email):
    if len(EXCLUDE_EMAILS) == 0:
        # Default == exclude none
        return False
    for pattern in EXCLUDE_EMAILS:
        if fnmatch.fnmatch(email, pattern):
            return True
    return False

def should_include_topic(topic):
    if len(INCLUDE_TOPICS) == 0:
        # Default = include all
        return True
    for pattern in INCLUDE_TOPICS:
        if fnmatch.fnmatch(topic, pattern):
            return True
    return False

def should_exclude_topic(topic):
    if len(EXCLUDE_TOPICS) == 0:
        # Default == exclude none
        return False
    for pattern in EXCLUDE_TOPICS:
        if fnmatch.fnmatch(topic, pattern):
            return True
    return False


# ######################################################################################################################
# #                                             MAIN                                                                   #
# ######################################################################################################################
def main():
    # clear the screen buffer
    os.system('cls' if os.name == 'nt' else 'clear')

    splash_screen()

    print(f"{Color.BOLD}Loading access token...{Color.END}")
    load_access_token()

    print(f"{Color.BOLD}Getting user accounts...{Color.END}")
    users = get_users()
    print(f"Got {len(users)} user accounts: {users}" )

    total_bytes = 0
    # Iterate over users
    for email, user_id, first_name, last_name in users:
        user_info = (
            f"{first_name} {last_name} - {email}" if first_name and last_name else f"{email}"
        )

        if not should_include_user(email):
            print(f"{Color.BOLD}==> User is NOT included:{Color.END} {user_info}")
            continue
        if should_exclude_user(email):
            print(f"{Color.BOLD}==> User is excluded:{Color.END} {user_info}")
            continue

        print(f"\n{Color.BOLD}{Color.DARK_CYAN}================================================================{Color.END}")
        print(f"{Color.BOLD}{Color.DARK_CYAN}Getting list of meetings for [{user_info}]...{Color.END}")
        print(f"{Color.BOLD}{Color.DARK_CYAN}================================================================{Color.END}")
        meetings = get_meetings_for(user_id)
        total_meetings = len(meetings)
        print(f"Found {total_meetings} meeting(s)")

        # Iterate over user's meetings
        for index, meeting in enumerate(meetings):
            success = False
            meeting_id = meeting["uuid"]
            meeting_topic = meeting.get("topic")
            meeting_time = meeting.get("start_time")

            if not should_include_topic(meeting_topic):
                print(f"{Color.BOLD}==> Meeting is NOT included:{Color.END} {meeting_topic}")
                continue
            if should_exclude_topic(meeting_topic):
                print(f"{Color.BOLD}==> Meeting is excluded:{Color.END} {meeting_topic}")
                continue

            print(f"{Color.BOLD}{BEHAVIOUR_MODE_ACTION} recordings for meeting ({index+1} of {total_meetings}): {meeting_topic} ({meeting_time}){Color.END}")
            try:
                meeting_download_info = make_download_info_for(meeting)
            except Exception:
                print(
                    f"{Color.RED}### Recording files missing for meeting: {Color.END}"
                    f"-- Topic: {meeting_topic}\n"
                    f"-- ID: {meeting_id}'\n"
                )
                continue

            num_files = len(meeting_download_info)
            print(f"Found {num_files} files for this meeting.")

            # Get the recording files for this meeting.
            file_number = 0
            for file_type, file_extension, download_url, recording_type, recording_id, recording_size in meeting_download_info:
                file_number += 1
                # Is the recording file online complete or not? Should we download it yet?
                if recording_type != RECORDING_FILE_INCOMPLETE:
                    filename, folder_name = (
                        format_filename({
                            "file_type": file_type,
                            "recording": meeting,
                            "file_extension": file_extension,
                            "recording_type": recording_type,
                            "recording_id": recording_id
                        })
                    )

                    # truncate URL to 64 characters
                    truncated_url = download_url[0:64] + "..."
                    print(
                        f"==> {BEHAVIOUR_MODE_ACTION} file ({file_number} of {num_files}) as '{recording_type}':"
                    )
                    if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD:
                        success |= download_recording(download_url, email, filename, folder_name, recording_size)
                    else:
                        total_bytes += recording_size
                        success = True
                else:
                    print(
                        f"{Color.RED}### Recording file is incomplete!{Color.END}"
                    )
                    success = False

            if not success:
                # if successful and it has not already been logged, write the ID of this recording to the completed file
                f"{Color.RED}### Recording download failed for some reason.{Color.END}"

                # if meeting_id not in COMPLETED_MEETING_IDS:
                #     with open(COMPLETED_MEETING_IDS_LOG, 'a') as log:
                #         COMPLETED_MEETING_IDS.add(meeting_id)
                #         log.write(meeting_id)
                #         log.write('\n')
                #         log.flush()

    print(f"\n{Color.BOLD}{Color.GREEN}*** All done! ***{Color.END}")
    if BEHAVIOUR_MODE == BEHAVIOUR_MODE_DOWNLOAD:
        save_location = os.path.abspath(DOWNLOAD_DIRECTORY)
        print(
            f"\n{Color.BLUE}Recordings have been saved to: {Color.UNDERLINE}{save_location}"
            f"{Color.END}\n"
        )
    else:
        total_mb = total_bytes / (1024*1024)
        total_gb = total_mb / 1024
        print(
            f"\n{Color.BLUE}Total size of the recordings that could be downloaded:{Color.END}\n"
            f"{total_gb} GB\n"
            f"{total_mb} MB\n"
            f"{total_bytes} bytes"
        )


def splash_screen():
    print(f"""
        {Color.DARK_CYAN}
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
        {Color.END}
    """)


if __name__ == "__main__":
    # tell Python to shutdown gracefully when SIGINT is received
    signal.signal(signal.SIGINT, handle_graceful_shutdown)

    main()
