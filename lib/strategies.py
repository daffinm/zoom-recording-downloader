import fnmatch
import re as regex
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import dateutil.parser as parser


class MeetingHelperStrategy(ABC):
    """
    An interface defining helper logic methods where different strategies are (or may be) needed.
    """

    @abstractmethod
    def should_ignore_user(self, email: str) -> bool:
        """
        Determines if a user should be ignored based on their email.
        Returns True if they should be ignored, False otherwise.
        """
        pass

    @abstractmethod
    def should_ignore_meeting(self, meeting: dict) -> bool:
        """
        Determines if a meeting should be ignored based on its details.
        Returns True if it should be ignored, False otherwise.
        """
        pass

    @abstractmethod
    def format_filename(self, meeting: dict, recording_file: dict) -> (str, str):
        """
        Generates the folder name and filename for a given meeting file (audio, video, transcript etc.).
        Returns a tuple of (folder_name, filename).
        """
        pass


class DefaultMeetingHelperStrategy(MeetingHelperStrategy):
    """
    The default strategy that uses the main .conf file for its logic.
    """
    # Filtering users and topics
    SECTION_KEY_INCLUDE = "Include"
    SECTION_KEY_EXCLUDE = "Exclude"
    KEY_EMAILS = "emails"
    KEY_TOPICS = "topics"
    # File and folder naming
    SECTION_KEY_FF = "FilepathFormat"
    KEY_TIMEZONE = "timezone"
    KEY_TIME_FORMAT = "strftime"
    KEY_FILENAME_FORMAT = "filename"
    KEY_FOLDERNAME_FORMAT = "folder"
    KEY_FILEPATH_REPLACE_OLD = "filepath_replace_old"
    KEY_FILEPATH_REPLACE_NEW = "filepath_replace_new"
    DEFAULT_TIMEZONE = "UTC"
    DEFAULT_TIME_FORMAT = "%Y.%m.%d - %I.%M %p"
    DEFAULT_FILENAME_FORMAT = "{meeting_time} - {topic} - {rec_type} - {recording_id}.{file_extension}"
    DEFAULT_FOLDERNAME_FORMAT = "{topic} - {meeting_time}"

    def __init__(self, config_params: dict):
        """
        Initializes the strategy with its specific configuration parameters.
        """
        self.config = config_params
        # Config sections
        self.section_include = self.config.get(self.SECTION_KEY_INCLUDE, {})
        self.section_exclude = self.config.get(self.SECTION_KEY_EXCLUDE, {})
        self.section_filepath_format = self.config.get(self.SECTION_KEY_FF, {})
        # User filtering settings
        self.include_emails = self.section_include.get(self.KEY_EMAILS, [])
        self.exclude_emails = self.section_exclude.get(self.KEY_EMAILS, [])
        # Meeting filtering settings
        self.include_topics = self.section_include.get(self.KEY_TOPICS, [])
        self.exclude_topics = self.section_exclude.get(self.KEY_TOPICS, [])
        # File and folder naming settings
        self.timezone_str = self.section_filepath_format.get(self.KEY_TIMEZONE, self.DEFAULT_TIMEZONE)
        self.strftime_format = self.section_filepath_format.get(self.KEY_TIME_FORMAT, f"{self.DEFAULT_TIME_FORMAT} {self.DEFAULT_TIMEZONE}")
        self.filename_format = self.section_filepath_format.get(self.KEY_FILENAME_FORMAT, self.DEFAULT_FILENAME_FORMAT)
        self.folder_format = self.section_filepath_format.get(self.KEY_FOLDERNAME_FORMAT, self.DEFAULT_FOLDERNAME_FORMAT)
        # The
        self.filepath_replace_old = self.section_filepath_format.get(self.KEY_FILEPATH_REPLACE_OLD, "")
        self.filepath_replace_new = self.section_filepath_format.get(self.KEY_FILEPATH_REPLACE_NEW, "")


    def should_ignore_user(self, email: str) -> bool:
        if not self._should_include_user(email):
            return True
        if self._should_exclude_user(email):
            return True
        return False


    def should_ignore_meeting(self, meeting: dict) -> bool:
        meeting_topic = meeting.get("topic", "")
        if not self._should_include_meeting(meeting_topic):
            return True
        if self._should_exclude_meeting(meeting_topic):
            return True
        return False


    def format_filename(self, meeting: dict, recording_file: dict) -> (str, str):
        file_extension = recording_file["file_extension"].lower()
        recording_id = recording_file["id"]
        recording_type = recording_file["recording_type"]

        invalid_chars_pattern = r'[<>:"/\\|?*\x00-\x1F]'
        topic = regex.sub(invalid_chars_pattern, '', meeting["topic"])
        rec_type = recording_type.replace("_", " ").title()
        meeting_time_utc = parser.parse(meeting["start_time"]).replace(tzinfo=timezone.utc)
        meeting_time_local = meeting_time_utc.astimezone(ZoneInfo(self.timezone_str))
        year = meeting_time_local.strftime("%Y")
        month = meeting_time_local.strftime("%m")
        day = meeting_time_local.strftime("%d")
        meeting_time = meeting_time_local.strftime(self.strftime_format)

        filename = self.filename_format.format(**locals()).replace(self.filepath_replace_old, self.filepath_replace_new)
        folder_name = self.folder_format.format(**locals()).replace(self.filepath_replace_old, self.filepath_replace_new)
        return folder_name, filename


    def _should_include_user(self, email: str) -> bool:
        if not self.include_emails:
            return True
        for pattern in self.include_emails:
            if fnmatch.fnmatch(email, pattern):
                return True
        return False


    def _should_exclude_user(self, email: str) -> bool:
        if not self.exclude_emails:
            return False
        for pattern in self.exclude_emails:
            if fnmatch.fnmatch(email, pattern):
                return True
        return False


    def _should_include_meeting(self, topic: str) -> bool:
        if not self.include_topics:
            return True
        for pattern in self.include_topics:
            if fnmatch.fnmatch(topic, pattern):
                return True
        return False


    def _should_exclude_meeting(self, topic: str) -> bool:
        if not self.exclude_topics:
            return False
        for pattern in self.exclude_topics:
            if fnmatch.fnmatch(topic, pattern):
                return True
        return False