from datetime import datetime
import pandas as pd
from pandas import DataFrame
from zoneinfo import ZoneInfo

from lib.console import Console


class MetadataDB:
    """
    Provides query and update methods for meeting metadata read from a CSV file.
    """
    class Columns:
        """
        Defines constants for the column names in the metadata CSV file.
        """
        MEETING_TOPIC = 'Topic'
        MEETING_ID = 'ID'
        MEETING_START_TIME_PST = 'Start_Time'
        MEETING_FILE_SIZE = 'File_Size_MB'
        MEETING_TYPE = "Type"
        BOOK_AUTHOR = 'Author'
        BOOK_TITLE = 'Book_Title'
        BOOK_ID = 'Book_ID'
        GROUP_ID = 'Group_ID'
        ACTION = 'Action'
        SESSION = 'Session'
        BOOK_CHAPTERS = 'Chapters'
        MEETING_YEAR = 'Year'
        MEETING_FILES_DOWNLOADED = 'Downloaded'
        MEETING_FILES_UPLOADED = 'Uploaded'
        MEETING_FILES_VERIFIED = 'Verified'
        DELETED_FROM_ZOOM = 'Deleted_from_Zoom'
        # Adds this column
        STATUS = 'Status'

    class Values:
        STATUS_YES = 'YES'
        STATUS_NO = ''

    class TimeInfo:
        class Zoom:
            FORMAT = "%Y-%m-%dT%H:%M:%SZ"
            TZ = ZoneInfo("UTC")  # Zoom stores times in UTC
        class CSV:
            FORMAT = "%d/%m/%Y %H:%M:%S"
            TZ = ZoneInfo("America/Los_Angeles")

    class ZoomMeetingWrapper:
        _UUID = "uuid"  # Unique identifier for instance of the meeting
        _ID = "id"  # Recurring Meeting ID
        _TOPIC = "topic"
        _START_TIME = "start_time"  # Start time of the meeting
        _TIMEZONE = "timezone"  # Timezone of the meeting

        def __init__(self, raw_meeting_data: dict):
            if not isinstance(raw_meeting_data, dict):
                raise TypeError("Meeting data must be a dictionary.")
            self._data = raw_meeting_data

        @property
        def uuid(self) -> str:
            return self._data[self._UUID]

        @property
        def id(self) -> str:
            meeting_id = self._data[self._ID]
            meeting_id = "{:011d}".format(meeting_id)
            meeting_id = f"{meeting_id[:3]} {meeting_id[3:7]} {meeting_id[7:]}"

            return meeting_id

        @property
        def topic(self) -> str:
            return self._data[self._TOPIC]

        @property
        def start_time(self) -> datetime:
            start_time_str_zoom = self._data[self._START_TIME]
            start_time_obj = datetime.strptime(start_time_str_zoom, MetadataDB.TimeInfo.Zoom.FORMAT)
            start_time_obj = start_time_obj.replace(tzinfo=MetadataDB.TimeInfo.Zoom.TZ)
            return start_time_obj

        def __str__(self):
            return f"ZoomMeeting(id={self.id}, topic={self.topic}, start_time={self.start_time})"

    class MeetingMetadata:

        def __init__(self, csv_data: DataFrame):
            if not isinstance(csv_data, DataFrame):
                raise TypeError("csv_data must be a DataFrame.")
            self._data = csv_data

        @property
        def size(self) -> bool:
            return len(self._data)

        @property
        def language(self) -> str:
            return self._data["Language"].iloc[0]

        @property
        def author(self) -> str:
            return self._data["Author"].iloc[0]

        @property
        def book_id(self) -> str:
            return self._data["Book_ID"].iloc[0]

        @property
        def book_title(self) -> str:
            return self._data["Book_Title"].iloc[0]

        @property
        def chapters(self) -> str:
            return self._data["Chapters"].iloc[0]

        @property
        def group_id(self) -> str:
            return self._data["Group_ID"].iloc[0]


    # Constructor!
    def __init__(self, csv_path: str):
        self.path = csv_path
        try:
            self.metadataFile = pd.read_csv(
                self.path,
                parse_dates=[self.Columns.MEETING_START_TIME_PST],
                dayfirst=True # Assume csv Start_Time is UK format date.
            )
        except FileNotFoundError:
            print(f"csv metadata file not found: {csv_path}")
            exit(1)

        # All meeting start times are PST and Zoom stores them in UTC, so we need to convert them to UTC.
        self.metadataFile[self.Columns.MEETING_START_TIME_PST] = (
            self.metadataFile[self.Columns.MEETING_START_TIME_PST]
            .dt.tz_localize(self.TimeInfo.CSV.TZ)  # Localize to PST
            .dt.tz_convert(self.TimeInfo.Zoom.TZ)  # Convert to UTC
        )

        # Ensure the status column exists, defaulting to 'unprocessed'
        if self.Columns.STATUS not in self.metadataFile.columns:
            self.metadataFile[self.Columns.STATUS] = self.Values.STATUS_NO


    def _find_by_criteria(self, **criteria) -> pd.DataFrame:
        """
         Finds meetings using a flexible set of key-value criteria.
         - Standard key=value is an AND condition.
         - A key with a list of values (key=[v1, v2]) is an OR condition.
         Example AND: find_by_criteria(Status='unprocessed', Author='John Doe')
         Example OR: find_by_criteria(Status='unprocessed', Author=['John Doe', 'Arthur Dailey'])
         """
        # Console.log(f"Finding metadata for meeting(s) by criteria:\n{criteria}")

        # Start with a mask that is True for all rows
        mask = pd.Series(True, index=self.metadataFile.index)

        # Sequentially apply each criterion to the mask
        for search_column, search_value in criteria.items():
            if isinstance(search_value, list):
                # If the value is a list, use .isin() for a logical OR
                mask = mask & (self.metadataFile[search_column].isin(search_value))
            else:
                # Otherwise, use a standard equality check
                mask = mask & (self.metadataFile[search_column] == search_value)

        # Return only the rows from the original DataFrame where the mask is True
        return self.metadataFile[mask]


    def find_csv_metadata_for(self, zoom_data:dict) -> MeetingMetadata:
        zoom_meeting_wrapper = self.ZoomMeetingWrapper(zoom_data)
        # Although the same Meeting ID will occur many times there should only be one with a particular start time.
        search_criteria = {
            self.Columns.MEETING_ID: zoom_meeting_wrapper.id,
            self.Columns.MEETING_START_TIME_PST: zoom_meeting_wrapper.start_time
        }
        # print(f"Looking for a meeting with {self.Columns.MEETING_ID}[{meeting_id}] and {self.Columns.MEETING_START_TIME_PST}[{meeting_start_datetime}]")
        matching_meetings = self._find_by_criteria(**search_criteria)
        metadata = self.MeetingMetadata(matching_meetings)
        return metadata

    def is_meeting_present(self, zoom_data:dict) -> bool:
        csv_metadata = self.find_csv_metadata_for(zoom_data)
        num_meetings = csv_metadata.size
        # print(f'Found {num_meetings} matching meeting(s)')

        if num_meetings == 0:
            return False
        if num_meetings == 1:
            return True
        # This block is only reached if num_meetings > 1
        raise ValueError(
            f"Data integrity error: Found {num_meetings} meetings. Expected 0 or 1:\n"
            f"{csv_metadata}"
        )

    # ------------------------------------
    # Junk pile
    # ------------------------------------

    def _count_by_meeting_id(self, meeting_id) -> int:
        # Although the same Meeting ID will occur many times there should only be one with a particular start time.
        search_criteria = {
            self.Columns.MEETING_ID: meeting_id
        }
        print(f"Looking for meetings with {self.Columns.MEETING_ID}[{meeting_id}]")
        matching_meetings = self._find_by_criteria(**search_criteria)
        num_meetings = len(matching_meetings)
        print(f'Found {num_meetings} matching meeting(s)')
        # If no meetings are found, return False
        return num_meetings


    def _count_by_meeting_date(self, meeting_start_date:datetime) -> int:
        # Although the same Meeting ID will occur many times there should only be one with a particular start time.
        search_criteria = {
            self.Columns.MEETING_START_TIME_PST: meeting_start_date
        }
        print(f"Looking for meetings {self.Columns.MEETING_START_TIME_PST}=[{meeting_start_date}]")
        matching_meetings = self._find_by_criteria(**search_criteria)
        num_meetings = len(matching_meetings)
        print(f'Found {num_meetings} matching meeting(s)')
        # If no meetings are found, return False
        return num_meetings


    def is_downloaded(self, meeting_id, meeting_datetime: datetime) -> bool:
        search_criteria = {
            self.Columns.MEETING_ID: meeting_id,
            self.Columns.MEETING_START_TIME_PST: meeting_datetime,
            self.Columns.MEETING_FILES_DOWNLOADED: self.Values.STATUS_YES
        }
        matching_meetings = self._find_by_criteria(**search_criteria)
        num_meetings = len(matching_meetings)
        return num_meetings == 1


    def get_unprocessed_meetings(self) -> pd.DataFrame:
        """
        A specific accessor to get all unprocessed meetings.
        """
        # Build the query string using the constants
        query_string = f"`{self.Columns.STATUS}` == '{self.Values.STATUS_NO}'"
        return self.metadataFile.query(query_string)

    def mark_as_processed(self, meeting_id):
        """
        Updates the status of a specific meeting to 'processed'.
        """
        # Find the row index using the meeting ID column constant
        idx = self.metadataFile.index[self.metadataFile[self.Columns.MEETING_ID] == meeting_id].tolist()
        if idx:
            # Update the status column using the status constant
            self.metadataFile.loc[idx[0], self.Columns.STATUS] = self.Values.STATUS_YES
        else:
            print(f"Warning: Meeting with ID {meeting_id} not found.")

    def save_metadata_changes(self):
        """
        Writes the updated DataFrame back to the original CSV file.
        """
        self.metadataFile.to_csv(self.path, index=False)