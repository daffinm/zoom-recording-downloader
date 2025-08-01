from datetime import datetime
import pandas as pd
from pandas import DataFrame
from zoneinfo import ZoneInfo

from lib.console import Console


class CsvFile:
    class Columns:
        """
        Defines constants for the column names in the metadata CSV file.
        """
        MEETING_TOPIC = 'Topic'
        MEETING_ID = 'ID'
        MEETING_START_TIME = 'Start_Time'
        MEETING_START_TIME_TZ = 'Start_Time_TZ' # Timezone-aware datetime object for the meeting start time
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
        ACTION_DELETE = 'Delete'


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
        start_time_obj = datetime.strptime(start_time_str_zoom, TimeInfo.Zoom.FORMAT)
        start_time_obj = start_time_obj.replace(tzinfo=TimeInfo.Zoom.TZ)
        return start_time_obj

    @property
    def timezone(self) -> str:
        timezone_str = self._data[self._TIMEZONE]
        return timezone_str

    def __str__(self):
        return f"ZoomMeeting(id={self.id}, topic={self.topic}, start_time={self.start_time})"


# Since you are using a mask to search the DataFrame, you should ensure that the `Start_Time` column and the value
# you compare against are both timezone-aware `datetime` objects in UTC.
#
# If you keep `Start_Time` as a string in the DataFrame, convert it to a UTC datetime just before comparison in your
# mask logic. For example, in `_find_by_criteria`, if the column is `Start_Time`, convert both sides to UTC `datetime` before comparing.
#
# Alternatively, you can preprocess the `Start_Time` column after reading the CSV (but not during `read_csv`) to add
# a new column with the UTC datetime, and use that for all mask-based searches. This keeps the original string untouched and ensures reliable comparisons.
class MetadataDB:

    class Row:

        def __init__(self, csv_data: DataFrame):
            if not isinstance(csv_data, DataFrame):
                raise TypeError("csv_data must be a DataFrame.")
            self._csv_data:DataFrame = csv_data

        @property
        def size(self) -> int:
            return len(self._csv_data)

        @property
        def language(self) -> str:
            return self._csv_data["Language"].iloc[0]

        @property
        def author(self) -> str:
            return self._csv_data["Author"].iloc[0]

        @property
        def book_id(self) -> str:
            return self._csv_data["Book_ID"].iloc[0]

        @property
        def book_title(self) -> str:
            return self._csv_data["Book_Title"].iloc[0]

        @property
        def chapters(self) -> str:
            return self._csv_data["Chapters"].iloc[0]

        @property
        def group_id(self) -> str:
            return self._csv_data["Group_ID"].iloc[0]


    # Constructor!
    def __init__(self, csv_path: str):
        self.filepath = csv_path
        try:
            self.metadata_file = pd.read_csv(self.filepath, dtype=str)
        except FileNotFoundError:
            print(f"csv metadata file not found: {csv_path}")
            exit(1)


    def _find_meeting(self, meeting:ZoomMeetingWrapper, criteria:dict=None) -> pd.DataFrame:
        # Validate the criteria if provided
        if not criteria is None:
            if not isinstance(criteria, dict):
                raise TypeError("criteria must be type dict")
            # Ensure the specified search criteria keys are valid columns in the DataFrame
            for search_column in criteria.keys():
                if search_column not in self.metadata_file.columns:
                    raise ValueError(f"Column '{search_column}' does not exist in the metadata file.")

        # Find all the metadata rows that match the meeting ID...
        subset_all_meeting_instances = self.metadata_file[self.metadata_file[CsvFile.Columns.MEETING_ID] == meeting.id]
        if subset_all_meeting_instances.empty:
            raise ValueError(f"No meetings found with ID {meeting.id}")

        # Convert the subset Start_Time col to datetime object that uses the correct timezone according to Zoom
        subset_all_meeting_instances_converted = subset_all_meeting_instances.copy()
        subset_all_meeting_instances_converted[CsvFile.Columns.MEETING_START_TIME] = pd.to_datetime(
            subset_all_meeting_instances[CsvFile.Columns.MEETING_START_TIME],
            format=TimeInfo.CSV.FORMAT
        ).dt.tz_localize(meeting.timezone)

        # Search the converted DataFrame for meetings that match the Start_Time and other criteria.
        full_search_criteria = {
            CsvFile.Columns.MEETING_START_TIME: meeting.start_time
        }
        # If additional criteria are provided, merge them into the search criteria
        if criteria is not None:
            full_search_criteria.update(criteria)

        # Start with a mask that is True for all rows of the subset DataFrame
        # This mask will be used to filter the DataFrame based on the search criteria
        mask = pd.Series(True, index=subset_all_meeting_instances_converted.index)

        # Sequentially apply each criterion to the mask
        for search_column, search_value in full_search_criteria.items():
            # Check if the search column is a pandas datetime column
            if pd.api.types.is_datetime64_any_dtype(subset_all_meeting_instances_converted[search_column]):
                mask &= subset_all_meeting_instances_converted[search_column] == pd.Timestamp(search_value)
            # Check if the search value is a list in which case we want to check if the column values are in that list
            elif isinstance(search_value, list):
                mask &= subset_all_meeting_instances_converted[search_column].isin(search_value)
            # Otherwise, we assume the search value is a single value to match against
            else:
                mask &= subset_all_meeting_instances_converted[search_column] == search_value

        # Return only the rows from the subset where the mask is True
        result = subset_all_meeting_instances_converted[mask]

        assert len(result) == 0 or len(result) == 1, \
            f"Data integrity error: Found {len(result)} meetings with ID {meeting.id} and start time {meeting.start_time}. Expected 0 or 1."

        return result


    def find_csv_metadata_for(self, zoom_meeting_data:dict) -> Row:
        meeting = ZoomMeetingWrapper(zoom_meeting_data)
        matching_meetings = self._find_meeting(meeting=meeting)
        metadata = self.Row(matching_meetings)
        return metadata


    def is_meeting_listed(self, zoom_data:dict) -> bool:
        csv_metadata = self.find_csv_metadata_for(zoom_data)
        num_meetings = csv_metadata.size

        if num_meetings == 0:
            return False
        if num_meetings == 1:
            return True
        # This block is only reached if num_meetings > 1
        raise ValueError(
            f"Data integrity error: Found {num_meetings} meetings. Expected 0 or 1:\n"
            f"{csv_metadata}"
        )


    def is_already_downloaded(self, zoom_meeting_data:dict) -> bool:
        zoom_meeting_wrapper = ZoomMeetingWrapper(zoom_meeting_data)
        criteria = {
            CsvFile.Columns.MEETING_FILES_DOWNLOADED: CsvFile.Values.STATUS_YES
        }
        matching_meetings = self._find_meeting(zoom_meeting_wrapper, criteria)
        return not matching_meetings.empty


    def is_meeting_to_be_deleted(self, zoom_meeting_data:dict) -> bool:
        zoom_meeting_wrapper = ZoomMeetingWrapper(zoom_meeting_data)
        criteria = {
            CsvFile.Columns.ACTION: CsvFile.Values.ACTION_DELETE
        }
        matching_meetings = self._find_meeting(zoom_meeting_wrapper, criteria)
        return not matching_meetings.empty


    def mark_as_downloaded(self, zoom_meeting_data:dict):
        zoom_meeting_wrapper = ZoomMeetingWrapper(zoom_meeting_data)
        matching_meeting = self._find_meeting(zoom_meeting_wrapper)

        if not matching_meeting.empty:
            idx = matching_meeting.index[0]
            self.metadata_file.loc[idx, CsvFile.Columns.MEETING_FILES_DOWNLOADED] = CsvFile.Values.STATUS_YES
        else:
            print(
                f"Warning: Meeting with ID {zoom_meeting_wrapper.id} and start time {zoom_meeting_wrapper.start_time} not found.")


    def save(self):
        self.metadata_file.to_csv(self.filepath, index=False)