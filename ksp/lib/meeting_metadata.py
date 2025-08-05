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
        AUTHOR = 'Author'
        BOOK_TITLE = 'Book_Title'
        LANGUAGE = 'Language'
        BOOK_ID = 'Book_ID'
        BOOK_CHAPTERS = 'Chapters'
        GROUP_ID = 'Group_ID'
        ACTION = 'Action'
        DOWNLOADED = 'Downloaded'
        FOLDER_NAME = 'Folder_Name'
        BASE_FILENAME = 'Base_Filename'

    class Values:
        YES = "YES"
        NO = "NO"
        DELETE = 'Delete'
        IGNORE = 'Ignore'

    @staticmethod
    def required_columns():
        """
        Returns a list of metadata columns that must have a value in the CSV metadata file.
        :return:
        """
        return [
            CsvFile.Columns.MEETING_ID,
            CsvFile.Columns.MEETING_START_TIME,
            CsvFile.Columns.AUTHOR,
            CsvFile.Columns.BOOK_TITLE,
            CsvFile.Columns.LANGUAGE,
            CsvFile.Columns.BOOK_ID,
            CsvFile.Columns.BOOK_CHAPTERS,
            CsvFile.Columns.GROUP_ID
        ]

class TimeInfo:
    class Zoom:
        FORMAT = "%Y-%m-%dT%H:%M:%SZ"
        TZ = ZoneInfo("UTC")  # Zoom stores times in UTC
    class CSV:
        FORMAT = "%d/%m/%Y %H:%M:%S"
        TZ = ZoneInfo("America/Los_Angeles")


class ZoomMeetingWrapper:
    _ID = "id"  # Recurring Meeting ID
    _TOPIC = "topic"
    _START_TIME = "start_time"  # Start time of the meeting
    _TIMEZONE = "timezone"  # Timezone of the meeting

    def __init__(self, raw_meeting_data: dict):
        if not isinstance(raw_meeting_data, dict):
            raise TypeError("Meeting data must be a dictionary.")
        self._data = raw_meeting_data

    @property
    def id(self) -> str:
        meeting_id = self._data[self._ID]
        meeting_id_str = str(meeting_id)
        if len(meeting_id_str) == 11:
            meeting_id_formatted = f"{meeting_id_str[:3]} {meeting_id_str[3:7]} {meeting_id_str[7:]}"
            return meeting_id_formatted
        elif len(meeting_id_str) == 10:
            meeting_id_formatted = f"{meeting_id_str[:3]} {meeting_id_str[3:6]} {meeting_id_str[6:]}"
            return meeting_id_formatted
        raise ValueError("Meeting ID must be 10 or 11 digits long.")

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
            if csv_data is None:
                raise ValueError("csv_data cannot be None")
            if not isinstance(csv_data, DataFrame):
                raise TypeError("If specified csv_data must be a pandas DataFrame")
            self._csv_data = csv_data
            # If not an empty DataFrame then validate that all cells in the important columns are not empty
            if not self._csv_data.empty:
                if self.action not in [CsvFile.Values.DELETE, CsvFile.Values.IGNORE]:
                    for column in CsvFile.required_columns():
                        if self._csv_data[column].isnull().any() or (self._csv_data[column] == '').any():
                            raise ValueError(f"csv_data: column '{column}' contains empty values")


        @property
        def empty(self) -> bool:
            return self._csv_data.empty

        @property
        def language(self) -> str:
            return self._csv_data[CsvFile.Columns.LANGUAGE].iloc[0]

        @property
        def author(self) -> str:
            return self._csv_data[CsvFile.Columns.AUTHOR].iloc[0]

        @property
        def book_id(self) -> str:
            return self._csv_data[CsvFile.Columns.BOOK_ID].iloc[0]

        @property
        def book_title(self) -> str:
            return self._csv_data[CsvFile.Columns.BOOK_TITLE].iloc[0]

        @property
        def chapters(self) -> str:
            return self._csv_data[CsvFile.Columns.BOOK_CHAPTERS].iloc[0]

        @property
        def group_id(self) -> str:
            return self._csv_data[CsvFile.Columns.GROUP_ID].iloc[0]

        @property
        def action(self) -> str:
            return self._csv_data[CsvFile.Columns.ACTION].iloc[0]

        @property
        def downloaded(self) -> str:
            return self._csv_data[CsvFile.Columns.DOWNLOADED].iloc[0]


    # Constructor!
    def __init__(self, csv_path: str):
        self.filepath = csv_path
        try:
            self.metadata_file = pd.read_csv(self.filepath, dtype=str)
        except FileNotFoundError:
            print(f"csv metadata file not found: {csv_path}")
            exit(1)
        except pd.errors.EmptyDataError:
            print(f"csv metadata file is empty: {csv_path}")
            exit(1)
        # Validate that the required columns are present in the DataFrame
        for column in CsvFile.required_columns():
            if column not in self.metadata_file.columns:
                raise ValueError(f"Metadata file is missing required column '{column}'")



    def _find_meeting(self, meeting:ZoomMeetingWrapper, criteria:dict=None) -> pd.DataFrame:
        """
        Returns a one row DataFrame containing the metadata for the specified meeting if it exists in the metadata file.
        If criteria is provided, it will filter the results based on the criteria
        If no matching meeting is found, returns None.
        :param meeting: wrapper object containing the meeting data.
        :param criteria: optional dictionary of additional search criteria to filter the results.
        :return: None if no matching meeting found, otherwise a DataFrame with the matching meeting metadata.
        :raises TypeError: If criteria is not a dictionary.
        :raises ValueError: If criteria contains a column that does not exist in the metadata file.
        :raises ValueError: If multiple meetings are found with the same ID and start time (should be unique).
        """
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
            return subset_all_meeting_instances

        # Convert the subset Start_Time col to datetime object that uses the correct timezone according to Zoom
        subset_all_meeting_instances_datetime = subset_all_meeting_instances.copy()
        subset_all_meeting_instances_datetime[CsvFile.Columns.MEETING_START_TIME] = pd.to_datetime(
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
        mask = pd.Series(True, index=subset_all_meeting_instances_datetime.index)

        # Sequentially apply each criterion to the mask
        for search_column, search_value in full_search_criteria.items():
            # Check if the search column is a pandas datetime column
            if pd.api.types.is_datetime64_any_dtype(subset_all_meeting_instances_datetime[search_column]):
                mask &= subset_all_meeting_instances_datetime[search_column] == pd.Timestamp(search_value)
            # Check if the search value is a list in which case we want to check if the column values are in that list
            elif isinstance(search_value, list):
                mask &= subset_all_meeting_instances_datetime[search_column].isin(search_value)
            # Otherwise, we assume the search value is a single value to match against
            else:
                mask &= subset_all_meeting_instances_datetime[search_column] == search_value

        # Return only the rows from the subset where the mask is True
        result:DataFrame = subset_all_meeting_instances_datetime[mask]

        num_results = len(result)
        if num_results > 1:
            raise ValueError(
                f"Data integrity error: Found {num_results} meetings with ID {meeting.id} and start time {meeting.start_time}. "
                "Expected 0 or 1. This indicates a problem with the metadata file."
            )
        # Could be empty if no meetings match the criteria
        return result


    def find_csv_metadata_for(self, zoom_meeting_data:dict) -> Row:
        meeting = ZoomMeetingWrapper(zoom_meeting_data)
        matching_meetings = self._find_meeting(meeting=meeting)
        metadata = self.Row(matching_meetings)
        return metadata


    def should_ignore_meeting(self, zoom_meeting_data:dict) -> bool:
        zoom_meeting_wrapper = ZoomMeetingWrapper(zoom_meeting_data)
        matching_meetings = self._find_meeting(zoom_meeting_wrapper)
        row = MetadataDB.Row(matching_meetings)

        if row.empty:
            Console.warn(f"Meeting ID=[{zoom_meeting_wrapper.id}] Topic=[{zoom_meeting_wrapper.topic}]: No metadata found.")
            return True
        if row.action == CsvFile.Values.IGNORE or row.action == CsvFile.Values.DELETE:
            Console.warn(f"Meeting ID=[{zoom_meeting_wrapper.id}] Topic=[{zoom_meeting_wrapper.topic}]: Action={row.action}")
            return True
        # This means we can delete files from disk after uploading them and then move on to the next batch.
        if row.downloaded == CsvFile.Values.YES:
            Console.warn(f"Meeting ID=[{zoom_meeting_wrapper.id}] Topic=[{zoom_meeting_wrapper.topic}]: Downloaded={row.downloaded}")
            return True

        return False


    def mark_as_downloaded(self, zoom_meeting_data:dict, folder_name:str, filename:str):
        zoom_meeting_wrapper = ZoomMeetingWrapper(zoom_meeting_data)
        matching_meeting = self._find_meeting(zoom_meeting_wrapper)

        if not matching_meeting.empty:
            idx = matching_meeting.index[0]
            self.metadata_file.loc[idx, CsvFile.Columns.DOWNLOADED] = CsvFile.Values.YES
            self.metadata_file.loc[idx, CsvFile.Columns.FOLDER_NAME] = folder_name
            self.metadata_file.loc[idx, CsvFile.Columns.BASE_FILENAME] = filename.rsplit('.', 1)[0]
        else:
            raise ValueError(
                f"Cannot mark as downloaded. Meeting ID=[{zoom_meeting_wrapper.id}], start_time={zoom_meeting_wrapper.start_time} not found in metadata.")


    def save(self):
        self.metadata_file.to_csv(self.filepath, index=False)