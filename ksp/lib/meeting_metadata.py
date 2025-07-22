# In your meeting_metadata.py file
from datetime import datetime

import pandas as pd


# from interfaces import IDataSource # Assuming you have this interface

class MeetingMetadata: # Or class MeetingManager(IDataSource):
    """
    Manages meeting data from a CSV file using defined constants.
    """
    # --- Constants for column names and status flags ---
    COL_MEETING_TOPIC = 'Topic'
    COL_MEETING_ID = 'ID'
    COL_MEETING_START_DATETIME = 'Start_Time'
    COL_MEETING_FILE_SIZE = 'File_Size_MB'
    COL_MEETING_TYPE = "Type"
    COL_BOOK_AUTHOR = 'Author'
    COL_BOOK_TITLE = 'Book_Title'
    COL_BOOK_ID = 'Book_ID'
    COL_GROUP_ID = 'Group_ID'
    COL_ACTION = 'Action'
    COL_SESSION = 'Session'
    COL_BOOK_CHAPTERS = 'Chapters'
    COL_MEETING_YEAR = 'Year'
    COL_MEETING_FILES_DOWNLOADED = 'Downloaded'
    COL_MEETING_FILES_UPLOADED = 'Uploaded'
    COL_MEETING_FILES_VERIFIED = 'Verified'
    COL_DELETED_FROM_ZOOM = 'Deleted_from_Zoom'
    # Adds this column
    COL_STATUS = 'Status'
    # Status flags - for boolean columns
    STATUS_YES = 'YES'
    STATUS_NO = ''

    # Constructor!
    def __init__(self, csv_path: str):
        self.path = csv_path
        try:
            self.metadataFile = pd.read_csv(
                self.path,
                parse_dates=[self.COL_MEETING_START_DATETIME],
                dayfirst=True # Assume csv Start_Time is UK format date.
            )
        except FileNotFoundError:
            print(f"csv metadata file not found: {csv_path}")
            exit(1)

        # Ensure the status column exists, defaulting to 'unprocessed'
        if self.COL_STATUS not in self.metadataFile.columns:
            self.metadataFile[self.COL_STATUS] = self.STATUS_NO

    def find_by_criteria(self, **criteria) -> pd.DataFrame:
        """
         Finds meetings using a flexible set of key-value criteria.
         - Standard key=value is an AND condition.
         - A key with a list of values (key=[v1, v2]) is an OR condition.
         Example AND: find_by_criteria(Status='unprocessed', Author='John Doe')
         Example OR: find_by_criteria(Status='unprocessed', Author=['John Doe', 'Arthur Dailey'])
         """
        # Start with a mask that is True for all rows
        mask = pd.Series(True, index=self.metadataFile.index)

        # Sequentially apply each criterion to the mask
        for column, value in criteria.items():
            if isinstance(value, list):
                # If the value is a list, use .isin() for a logical OR
                mask = mask & (self.metadataFile[column].isin(value))
            else:
                # Otherwise, use a standard equality check
                mask = mask & (self.metadataFile[column] == value)

        # Return only the rows from the original DataFrame where the mask is True
        return self.metadataFile[mask]

    def is_meeting_listed_once(self, meeting_id, meeting_start_datetime: datetime) -> bool:
        # Although the same Meeting ID will occur many times there should only be one with a particular start time.
        search_criteria = {
            MeetingMetadata.COL_MEETING_ID: meeting_id,
            MeetingMetadata.COL_MEETING_START_DATETIME: meeting_start_datetime
        }
        print(f"Looking for a meeting with {MeetingMetadata.COL_MEETING_ID}[{meeting_id}] and {MeetingMetadata.COL_MEETING_START_DATETIME}[{meeting_start_datetime}]")
        matching_meetings = self.find_by_criteria(**search_criteria)
        num_meetings = len(matching_meetings)
        print(f'Found {num_meetings} matching meeting(s)')

        if num_meetings == 0:
            return False
        if num_meetings == 1:
            return True
        # This block is only reached if num_meetings > 1
        raise ValueError(
            f"Data integrity error: Found {num_meetings} meetings. Expected 0 or 1:\n"
            f"{matching_meetings}"
        )

    def is_downloaded(self, meeting_id, meeting_datetime: datetime) -> bool:
        search_criteria = {
            MeetingMetadata.COL_MEETING_ID: meeting_id,
            MeetingMetadata.COL_MEETING_START_DATETIME: meeting_datetime,
            MeetingMetadata.COL_MEETING_FILES_DOWNLOADED: MeetingMetadata.STATUS_YES
        }
        matching_meetings = self.find_by_criteria(**search_criteria)
        num_meetings = len(matching_meetings)
        return num_meetings == 1


    def get_unprocessed_meetings(self) -> pd.DataFrame:
        """
        A specific accessor to get all unprocessed meetings.
        """
        # Build the query string using the constants
        query_string = f"`{self.COL_STATUS}` == '{self.STATUS_NO}'"
        return self.metadataFile.query(query_string)

    def mark_as_processed(self, meeting_id):
        """
        Updates the status of a specific meeting to 'processed'.
        """
        # Find the row index using the meeting ID column constant
        idx = self.metadataFile.index[self.metadataFile[self.COL_MEETING_ID] == meeting_id].tolist()
        if idx:
            # Update the status column using the status constant
            self.metadataFile.loc[idx[0], self.COL_STATUS] = self.STATUS_YES
        else:
            print(f"Warning: Meeting with ID {meeting_id} not found.")

    def save_changes(self):
        """
        Writes the updated DataFrame back to the original CSV file.
        """
        self.metadataFile.to_csv(self.path, index=False)