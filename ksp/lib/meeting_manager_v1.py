# In your meeting_manager.py file

import pandas as pd
# from interfaces import IDataSource # Assuming you have this interface

class MeetingManager: # Or class MeetingManager(IDataSource):
    """
    Manages meeting data from a CSV file using defined constants.
    """
    # --- Constants for column names and status flags ---
    COL_MEETING_ID = 'Meeting_ID'
    COL_TOPIC = 'Meeting_Topic'
    COL_STATUS = 'Status'

    STATUS_PROCESSED = 'processed'
    STATUS_UNPROCESSED = 'unprocessed'

    # Constructor!
    def __init__(self, csv_path: str):
        self.path = csv_path
        try:
            self.dataFile = pd.read_csv(self.path)
        except FileNotFoundError:
            # Define columns for a new file using the constants
            cols = [self.COL_MEETING_ID, self.COL_TOPIC, self.COL_STATUS]
            self.dataFile = pd.DataFrame(columns=cols)

        # Ensure the status column exists, defaulting to 'unprocessed'
        if self.COL_STATUS not in self.dataFile.columns:
            self.dataFile[self.COL_STATUS] = self.STATUS_UNPROCESSED

    def get_unprocessed_meetings(self) -> pd.DataFrame:
        """
        A specific accessor to get all unprocessed meetings.
        """
        # Build the query string using the constants
        query_string = f"`{self.COL_STATUS}` == '{self.STATUS_UNPROCESSED}'"
        return self.dataFile.query(query_string)

    def mark_as_processed(self, meeting_id):
        """
        Updates the status of a specific meeting to 'processed'.
        """
        # Find the row index using the meeting ID column constant
        idx = self.dataFile.index[self.dataFile[self.COL_MEETING_ID] == meeting_id].tolist()
        if idx:
            # Update the status column using the status constant
            self.dataFile.loc[idx[0], self.COL_STATUS] = self.STATUS_PROCESSED
        else:
            print(f"Warning: Meeting with ID {meeting_id} not found.")

    def save_changes(self):
        """
        Writes the updated DataFrame back to the original CSV file.
        """
        self.dataFile.to_csv(self.path, index=False)