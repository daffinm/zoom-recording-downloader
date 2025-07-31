# In your main_script.py file

from datetime import datetime
from meeting_metadata import MetadataDB

meeting_csv_file = '../metadata/David_Wood_Zoom_Recordings-2022-11-06--2024-11-07.csv'

# 1. Initialize the manager
manager = MetadataDB(meeting_csv_file)


def get_all_unprocessed_meetings():
    # 2. Get the metadata for meetings that need to be processed
    search_criteria = {
        MetadataDB.BOOK_AUTHOR: 'KSP',
        MetadataDB.STATUS: MetadataDB.STATUS_NO
    }
    meetings_to_process = manager._find_by_criteria(**search_criteria)
    print(f"Found {len(meetings_to_process)} meetings to process.")
    # 3. Loop through them and access data using the constants
    for index, meeting in meetings_to_process.iterrows():
        # Access DataFrame columns using the constants from the MeetingManager class
        meeting_id = meeting[MetadataDB.MEETING_ID]
        topic = meeting[MetadataDB.MEETING_TOPIC]

        print(f"Processing meeting ID[{meeting_id}], Topic[{topic}]")

        # --- Your download and renaming logic would go here ---
        success = True  # Placeholder

        # 4. If successful, mark it as processed
        if success:
            # Pass the meeting_id variable to the processing method
            manager.mark_as_downloaded(meeting_id)
            print(f"Marked meeting {meeting_id} as processed.")
    # 5. Save all changes back to the CSV file
    manager.save_metadata_changes()
    print(f"All changes have been saved to: {meeting_csv_file}")


def assert_equals(expected, actual):
    assert expected == actual, f"Test Failed:\nExpected: {expected}\nActual:   {actual}"


def xtest_is_downloaded():
    meeting_id = "890 2076 3746"
    meeting_datetime = "06/11/2022 10:52:43"
    result = manager.is_downloaded(meeting_id, meeting_datetime)
    assert_equals(False, result)

def test_is_listed_once_format_uk_true():
    meeting_id = "890 2076 3746"
    meeting_datetime_string = "06/11/2022 10:52:43"
    datetime_format = "%d/%m/%Y %H:%M:%S"
    meeting_datetime_obj = datetime.strptime(meeting_datetime_string, datetime_format)
    result = manager.is_meeting_present(meeting_id, meeting_datetime_obj)
    assert result is True

def test_is_listed_once_format_us_true():
    meeting_id = "890 2076 3746"
    meeting_datetime_string = "11/06/2022 10:52:43"
    datetime_format = "%m/%d/%Y %H:%M:%S"
    meeting_datetime_obj = datetime.strptime(meeting_datetime_string, datetime_format)
    result = manager.is_meeting_present(meeting_id, meeting_datetime_obj)
    assert result is True


test_is_listed_once_format_uk_true()