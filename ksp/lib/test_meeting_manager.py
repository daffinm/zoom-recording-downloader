# In your main_script.py file

from datetime import datetime
from meeting_manager import MeetingManager

meeting_csv_file = '../metadata/David_Wood_Zoom_Recordings-2022-11-06--2024-11-07.csv'

# 1. Initialize the manager
manager = MeetingManager(meeting_csv_file)


def get_all_unprocessed_meetings():
    # 2. Get the metadata for meetings that need to be processed
    search_criteria = {
        MeetingManager.COL_BOOK_AUTHOR: 'KSP',
        MeetingManager.COL_STATUS: MeetingManager.STATUS_NO
    }
    meetings_to_process = manager.find_by_criteria(**search_criteria)
    print(f"Found {len(meetings_to_process)} meetings to process.")
    # 3. Loop through them and access data using the constants
    for index, meeting in meetings_to_process.iterrows():
        # Access DataFrame columns using the constants from the MeetingManager class
        meeting_id = meeting[MeetingManager.COL_MEETING_ID]
        topic = meeting[MeetingManager.COL_MEETING_TOPIC]

        print(f"Processing meeting ID[{meeting_id}], Topic[{topic}]")

        # --- Your download and renaming logic would go here ---
        success = True  # Placeholder

        # 4. If successful, mark it as processed
        if success:
            # Pass the meeting_id variable to the processing method
            manager.mark_as_processed(meeting_id)
            print(f"Marked meeting {meeting_id} as processed.")
    # 5. Save all changes back to the CSV file
    manager.save_changes()
    print(f"All changes have been saved to: {meeting_csv_file}")


def assert_equals(expected, actual):
    assert expected == actual, f"Test Failed:\nExpected: {expected}\nActual:   {actual}"


def xtest_is_downloaded():
    meeting_id = "890 2076 3746"
    meeting_datetime = "06/11/2022 10:52:43"
    result = manager.is_downloaded(meeting_id, meeting_datetime)
    assert_equals(False, result)

def xtest_is_present_true_v1():
    meeting_id = "890 2076 3746"
    meeting_datetime_uk = "06/11/2022 10:52:43"
    meeting_datetime_us = "11/06/2022 10:52:43"
    result = manager.is_present(meeting_id, meeting_datetime_uk)
    assert_equals(True, result)
    result = manager.is_present(meeting_id, meeting_datetime_us)
    assert_equals(True, result)

def test_is_present_true():
    meeting_id = "890 2076 3746"
    # The date string from your test data (UK format)
    meeting_datetime_string_uk = "06/11/2022 10:52:43"
    meeting_datetime_string_us = "11/06/2022 10:52:43"
    # Define the format to match the string
    uk_format = "%d/%m/%Y %H:%M:%S"
    us_format = "%m/%d/%Y %H:%M:%S"

    # Convert the string to a true datetime object
    meeting_datetime_obj = datetime.strptime(meeting_datetime_string_uk, uk_format)
    # Now pass the object to your function
    result = manager.is_present(meeting_id, meeting_datetime_obj)
    # This assertion is now reliable and testing the correct logic
    assert result is True

    # Convert the string to a true datetime object
    meeting_datetime_obj = datetime.strptime(meeting_datetime_string_us, us_format)
    # Now pass the object to your function
    result = manager.is_present(meeting_id, meeting_datetime_obj)
    # This assertion is now reliable and testing the correct logic
    assert result is True


# get_all_unprocessed_meetings()
test_is_present_true()