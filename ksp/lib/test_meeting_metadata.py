import unittest
import shutil
from datetime import datetime

from meeting_metadata import MetadataDB

class TestMetadataDB(unittest.TestCase):
    filepath_csv_original = '../metadata/David_Wood_Zoom_Recordings-2022-11-06--2022-11-07 - Meetings.csv'
    filepath_csv_test = '../metadata/David_Wood_Zoom_Recordings-2022-11-06--2022-11-07 - Meetings - COPY.csv'

    zoom_meeting_data_to_download = {'account_id': 'NKeXSYsCQumvGVXQMqbwRA', 'duration': 97, 'host_id': 'JI5dmsyfS2Sa06vQmSAH3Q', 'id': 81679270835, 'recording_count': 3, 'recording_files': [{'download_url': 'https://us02web.zoom.us/rec/download/f3y1GTNFawfGNS9tKztDe9aZ-WBHHbbKX19bF5UbAy7ThMpeeAY1Owj0DgGp6NqFQDdV3oElytBf8gRi.csCvo6nKOtp-Yc0f', 'file_extension': 'TXT', 'file_size': 156, 'file_type': 'CHAT', 'id': '094245e7-4ac5-4762-a053-db8459cefc38', 'meeting_id': 'h9g61rB5QdqNgL+Pc6W1aw==', 'play_url': 'https://us02web.zoom.us/rec/play/f3y1GTNFawfGNS9tKztDe9aZ-WBHHbbKX19bF5UbAy7ThMpeeAY1Owj0DgGp6NqFQDdV3oElytBf8gRi.csCvo6nKOtp-Yc0f', 'recording_end': '2022-11-06T22:29:48Z', 'recording_start': '2022-11-06T20:52:11Z', 'recording_type': 'chat_file', 'status': 'completed'}, {'download_url': 'https://us02web.zoom.us/rec/download/mV6k_eRoN5sB9JFd1SWpOxbtwHSykMbUDnbDafNC4RxDiPs5BIHcYK1Lu-ZzyzeToTzCqsE9Fygm8MBn.aFZwzWtvStWidWXL', 'file_extension': 'MP4', 'file_size': 673570859, 'file_type': 'MP4', 'id': '587ac804-b508-4c37-9fbe-f240caa970a2', 'meeting_id': 'h9g61rB5QdqNgL+Pc6W1aw==', 'play_url': 'https://us02web.zoom.us/rec/play/mV6k_eRoN5sB9JFd1SWpOxbtwHSykMbUDnbDafNC4RxDiPs5BIHcYK1Lu-ZzyzeToTzCqsE9Fygm8MBn.aFZwzWtvStWidWXL', 'recording_end': '2022-11-06T22:29:48Z', 'recording_start': '2022-11-06T20:52:11Z', 'recording_type': 'shared_screen_with_gallery_view', 'status': 'completed'}, {'download_url': 'https://us02web.zoom.us/rec/download/MPxNFLV2giphxB4Bfbw5TzS098XGYKYACj5C6TXXHaRaS1P_NED-vj_o3zXReG9g2x0uJ149YE-heSWN.znqL5wqqF-yxWZOl', 'file_extension': 'M4A', 'file_size': 81811758, 'file_type': 'M4A', 'id': 'e31eb6de-bea4-4817-bb0e-45fb1cd83f2a', 'meeting_id': 'h9g61rB5QdqNgL+Pc6W1aw==', 'play_url': 'https://us02web.zoom.us/rec/play/MPxNFLV2giphxB4Bfbw5TzS098XGYKYACj5C6TXXHaRaS1P_NED-vj_o3zXReG9g2x0uJ149YE-heSWN.znqL5wqqF-yxWZOl', 'recording_end': '2022-11-06T22:29:48Z', 'recording_start': '2022-11-06T20:52:11Z', 'recording_type': 'audio_only', 'status': 'completed'}], 'recording_play_passcode': '', 'share_url': 'https://us02web.zoom.us/rec/share/9FXS1nQbAlfX0p3J2Fhx4i9EomRkNBP7rnCgmpDBkQprXeMNuS0aYqdZZH35lzS8.E2nqXYc0925Lqfmq', 'start_time': '2022-11-06T20:52:10Z', 'timezone': 'America/Los_Angeles', 'topic': 'G3-Own Your Sovereignty', 'total_size': 755382773, 'type': 8, 'uuid': 'h9g61rB5QdqNgL+Pc6W1aw=='}
    zoom_meeting_data_to_delete = {'account_id': 'NKeXSYsCQumvGVXQMqbwRA', 'duration': 0, 'host_id': 'JI5dmsyfS2Sa06vQmSAH3Q', 'id': 81679270835, 'recording_count': 2, 'recording_files': [{'download_url': 'https://us02web.zoom.us/rec/download/s9Al_flgWfImuVNGysxO6aFe-IXdZgragSwl_e00NskQx-jSZwFVQlA3faivrDBhvGhHVLIAx3IRF0J_._BrS4ufsQwifSSY0', 'file_extension': 'MP4', 'file_size': 682649, 'file_type': 'MP4', 'id': '0446e0fd-140d-4e3a-9a07-fbc514de5158', 'meeting_id': 'gcTWlKyET4q4oXRJCNZ7eA==', 'play_url': 'https://us02web.zoom.us/rec/play/s9Al_flgWfImuVNGysxO6aFe-IXdZgragSwl_e00NskQx-jSZwFVQlA3faivrDBhvGhHVLIAx3IRF0J_._BrS4ufsQwifSSY0', 'recording_end': '2022-11-06T20:27:30Z', 'recording_start': '2022-11-06T20:26:57Z', 'recording_type': 'shared_screen_with_speaker_view', 'status': 'completed'}, {'download_url': 'https://us02web.zoom.us/rec/download/gdSGrUaF5CjgrK5F0ZCKyOLrtbRfQievme2yBsyjFiiY4eQph18zgMx32Fi_Fx3CW8tEXrj0eLfPVGH8.U_rNDvZnaVipBrt5', 'file_extension': 'M4A', 'file_size': 514068, 'file_type': 'M4A', 'id': 'fe3ddd50-a375-434f-be41-dcac3b4d9f45', 'meeting_id': 'gcTWlKyET4q4oXRJCNZ7eA==', 'play_url': 'https://us02web.zoom.us/rec/play/gdSGrUaF5CjgrK5F0ZCKyOLrtbRfQievme2yBsyjFiiY4eQph18zgMx32Fi_Fx3CW8tEXrj0eLfPVGH8.U_rNDvZnaVipBrt5', 'recording_end': '2022-11-06T20:27:30Z', 'recording_start': '2022-11-06T20:26:57Z', 'recording_type': 'audio_only', 'status': 'completed'}], 'recording_play_passcode': '', 'share_url': 'https://us02web.zoom.us/rec/share/GZLsmxp2V4ybkqBsy2Zr6jbeTRIEQisY7_oJW4SmicSYoWjaFdrpEFiMpPrVNxd_.wfQ49_azsK2qFxaN', 'start_time': '2022-11-06T20:26:56Z', 'timezone': 'America/Los_Angeles', 'topic': 'G3-Own Your Sovereignty', 'total_size': 1196717, 'type': 8, 'uuid': 'gcTWlKyET4q4oXRJCNZ7eA=='}
    zoom_meeting_data_is_downloaded = {'account_id': 'NKeXSYsCQumvGVXQMqbwRA', 'duration': 75, 'host_id': 'JI5dmsyfS2Sa06vQmSAH3Q', 'id': 89020763746, 'recording_count': 2, 'recording_files': [{'download_url': 'https://us02web.zoom.us/rec/download/zbA0WCws45UY0_8sDflw5se_fqH0JSp8mp-lLS3B2CURX1vG76r2ZaCHxav3XdyGJVbgY8s8JeGUHnqr.L8iEJ4bkxT8mdDEk', 'file_extension': 'MP4', 'file_size': 482905717, 'file_type': 'MP4', 'id': '113c0788-949a-400f-93eb-cf241e18d8b1', 'meeting_id': '8yLU4XQrQjC0AZn/AbU03w==', 'play_url': 'https://us02web.zoom.us/rec/play/zbA0WCws45UY0_8sDflw5se_fqH0JSp8mp-lLS3B2CURX1vG76r2ZaCHxav3XdyGJVbgY8s8JeGUHnqr.L8iEJ4bkxT8mdDEk', 'recording_end': '2022-11-06T20:15:06Z', 'recording_start': '2022-11-06T18:52:45Z', 'recording_type': 'shared_screen_with_gallery_view', 'status': 'completed'}, {'download_url': 'https://us02web.zoom.us/rec/download/3150ZVmq3CgA4Eea-qqUWTvhK0Ni6QY73-jMdAvbKa89RVtRHrkrcKsZs8NfUmK36DVxbq8L4flRfpdt.jKxE9bGxQNAvVQyv', 'file_extension': 'M4A', 'file_size': 72147259, 'file_type': 'M4A', 'id': '5400ec7c-62b2-4fe5-86a6-1c40b39e1d07', 'meeting_id': '8yLU4XQrQjC0AZn/AbU03w==', 'play_url': 'https://us02web.zoom.us/rec/play/3150ZVmq3CgA4Eea-qqUWTvhK0Ni6QY73-jMdAvbKa89RVtRHrkrcKsZs8NfUmK36DVxbq8L4flRfpdt.jKxE9bGxQNAvVQyv', 'recording_end': '2022-11-06T20:15:06Z', 'recording_start': '2022-11-06T18:52:45Z', 'recording_type': 'audio_only', 'status': 'completed'}], 'recording_play_passcode': '', 'share_url': 'https://us02web.zoom.us/rec/share/zYEyewkel5UmSHOLipCHXuH-oWGw6a-5yM-tbTsAkIaY7_IVjPxf1j3F_WdOHhU.NRS9JcYtGxj7OMlT', 'start_time': '2022-11-06T18:52:43Z', 'timezone': 'America/Los_Angeles', 'topic': 'G5-Tyranny Against Human Consciousness', 'total_size': 555052976, 'type': 8, 'uuid': '8yLU4XQrQjC0AZn/AbU03w=='}

    def setUp(self):
        shutil.copy(self.filepath_csv_original, self.filepath_csv_test)
        self.db = MetadataDB(self.filepath_csv_test)

    def tearDown(self):
        # Cleanup code: runs after each test
        pass

    def assert_files_equal(self, filepath_expected, filepath_actual):
        with open(filepath_expected, 'rb') as expected_file, open(filepath_actual, 'rb') as actual_file:
            self.assertEqual(expected_file.read(), actual_file.read(), "Expected and actual files differ")


    def assert_files_differ(self, filepath_expected, filepath_actual):
        with open(filepath_expected, 'rb') as expected_file, open(filepath_actual, 'rb') as actual_file:
            self.assertTrue(expected_file.read() != actual_file.read(), "Expected and actual files should differ")


    def test_open_and_save_does_not_change_the_file_contents(self):
        self.db.save()
        self.assert_files_equal(filepath_expected=self.filepath_csv_original, filepath_actual=self.filepath_csv_test)

    def test_open_search_and_save_does_not_change_the_file_contents(self):
        meeting_data = self.zoom_meeting_data_to_download
        result = self.db.find_csv_metadata_for(meeting_data)
        self.assertTrue(result.size == 1, "Meeting data should be found in the metadata")
        self.db.save()
        self.assert_files_equal(filepath_expected=self.filepath_csv_original, filepath_actual=self.filepath_csv_test)

    def test_open_update_and_save_changes_the_file_contents(self):
        meeting_data = self.zoom_meeting_data_to_download
        self.db.mark_as_downloaded(meeting_data)
        self.db.save()
        self.assert_files_differ(filepath_expected=self.filepath_csv_original, filepath_actual=self.filepath_csv_test)

    def test_meeting_exists_in_metadata(self):
        meeting_data = self.zoom_meeting_data_to_download
        result = self.db.is_meeting_present(meeting_data)
        self.assertTrue(result, "Meeting data should be found in the metadata")

    def test_meeting_is_to_be_deleted_true(self):
        meeting_data = self.zoom_meeting_data_to_delete
        result1 = self.db.find_csv_metadata_for(meeting_data)
        self.assertTrue(result1.size == 1, "Meeting data should be found in the metadata")
        result2 = self.db.is_meeting_to_be_deleted(meeting_data)
        self.assertTrue(result2, "Meeting should be marked as 'action delete' in the metadata")

    def test_meeting_is_to_be_deleted_false(self):
        meeting_data = self.zoom_meeting_data_to_download
        result1 = self.db.find_csv_metadata_for(meeting_data)
        self.assertTrue(result1.size == 1, "Meeting data should be found in the metadata")
        result2 = self.db.is_meeting_to_be_deleted(meeting_data)
        self.assertFalse(result2, "Meeting should NOT be marked as 'action delete' in the metadata")

    def test_meeting_has_been_downloaded_true(self):
        meeting_data = self.zoom_meeting_data_is_downloaded
        result1 = self.db.find_csv_metadata_for(meeting_data)
        self.assertTrue(result1.size == 1, "Meeting data should be found in the metadata")
        result2 = self.db.is_already_downloaded(meeting_data)
        self.assertTrue(result2, "Meeting should be marked as 'downloaded' in the metadata")

    def test_meeting_has_been_downloaded_false(self):
        meeting_data = self.zoom_meeting_data_to_download
        result1 = self.db.find_csv_metadata_for(meeting_data)
        self.assertTrue(result1.size == 1, "Meeting data should be found in the metadata")
        result2 = self.db.is_already_downloaded(meeting_data)
        self.assertFalse(result2, "Meeting should NOT be marked as 'downloaded' in the metadata")


if __name__ == '__main__':
    unittest.main()


# def assert_equals(expected, actual):
#     assert expected == actual, f"Test Failed:\nExpected: {expected}\nActual:   {actual}"
#
#
# def xtest_is_downloaded():
#     meeting_id = "890 2076 3746"
#     meeting_datetime = "06/11/2022 10:52:43"
#     result = manager.is_downloaded(meeting_id, meeting_datetime)
#     assert_equals(False, result)
#
# def test_is_listed_once_format_uk_true():
#     meeting_id = "890 2076 3746"
#     meeting_datetime_string = "06/11/2022 10:52:43"
#     datetime_format = "%d/%m/%Y %H:%M:%S"
#     meeting_datetime_obj = datetime.strptime(meeting_datetime_string, datetime_format)
#     result = manager.is_meeting_present(meeting_id, meeting_datetime_obj)
#     assert result is True
#
# def test_is_listed_once_format_us_true():
#     meeting_id = "890 2076 3746"
#     meeting_datetime_string = "11/06/2022 10:52:43"
#     datetime_format = "%m/%d/%Y %H:%M:%S"
#     meeting_datetime_obj = datetime.strptime(meeting_datetime_string, datetime_format)
#     result = manager.is_meeting_present(meeting_id, meeting_datetime_obj)
#     assert result is True
#
#
# test_is_listed_once_format_uk_true()