import shutil
import mutagen
from mutagen.mp4 import MP4, MP4Cover

# mutagen.File knows how to open any file (works with both MP4 and M4A):
# https://mutagen.readthedocs.io/en/latest/user/gettingstarted.html
# https://mutagen.readthedocs.io/en/latest/api/base.html#mutagen.File

class Keys:
    TITLE = "title"
    ARTIST = "artist"
    ALBUM_ARTIST = "albumartist"
    ALBUM = "album"
    DATE = "date"
    TRACK_NUMBER = "tracknumber"
    GENRE = "genre"
    COMMENT = "comment"
    COPYRIGHT = "copyright"

audio_original = "./media/EN_Bk01_Ch11-12_20221106-1852.m4a"
video_original = "./media/EN_Bk01_Ch11-12_20221106-1852.mp4"
audio_copy = "./media/copy_audio.m4a"
video_copy = "./media/copy_video.mp4"

def setup():
    shutil.copy(audio_original, audio_copy)
    shutil.copy(video_original, video_copy)

def add_metadata_to_media_file(file_path, title, artist, album_artist, album, date, track_number, num_tracks, genre, comment, copy_right):
    with open(file_path, 'r+b') as file:
        audio_file = mutagen.File(file, easy=True)

        print('before:', audio_file.pprint(), end='\n\n')

        audio_file[Keys.TITLE] = title
        audio_file[Keys.ARTIST] = artist
        audio_file[Keys.ALBUM_ARTIST] = album_artist
        audio_file[Keys.ALBUM] = album
        audio_file[Keys.DATE] = date
        audio_file[Keys.TRACK_NUMBER] = f"{track_number}/{num_tracks}"
        audio_file[Keys.GENRE] = genre
        audio_file[Keys.COMMENT] = comment
        audio_file[Keys.COPYRIGHT] = copy_right
        audio_file.save(file)

        print('after:', audio_file.pprint(), end='\n\n')
        print(type(audio_file), type(audio_file.tags), end='\n\n')

def add_cover_image_to_media_file(media_file_path, cover_image_path):
    audio_file = MP4(media_file_path)
    with open(cover_image_path, "rb") as f:
        audio_file["covr"] = [
            MP4Cover(f.read(), imageformat=MP4Cover.FORMAT_JPEG)
        ]
    audio_file.save()


def add_metadata_to_ksp_media_file(media_file_path, track_title, readers, book_title, recording_year, track_number,
                                   num_tracks, language, cover_image_path):
    author = "Kaleb Seth Perl"
    genre = "Spoken Word"
    add_metadata_to_media_file(media_file_path, track_title, readers, author, book_title, recording_year,
                               track_number, num_tracks, genre, language, author)
    add_cover_image_to_media_file(media_file_path, cover_image_path)


def test_01():
    setup()

    track_title = "Ch01"
    readers = "Tan Zaman"
    book_title = "Tyranny Against Human Consciousness"
    recording_year = "2023"
    track_number = 1
    num_tracks = 22
    language = "English (EN)"
    cover_image = "./media/KSP Logo small.jpeg"

    add_metadata_to_ksp_media_file(audio_copy, track_title, readers, book_title, recording_year, track_number, num_tracks, language, cover_image)
    # add_metadata_to_ksp_media_file(video_copy, track_title, readers, book_title, recording_year, track_number, num_tracks, language, cover_image)

test_01()