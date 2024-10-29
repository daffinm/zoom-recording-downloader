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
audio_copy = "./media/audio_copy.m4a"
cover_image = "./media/KSP Logo small.jpeg"

num_tracks = 3

title = "Ch0?-0?"
artist = "Reader Man"
album_artist = "Kaleb Seth Perl"
album = "Book Title"
date = "2023"
track_number = 1
genre = "Spoken Word"
comment = "Language EN"
copy_right = "Kaleb Seth Perl"

def setup():
    shutil.copy(audio_original, audio_copy)

def add_easy_tags():
    with open(audio_copy, 'r+b') as file:
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

def add_cover_image():
    audio_file = MP4(audio_copy)
    with open(cover_image, "rb") as f:
        audio_file["covr"] = [
            MP4Cover(f.read(), imageformat=MP4Cover.FORMAT_JPEG)
        ]
    audio_file.save()

setup()
add_easy_tags()
add_cover_image()

