Waldo's challenge
=================

To download all image files just execute `./exif.py`. It'll save the image
filename and the exif tags, as key/value pair respectively in a sqlite3
`files.db`.

The schema of the database is the following:

    create table photos (
    id integer primary key autoincrement,
    filename text not null,
    exif text not null);

So, if you need to query the exif value of a file, you just have to
provide an image filename in the where clause of a select sentence.
