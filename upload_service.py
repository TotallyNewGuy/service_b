import csv
import traceback
from quart import flash, request, redirect
from werkzeug.utils import secure_filename

from db_service import save_csv_to_db, update_schd_headway

ALLOWED_EXTENSIONS = {"csv"}


async def read_csv_file():
    if request.method == "POST":
        # check if the post request has the file part
        async_file = await request.files
        if "file" not in async_file:
            flash("No file part")
            return redirect(request.url)
        file = async_file["file"]

        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == "":
            flash("No selected file")
            return redirect(request.url)
        if file and _allowed_file(file.filename):
            filename = secure_filename(file.filename)
            return file
        else:
            return "Invalid file", 400
    return None


async def save_train_csv_file(file):
    try:
        # todo use pandas read_csv for this? https://stackoverflow.com/questions/22604564/create-pandas-dataframe-from-a-string
        byte_csv = file.read()
        # creates list of strings, one for each row of csv
        data: list[str] = byte_csv.decode("utf-8").splitlines()
        # converts each row into a dict
        reader = csv.DictReader(data, delimiter=",")
        # put all dicts into a list (list of rows)
        all_entries = list(reader)

        await save_csv_to_db(all_entries)

        # calculate schd_headway for each trip only after a new csv is uploaded
        await update_schd_headway()

        return (True, "Successfully updating")

    except Exception as e:
        error_msg = traceback.format_exc()
        return (False, error_msg)


def _allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS