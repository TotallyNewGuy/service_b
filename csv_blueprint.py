from quart import Blueprint

from upload_service import read_csv_file, save_train_csv_file

# create a blueprint for url prefix
csv_blueprint = Blueprint('csv_api', __name__, url_prefix='/csv')

@csv_blueprint.route('/', methods=['POST'])
async def upload_schedule():
    file = await read_csv_file()
    successful, data = await save_train_csv_file(file)
    if not successful:
        return str(data), 500
    return data, 200


@csv_blueprint.route('/upload/')
async def train_csv_upload_page():
    return """
        <!doctype html>
        <title>Upload csv file</title>
        <h1>Upload Train CSV File</h1>
        <form action="/csv/" method=post enctype=multipart/form-data>
        <input type=file name=file>
        <input type=submit value=Upload>
        </form>
    """