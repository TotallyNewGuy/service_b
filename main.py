import os
import asyncio
from quart import Quart
from quart_cors import cors
from dotenv import load_dotenv

from db_util import init_db
from csv_blueprint import csv_blueprint
from pub_sub_util import get_streaming_pull, get_subscriber, init_pub_sub_client


def create_app():
    load_dotenv()
    app = Quart(__name__)
    app = cors(app, allow_origin="*")
    app.register_blueprint(csv_blueprint)
    return app


app = create_app()


async def read_and_save():
    while True:
        streaming_pull_future = get_streaming_pull(get_subscriber())
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=5.0)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
        await asyncio.sleep(1)


@app.before_serving
async def start_background_task():
    init_db()
    init_pub_sub_client()
    # Start the background task
    asyncio.create_task(read_and_save())


@app.route("/")
async def hello_world():
    return "<p>Hello World from Service B!</p>"


if __name__ == "__main__":
    # Run the Quart(Flask) app when run this Python file
    DEBUG = os.getenv("DEBUG").lower() == 'true'
    port = int(os.getenv("PORT", 8080))
    print(f"listening post: {port}")
    app.run(debug=DEBUG, port=port, host="0.0.0.0")