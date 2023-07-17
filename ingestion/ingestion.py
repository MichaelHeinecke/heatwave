# Adapted from https://developer.dataplatform.knmi.nl/example-scripts

# TODO 1: Check if it's the right dataset
# TODO 2: Read in file name of last ingested file to retrieve newer ones
# TODO 3: Write out last ingested filename
import logging
import os
import sys
from datetime import datetime

import requests

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

api_url = "https://api.dataplatform.knmi.nl/open-data"
api_version = "v1"


def main():
    # Parameters
    api_key = os.getenv("API_KEY")
    dataset_name = "Actuele10mindataKNMIstations"
    dataset_version = "2"
    max_keys = "10"

    # Use list files request to request first 10 files of the day.
    timestamp = datetime.utcnow().date().strftime("%Y%m%d")
    start_after_filename_prefix = f"KMDS__OPER_P___10M_OBS_L2_{timestamp}"
    list_files_response = requests.get(
        f"{api_url}/{api_version}/datasets/{dataset_name}/versions/{dataset_version}/files",
        headers={"Authorization": api_key},
        params={"maxKeys": max_keys, "startAfterFilename": start_after_filename_prefix},
    )
    list_files = list_files_response.json()

    logger.info(f"List files response:\n{list_files}")
    dataset_files = list_files.get("files")

    # Retrieve first file in the list files response
    filename = dataset_files[0].get("filename")
    logger.info(f"Retrieve file with name: {filename}")
    endpoint = (
        f"{api_url}/{api_version}/datasets/{dataset_name}"
        + f"/versions/{dataset_version}/files/{filename}/url"
    )
    get_file_response = requests.get(endpoint, headers={"Authorization": api_key})
    if get_file_response.status_code != 200:
        logger.error("Unable to retrieve download url for file")
        logger.error(get_file_response.text)
        sys.exit(1)

    download_url = get_file_response.json().get("temporaryDownloadUrl")
    download_file_from_temporary_download_url(download_url, filename)


def download_file_from_temporary_download_url(download_url, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)

    logger.info(f"Successfully downloaded dataset file to {filename}")


if __name__ == "__main__":
    main()
