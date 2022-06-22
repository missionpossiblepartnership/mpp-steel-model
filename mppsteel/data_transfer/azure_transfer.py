"""Script to intiate data transfer"""

from copy import deepcopy
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from mppsteel.data_transfer.transfer_functions import create_scenario_metadata, upload_to_container, create_zipped_file

from mppsteel.config.model_scenarios import MAIN_SCENARIO_RUNS
from mppsteel.config.model_config import COMBINED_OUTPUT_FOLDER_NAME

from tqdm import tqdm

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)

def create_and_load(blob_service_client, scenario_file_container: dict, scenario: str, zipped: bool = False) -> None:
    scenario_metadata = scenario_file_container[scenario]
    container_name = scenario_metadata["container_name"]
    files_to_upload = scenario_metadata["files_to_upload"]

    # Create the container
    try:
        blob_service_client.create_container(container_name)
    except:
        raise ValueError(f"Failed on {container_name}")

    # transfer the files
    if zipped:
        zipped_file = create_zipped_file(files_to_upload, container_name)
        upload_to_container(blob_service_client, container_name, zipped_file)
        logger.info(f"{scenario} Scenario: Successfully uploaded zipped folder to container")
    else:
        for filename in tqdm(files_to_upload, total=len(files_to_upload), desc=f"File upload for {scenario}"):
            upload_to_container(blob_service_client, container_name, filename)
        logger.info(f"{scenario} Scenario: Successfully uploaded all {len(files_to_upload)} file(s) to {container_name} container")

    return None


def create_container_and_load_data(blob_service_client, scenario_file_container: dict, chosen_scenario: str = '', zipped: bool = False) -> None:
    if chosen_scenario:
        create_and_load(scenario_file_container, chosen_scenario, zipped)
    else:
        for scenario in tqdm(scenario_file_container, total=len(scenario_file_container), desc="Scenario container and upload"):
            create_and_load(blob_service_client, scenario_file_container, scenario, zipped)
    return None


def load_blob_service_client(connect_str: str):
    return BlobServiceClient.from_connection_string(connect_str)


def full_transfer_flow(connect_str: str, include_combined_data: bool, zipped: bool):
    scenarios = deepcopy(MAIN_SCENARIO_RUNS)
    if include_combined_data:
        scenarios.append(f"{COMBINED_OUTPUT_FOLDER_NAME}")
    blob_service_client = load_blob_service_client(connect_str)
    scenario_metadata = create_scenario_metadata(MAIN_SCENARIO_RUNS)
    create_container_and_load_data(blob_service_client, scenario_metadata, zipped)
