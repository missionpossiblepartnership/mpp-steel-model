import os
import re
import datetime
import git

from zipfile import ZipFile
from tqdm import tqdm

from mppsteel.config.model_config import DATETIME_FORMAT, OUTPUT_FOLDER
from mppsteel.config.model_scenarios import MAIN_SCENARIO_RUNS
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


DATE_REGEX_PATTERN = r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
TIME_REGEX_PATTERN = r"[0-9]{2}-[0-9]{2}"

def list_all_folders_in_directory(path_to_dir: str, subset_string: str = None) -> list:
    all_folders = [x[0] for x in os.walk(path_to_dir)]
    if subset_string:
        return [x for x in all_folders if subset_string in x]
    return all_folders

def remove_folder_paths(folder_list: list, subpaths_to_remove: str) -> list:
    return [path for path in folder_list if os.path.basename(os.path.normpath(path)) != subpaths_to_remove]

def include_folder_paths(folder_list: list, subpaths_to_include: str) -> list:
    return [path for path in folder_list if subpaths_to_include in os.path.basename(os.path.normpath(path)).split(' ')[0]]

def get_last_modified_folder(folder_list: list) -> str:
    return max(folder_list, key=os.path.getmtime)

def get_files_with_ext(path_to_dir: str, ext: str, return_full_path: bool = False) -> list:
    all_filenames = os.listdir(path_to_dir)
    relevant_files = [filename for filename in all_filenames if filename.endswith(ext)]
    if return_full_path:
        return [os.path.join(path_to_dir, filename) for filename in relevant_files]
    return relevant_files

def get_current_sha() -> str:
    repo = git.Repo(search_parent_directories=True)
    return repo.head.object.hexsha

def clean_folders(folder_list: list, subpaths_to_remove: str = None, subpaths_to_include: str = None) -> list:
    cleaned_folders = remove_folder_paths(folder_list, subpaths_to_remove)
    return include_folder_paths(cleaned_folders, subpaths_to_include)

def clean_container_string(str_to_clean: str) -> str:
    return str_to_clean.replace(' ', '-').replace('_', '-').lower()


def get_date_and_time(path_to_dir: str, use_current_date: bool = False, include_sha: bool = False, scenario: str = ''):
    date_and_time = datetime.datetime.now().strftime(DATETIME_FORMAT)
    if not use_current_date: 
        date_match = re.findall(DATE_REGEX_PATTERN, path_to_dir)
        time_match = re.findall(TIME_REGEX_PATTERN, path_to_dir)
        date_and_time = f"{date_match[0]} {time_match[-1]}"
    if scenario:
        date_and_time = f"{scenario} {date_and_time}"
    if include_sha:
        date_and_time = f"{date_and_time} {get_current_sha()}"
    return clean_container_string(date_and_time)


all_filepaths = list_all_folders_in_directory(OUTPUT_FOLDER)

def create_scenario_metadata(scenario_list: list = MAIN_SCENARIO_RUNS) -> dict:
    scenario_file_dict = {}
    for scenario in tqdm(scenario_list, total=len(scenario_list), desc="Scenario Metadata"):
        cleaned_folders = clean_folders(all_filepaths, 'graphs', scenario)
        last_modified_folder = get_last_modified_folder(cleaned_folders)
        files_in_folder = get_files_with_ext(last_modified_folder, 'csv', True)
        new_container_name = get_date_and_time(
            path_to_dir=last_modified_folder,
            use_current_date=False,
            include_sha=False,
            scenario=scenario
        )
        scenario_file_dict[scenario] = {
            "container_name": new_container_name,
            "last_modified_folder": last_modified_folder,
            "files_to_upload": files_in_folder
        }
    return scenario_file_dict

def upload_to_container(blob_service_client, container_name, local_file_name) -> None:
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
    # Upload the created file
    with open(local_file_name, "rb") as data:
        blob_client.upload_blob(data)
    return None

def create_zipped_file(list_of_files: list, zipped_file_name: str):
    # Create a ZipFile Object
    with ZipFile(f"{zipped_file_name}.zip", "w") as zip_object:
        # Add multiple files to the zip
        for file in list_of_files:
            zip_object.write(file)
