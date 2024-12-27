
import logging
import os
import subprocess
import time
from typing import Dict

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def download_cluster_files(cluster_urls: Dict[str, str], output_dir: str = "cluster_files") -> None:
    """
    Download all cluster.idx files using `wget`, but skip files that already exist.

    :param cluster_urls: Dictionary where keys are version names and values are URLs to download.
    :param output_dir: Directory where the downloaded files will be saved.
    """
    os.makedirs(output_dir, exist_ok=True)

    for version, url in cluster_urls.items():
        output_file = os.path.join(output_dir, f"{version}_cluster.idx")

        if os.path.exists(output_file):
            logger.info(f"File already exists, skipping download: {output_file}")
            continue

        logger.info(f"Downloading {url} -> {output_file}")

        # Use wget to download the file directly to disk
        try:
            subprocess.run([
                "wget", "-q", "-O", output_file, url
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to download {url}: {e}")
        time.sleep(2)

def combine_cluster_files(cluster_urls: Dict[str, str], output_dir: str = "cluster_files", combined_output: str = "combined_cluster.idx") -> None:
    """
    Combine all cluster.idx files into one output file, appending the cluster version.
    Then sort the combined file by url_prefix (column 2) and timestamp (column 3) in descending order.

    :param cluster_urls: Dictionary where keys are version names and values are URLs to download.
    :param output_dir: Directory containing the downloaded cluster.idx files.
    :param combined_output: Path to the combined output file.
    """
    temp_combined = combined_output + ".tmp"

    output_file = f"{output_dir}/{combined_output}"

    if os.path.exists(output_file):
        logger.info(f"Combined file {output_file} already exists, skipping combining and sorting.")
        return

    # Combine all cluster files with the cluster version prepended
    with open(temp_combined, "w") as temp_file:
        for version, _ in cluster_urls.items():
            full_input_path = os.path.join(output_dir, f"{version}_cluster.idx")
            if os.path.exists(full_input_path):
                subprocess.run([
                    "awk", f"{{print \"{version} \" \t $0}}", full_input_path
                ], stdout=temp_file, check=True)

    # Sort the combined file by url_prefix (column 2) and timestamp (column 3) in descending order
    sorted_combined = combined_output + ".sorted"
    subprocess.run([
        "sort", "-k2,2r", "-k3,3r", temp_combined, "-o", sorted_combined
    ], check=True)

    # Move the sorted file to the final output path
    subprocess.run(["mv", sorted_combined, output_file], check=True)
    os.remove(temp_combined)

    logger.info(f"Combined and sorted cluster files into {output_file}")


if __name__ == "__main__":
    # Step 1: Download all cluster.idx files
    logger.info("Step 1: Downloading cluster.idx files.")
    download_cluster_files(
        config.CLUSTER_IDX_URLS,
        output_dir=config.config["CLUSTER_DIR"]
    )

    # Step 2: Combine all cluster.idx files into one
    logger.info("Step 2: Combining cluster.idx files into one.")

    combine_cluster_files(
        config.CLUSTER_IDX_URLS,
        output_dir=config.config["CLUSTER_DIR"],
        combined_output=config.config["COMBINED_CLUSTER_FILE"],
    )
