import os
import numpy as np
import dask
from dask import delayed
from dask.distributed import Client, LocalCluster
from Bio.PDB.MMCIFParser import MMCIFParser
import pandas as pd
import time

def calculate_contact_order(cif_file, distance_cutoff=8.0):
    """
    Calculate contact order for a protein structure in a CIF file.
    Checks if the file is empty or contains valid data.

    Parameters:
        cif_file (str): Path to the CIF file.
        distance_cutoff (float): Distance threshold (in Å) to define a contact.

    Returns:
        dict: Filename and contact order result.
    """
    parser = MMCIFParser(QUIET=True)

    if os.path.getsize(cif_file) == 0:
        return {"file": cif_file, "contact_order": None, "error": "File is empty"}

    try:
        structure = parser.get_structure("protein", cif_file)

        residues = []
        coordinates = []
        
        for model in structure:
            for chain in model:
                for residue in chain:
                    if 'CA' in residue:
                        residues.append(residue)
                        coordinates.append(residue['CA'].get_coord())

        if len(residues) == 0:
            return {"file": cif_file, "contact_order": None, "error": "No residues found in the file"}

        coordinates = np.array(coordinates)
        num_residues = len(residues)

        if num_residues < 2:
            return {"file": cif_file, "contact_order": None, "error": "Too few residues to calculate contact order"}

        distances = np.linalg.norm(coordinates[:, None, :] - coordinates[None, :, :], axis=-1)

        contacts = (distances < distance_cutoff) & (distances > 0)

        total_contact_order = 0
        num_contacts = 0

        for i in range(num_residues):
            for j in range(i + 1, num_residues):
                if contacts[i, j]:
                    sequence_distance = abs(i - j)
                    total_contact_order += sequence_distance
                    num_contacts += 1

        contact_order = total_contact_order / (num_residues * num_contacts) if num_contacts > 0 else 0

        return {"file": cif_file, "contact_order": contact_order, "error": None}

    except Exception as e:
        return {"file": cif_file, "contact_order": None, "error": str(e)}


def process_cif_files(file_list, distance_cutoff=8.0):
    tasks = [delayed(calculate_contact_order)(file, distance_cutoff) for file in file_list]
    results = dask.compute(*tasks)
    return results


if __name__ == "__main__":
    cluster = LocalCluster(
        n_workers=16,
        threads_per_worker=1,
        memory_limit="4GB"
    )

    client = Client(cluster)
    print("Dask cluster initialized.")

    scratch_directory = os.getenv("SCRATCH")
    cif_directory = f"{scratch_directory}/lsc_data/data"
    output_file = f"{scratch_directory}/lsc_data/contact_order_results.csv"
    output_log_file = f"{scratch_directory}/lsc_data/logs.csv"

    file_list = [os.path.join(cif_directory, f) for f in os.listdir(cif_directory) if f.endswith(".cif")]

    if not file_list:
        print("No CIF files found in the specified directory.")
    else:

        global_start_time = time.time()

        num_checkpoints = 100
        chunk_size = len(file_list) // num_checkpoints

        for checkpoint_idx in range(num_checkpoints):
            start_idx = checkpoint_idx * chunk_size
            end_idx = (checkpoint_idx + 1) * chunk_size if checkpoint_idx < num_checkpoints - 1 else len(file_list)

            start_time = time.time()
            results = process_cif_files(file_list[start_idx:end_idx], distance_cutoff=8.0)
            end_time = time.time()

            df = pd.DataFrame(results)
            df_logs = pd.DataFrame.from_dict(
                {
                    "start_idx": [start_idx],
                    "end_idx": [end_idx],
                    "start_time": [start_time],
                    "end_time": [end_time],
                    "checkpoint_duration": [end_time-start_time],
                    "global_duration": [end_time-global_start_time],
                    }
                )
            if checkpoint_idx==0:
                df.to_csv(output_file, index=False)
                df_logs.to_csv(output_log_file, index=False)
            else:
                df.to_csv(output_file, mode='a', index=False, header=False)
                df_logs.to_csv(output_log_file, mode='a', index=False, header=False)
            
            print(f"Checkpoint {checkpoint_idx+1}/{num_checkpoints}")
            print(f"Processing time: {end_time - start_time:.2f} seconds")
            print(f"Results saved to {output_file}, logs to {output_log_file}")
        global_end_time = time.time()
        
        print(f"Processing time: {global_end_time - global_end_time:.2f} seconds")
        print(f"Results saved to {output_file}")
        print("Processing completed successfully.")