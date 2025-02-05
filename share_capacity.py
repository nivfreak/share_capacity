import argparse
import csv
import datetime
import json
import os
import yaml

import qumulo.lib.auth as auth
import qumulo.lib.request as request
import qumulo.rest.smb as smb
import qumulo.rest.nfs as nfs
import qumulo.rest.s3 as s3
import qumulo.rest.fs as fs

def load_config(config_file=".config"):
    """
    Loads configuration from a YAML file.
    The file should contain keys 'host' and 'access_token'.
    Returns a dictionary (or empty dict if file not found).
    """
    config = {}
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            config = yaml.safe_load(f) or {}
    return config

def get_connection(host, access_token):
    """
    Return an authenticated Qumulo connection using the provided host and access token.
    See https://docs.qumulo.com/administrator-guide/connecting-to-external-services/creating-using-access-tokens-to-authenticate-external-services-qumulo-core.html
    """
    creds = auth.Credentials(bearer_token=access_token)
    return request.Connection(host=host, port=8000, credentials=creds)

def get_smb_shares(conn):
    response = smb.smb_list_shares(conn, conn.credentials)
    return response.data["entries"]

def get_nfs_exports(conn):
    response = nfs.nfs_list_exports(conn, conn.credentials)
    return response.data["entries"]

def get_s3_buckets(conn):
    s3_client = s3.S3(conn)
    bucket_list_obj = s3_client.list_buckets()
    return bucket_list_obj.buckets

def get_directory_capacity(conn, path):
    """
    Retrieves the capacity usage (in GB) for the given directory path.
    Calls fs.read_dir_aggregates and extracts the 'total_capacity' field.
    """
    token = conn.credentials.bearer_token
    response = fs.read_dir_aggregates(conn, token, path)
    return float(response.data["total_capacity"]) / 1e9

def get_cluster_stats(conn):
    """
    Retrieves the free cluster space and total (usable) cluster space (both in GB).
    """
    token = conn.credentials.bearer_token
    response = fs.read_fs_stats(conn, token)
    free_space = float(response.data["free_size_bytes"]) / 1e9
    usable_space = float(response.data["total_size_bytes"]) / 1e9
    return free_space, usable_space

def get_user_metadata(conn, path):
    """
    Retrieves all user-metadata entries for the given path and returns them as a dict.
    We'll also decode bytes->str so json.dumps() won't fail.
    """
    token = conn.credentials
    metadata_iter = fs.list_user_metadata(conn, token, path=path)
    tags = {}

    for page in metadata_iter:
        # 'page' should be a RestResponse
        if not hasattr(page, "data") or not isinstance(page.data, dict):
            continue

        entries = page.data.get("entries", [])
        for record in entries:
            key = record.get("key")
            value = record.get("value")

            # If value is bytes, convert it to a UTF-8 string (or use another strategy if needed).
            if isinstance(value, bytes):
                try:
                    value = value.decode("utf-8")
                except UnicodeDecodeError:
                    # If it fails, maybe fallback to a representation or 'replace' errors
                    value = value.decode("utf-8", errors="replace")

            if key is not None:
                tags[key] = value

    return tags

def process_exposure_item(item, protocol, exposure_type, name_key, fs_path_key, free_space, usable_space, conn):
    """
    Processes a single exposure item (share, export, or bucket) using the given key names.
    For dictionary items (SMB, NFS), keys are looked up via item[key]. For objects (S3),
    keys are accessed via getattr(item, key).
    Returns a list:
    [protocol, exposure_type, name, fs_path, used_space, free_space, usable_space, used_pct, tags_dict]
    """
    if isinstance(item, dict):
        name = item[name_key]
        fs_path = item[fs_path_key]
    else:
        name = getattr(item, name_key)
        fs_path = getattr(item, fs_path_key)

    used_space = get_directory_capacity(conn, fs_path)
    used_pct = (used_space / (used_space + free_space)) * 100 if free_space > 0 else 0
    tags_dict = get_user_metadata(conn, fs_path)

    return [protocol, exposure_type, name, fs_path, used_space, free_space, usable_space, used_pct, tags_dict]

def write_csv_enumerated(data_rows, date_str, output_filename):
    """
    Writes the enumerated (non-deduplicated) data rows to CSV.
    Numeric values are rounded to three decimal places.
    Each row is expected to be in the format:
    [protocol, exposure_type, name, fs_path, used_space, free_space, usable_space, used_pct, tags_dict]
    """
    with open(output_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Date",
            "Protocol",
            "Exposure Type",
            "Exposure Name",
            "Filesystem Path",
            "Path Used Space (GB)",
            "Cluster Free Space (GB)",
            "Cluster Usable Space (GB)",
            "Used %",
            "tags"
        ])
        for row in data_rows:
            protocol, exposure_type, name, fs_path, used_space, free_space, usable_space, used_pct, tags_dict = row
            writer.writerow([
                date_str,
                protocol,
                exposure_type,
                name,
                fs_path,
                f"{used_space:.3f}",
                f"{free_space:.3f}",
                f"{usable_space:.3f}",
                f"{used_pct:.3f}",
                json.dumps(tags_dict)
            ])

def write_csv_dedup(dedup_rows, date_str, output_filename):
    """
    Writes the deduplicated data rows to CSV.
    Each row represents one unique filesystem path, with an extra column 'Exposures'
    listing the exposures (e.g. "SMB:Share:Name, NFS:Export:Name") associated with that path.
    """
    with open(output_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Date",
            "Filesystem Path",
            "Path Used Space (GB)",
            "Cluster Free Space (GB)",
            "Cluster Usable Space (GB)",
            "Used %",
            "Exposures",
            "tags"
        ])
        for row in dedup_rows:
            writer.writerow([
                date_str,
                row["fs_path"],
                f"{row['used_space']:.3f}",
                f"{row['free_space']:.3f}",
                f"{row['usable_space']:.3f}",
                f"{row['used_pct']:.3f}",
                ", ".join(row["exposures"]),
                json.dumps(row["tags"])
            ])

def deduplicate_rows(data_rows):
    """
    Deduplicates exposure rows by the filesystem path.
    For rows with the same path, assumes used_space, free_space, usable_space, and used_pct are the same,
    and aggregates the exposure information into a list.
    Returns a list of dictionaries with keys:
    fs_path, used_space, free_space, usable_space, used_pct, exposures, tags.
    """
    dedup = {}
    for row in data_rows:
        protocol, exposure_type, name, fs_path, used_space, free_space, usable_space, used_pct, tags_dict = row
        exposure = f"{protocol}:{exposure_type}:{name}"
        if fs_path in dedup:
            dedup[fs_path]["exposures"].append(exposure)
        else:
            dedup[fs_path] = {
                "fs_path": fs_path,
                "used_space": used_space,
                "free_space": free_space,
                "usable_space": usable_space,
                "used_pct": used_pct,
                "exposures": [exposure],
                "tags": tags_dict
            }
    return list(dedup.values())

def main():
    parser = argparse.ArgumentParser(description="Generate Qumulo exposure report.")
    parser.add_argument("--enumerate-exposures", action="store_true",
                        help="Output one exposure per CSV line (do not deduplicate by path).")
    parser.add_argument("--host", help="Qumulo cluster host")
    parser.add_argument("--access-token", help="Qumulo access token")
    args = parser.parse_args()

    # Load configuration from .config if available.
    config = load_config()
    host = args.host if args.host else config.get("host")
    access_token = args.access_token if args.access_token else config.get("access_token")
    if not host or not access_token:
        parser.error("Host and access token must be provided via command line or .config file.")

    conn = get_connection(host, access_token)
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    base_filename = f"qumulo_exposure_report_{today}"
    # Deduplication is the default. If enumerate-exposures is specified, output the enumerated report.
    if args.enumerate_exposures:
        output_csv = f"{base_filename}_enumerated.csv"
    else:
        output_csv = f"{base_filename}_dedup.csv"

    free_space, usable_space = get_cluster_stats(conn)
    data_rows = []

    # Process SMB shares
    smb_shares = get_smb_shares(conn)
    for share in smb_shares:
        row = process_exposure_item(
            share,
            protocol="SMB",
            exposure_type="Share",
            name_key="share_name",
            fs_path_key="fs_path",
            free_space=free_space,
            usable_space=usable_space,
            conn=conn
        )
        data_rows.append(row)

    # Process NFS exports
    nfs_exports = get_nfs_exports(conn)
    for export in nfs_exports:
        row = process_exposure_item(
            export,
            protocol="NFS",
            exposure_type="Export",
            name_key="export_path",
            fs_path_key="fs_path",
            free_space=free_space,
            usable_space=usable_space,
            conn=conn
        )
        data_rows.append(row)

    # Process S3 buckets
    s3_buckets = get_s3_buckets(conn)
    for bucket in s3_buckets:
        row = process_exposure_item(
            bucket,
            protocol="S3",
            exposure_type="Bucket",
            name_key="name",
            fs_path_key="path",
            free_space=free_space,
            usable_space=usable_space,
            conn=conn
        )
        data_rows.append(row)

    # CSV output
    if args.enumerate_exposures:
        write_csv_enumerated(data_rows, today, output_csv)
    else:
        dedup_rows = deduplicate_rows(data_rows)
        write_csv_dedup(dedup_rows, today, output_csv)

    print(f"Report written to {output_csv}")

    count_smb = len(smb_shares)
    count_nfs = len(nfs_exports)
    count_s3 = len(s3_buckets)
    total = count_smb + count_nfs + count_s3
    print(f"Summary: Processed {count_smb} SMB shares, {count_nfs} NFS exports, and {count_s3} S3 buckets "
          f"({total} total exposures).")


if __name__ == "__main__":
    main()
