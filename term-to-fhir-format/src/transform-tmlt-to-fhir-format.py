import pandas as pd
import xlrd
import requests
import json
from utils import (
    tmlt_record_to_fhir_format,
    unzip_file,
    save_to_ndjson,
    load_google_service_account_key,
    init_storage_client,
    upload_blob_to_gcp_storage,
)


fhir_url = "https://fhirterm.sil-th.org/fhir-server/api/v4/$import"
release_file_name = "TMLT20221003.zip"
release_date_str = release_file_name.replace("TMLT", "").replace(".zip", "")
release_file_path = f"./data/{release_file_name}"
release_extracted_dir = f"./data/TMLT{release_date_str}"

# Extract release
unzip_file(release_file_path, release_extracted_dir)

# Transform release to CodeSystem
snapshot_file_path = f"{release_extracted_dir}/TMLTRF{release_date_str}/TMLT_SNAPSHOT{release_date_str}.xls"
relation_file_path = f"{release_extracted_dir}/TMLTRF{release_date_str}_BONUS/Relationship/PANELtoITEM{release_date_str}.xls"
snapshot = pd.read_excel(snapshot_file_path, dtype=str)
relation = pd.read_excel(relation_file_path, dtype=str)
childs_item_of_panel = relation.groupby("TMLT_PANEL")["TMLT_ITEM"].apply(list).to_dict()
parent_panel_of_item = {
    row["TMLT_ITEM"]: row["TMLT_PANEL"] for i, row in relation.iterrows()
}
records = snapshot.to_dict(orient="records")
concept = [
    tmlt_record_to_fhir_format(r, childs_item_of_panel, parent_panel_of_item)
    for r in records
]
f = open("./config/tmlt-base.json")
data = json.load(f)
data['id'] = f"TMLT{release_date_str}"
data['version'] = release_date_str
data["concept"] = concept

# Save to ndjson
ndjson_path = f"./data/TMLT{release_date_str}.ndjson"
save_to_ndjson(data, ndjson_path)

# Uplaod to Google Cloud Storage
bucket_name = "this-th-fhir-bulk-data"
object_name = f"TMLT{release_date_str}.ndjson"
gcloud_credentials = load_google_service_account_key("./gcloud-sa.json")
storage_client = init_storage_client(gcloud_credentials)
upload_blob_to_gcp_storage(storage_client, bucket_name, ndjson_path, object_name)


# Trigger FHIR import
# headers = {'Content-type': 'application/json'}
# result = requests.post(
#     url=fhir_url,
#     data=json.dumps(data, ensure_ascii=False).encode('utf-8'),
#     headers=headers
# )
