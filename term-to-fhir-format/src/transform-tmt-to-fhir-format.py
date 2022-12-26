import pandas as pd
import xlrd
import json
import requests
from utils import (
    sub_record_to_fhir_format,
    vtm_record_to_fhir_format,
    gp_record_to_fhir_format,
    gpu_record_to_fhir_format,
    tp_record_to_fhir_format,
    tpu_record_to_fhir_format,
    unzip_file,
    save_to_ndjson,
    load_google_service_account_key,
    init_storage_client,
    upload_blob_to_gcp_storage,
)

fhir_url = "https://fhirterm.sil-th.org/fhir-server/api/v4/$import"
release_file_name = "TMTRF20221017.zip"
release_date_str = release_file_name.replace("TMTRF", "").replace(".zip", "")
release_file_path = f"./data/{release_file_name}"
release_extracted_dir = f"./data/TMTRF{release_date_str}"

# Extract release
unzip_file(release_file_path, release_extracted_dir)

# Transform release to CodeSystem
sub_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Concept/SUBS{release_date_str}.xls"
vtm_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Concept/VTM{release_date_str}.xls"
gp_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Concept/GP{release_date_str}.xls"
gpu_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Concept/GPU{release_date_str}.xls"
tp_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Concept/TP{release_date_str}.xls"
tpu_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Concept/TPU{release_date_str}.xls"
gpu_to_tpu_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Relationship/GPUtoTPU{release_date_str}.xls"
tp_to_tpu_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Relationship/TPtoTPU{release_date_str}.xls"
sub_to_vtm_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Relationship/SUBStoVTM{release_date_str}.xls"
vtm_to_gp_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Relationship/VTMtoGP{release_date_str}.xls"
gp_to_gpu_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Relationship/GPtoGPU{release_date_str}.xls"
gp_to_tp_file_path = f"./data/TMTRF{release_date_str}/TMTRF{release_date_str}_BONUS/Relationship/GPtoTP{release_date_str}.xls"

sub = pd.read_excel(sub_file_path, dtype=str)
vtm = pd.read_excel(vtm_file_path, dtype=str)
gp = pd.read_excel(gp_file_path, dtype=str)
gpu = pd.read_excel(gpu_file_path, dtype=str)
tp = pd.read_excel(tp_file_path, dtype=str)
tpu = pd.read_excel(tpu_file_path, dtype=str)

sub_records = sub.to_dict(orient="records")
vtm_records = vtm.to_dict(orient="records")
gp_records = gp.to_dict(orient="records")
gpu_records = gpu.to_dict(orient="records")
tp_records = tp.to_dict(orient="records")
tpu_records = tpu.to_dict(orient="records")


sub_to_vtm = pd.read_excel(sub_to_vtm_file_path, dtype=str)
vtm_to_gp = pd.read_excel(vtm_to_gp_file_path, dtype=str)
gp_to_gpu = pd.read_excel(gp_to_gpu_file_path, dtype=str)
gp_to_tp = pd.read_excel(gp_to_tp_file_path, dtype=str)
gpu_to_tpu = pd.read_excel(gpu_to_tpu_file_path, dtype=str)
tp_to_tpu = pd.read_excel(tp_to_tpu_file_path, dtype=str)

parent_gpu_of_tpu = {row["TPUID"]: row["GPUID"] for _, row in gpu_to_tpu.iterrows()}
parent_tp_of_tpu = {row["TPUID"]: row["TPID"] for _, row in tp_to_tpu.iterrows()}
parent_sub_of_vtm = {row["VTMID"]: row["SUBSID"] for _, row in sub_to_vtm.iterrows()}
parent_vtm_of_gp = {row["GPID"]: row["VTMID"] for _, row in vtm_to_gp.iterrows()}
parent_gp_of_gpu = {row["GPUID"]: row["GPID"] for _, row in gp_to_gpu.iterrows()}
parent_gp_of_tp = {row["TPID"]: row["GPID"] for _, row in gp_to_tp.iterrows()}

childs_vtm_of_sub = sub_to_vtm.groupby("SUBSID")["VTMID"].apply(list).to_dict()
childs_gp_of_vtm = vtm_to_gp.groupby("VTMID")["GPID"].apply(list).to_dict()
childs_gpu_of_gp = gp_to_gpu.groupby("GPID")["GPUID"].apply(list).to_dict()
childs_tp_of_gp = gp_to_tp.groupby("GPID")["TPID"].apply(list).to_dict()
childs_tpu_of_gpu = gpu_to_tpu.groupby("GPUID")["TPUID"].apply(list).to_dict()
childs_tpu_of_tp = tp_to_tpu.groupby("TPID")["TPUID"].apply(list).to_dict()

concept_sub = [sub_record_to_fhir_format(r, childs_vtm_of_sub) for r in sub_records]
concept_vtm = [
    vtm_record_to_fhir_format(r, parent_sub_of_vtm, childs_gp_of_vtm)
    for r in vtm_records
]
concept_gp = [
    gp_record_to_fhir_format(r, parent_vtm_of_gp, childs_gpu_of_gp, childs_tp_of_gp)
    for r in gp_records
]
concept_gpu = [
    gpu_record_to_fhir_format(r, parent_gp_of_gpu, childs_tpu_of_gpu)
    for r in gpu_records
]
concept_tp = [
    tp_record_to_fhir_format(r, parent_gp_of_tp, childs_tpu_of_tp) for r in tp_records
]
concept_tpu = [
    tpu_record_to_fhir_format(r, parent_gpu_of_tpu, parent_tp_of_tpu)
    for r in tpu_records
]

concept = [
    *concept_sub,
    *concept_vtm,
    *concept_gp,
    *concept_gpu,
    *concept_tp,
    *concept_tpu,
]

f = open("./config/tmt-base.json")
data = json.load(f)
data['id'] = f"TMTRF{release_date_str}"
data['version'] = release_date_str
data['concept'] = concept

# Save to ndjson
ndjson_path = f"./data/TMTRF{release_date_str}.ndjson"
save_to_ndjson(data, ndjson_path)

# Uplaod to Google Cloud Storage
bucket_name = "this-th-fhir-bulk-data"
object_name = f"TMTRF{release_date_str}.ndjson"
gcloud_credentials = load_google_service_account_key("./gcloud-sa.json")
storage_client = init_storage_client(gcloud_credentials)
upload_blob_to_gcp_storage(storage_client, bucket_name, ndjson_path, object_name)

# Trigger FHIR import
# headers = {"Content-type": "application/json"}
# result = requests.post(
#     url=fhir_url,
#     data=json.dumps(data, ensure_ascii=False).encode("utf-8"),
#     headers=headers,
# )
