import pandas as pd
from datetime import datetime
import zipfile
import ndjson
from google.cloud import storage
from google.oauth2 import service_account


def format_date(date_string, original_format, target_format):
    return datetime.strftime(datetime.strptime(date_string, original_format), target_format)


def format_space(word):
    return " ".join(word.strip().split())


def unzip_file(zip_file_path, extrated_dir):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extrated_dir)


def save_to_ndjson(data, file_path):
    with open(file_path, 'w') as f:
        writer = ndjson.writer(f, ensure_ascii=False)
        writer.writerow(data)


def load_google_service_account_key(file_path):
    return service_account.Credentials.from_service_account_file(file_path) # /path/to/file.json


def init_storage_client(credentials):
    return storage.Client(credentials=credentials)


def upload_blob_to_gcp_storage(storage_client, bucket_name, source_file_path, destination_blob_path):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_path)

    blob.upload_from_filename(source_file_path)

    print(
        f"File {source_file_path} uploaded to {destination_blob_path}."
    )


def tmlt_record_to_fhir_format(tmlt_record, childs_item_of_panel, parent_panel_of_item):
    code = format_space(tmlt_record.get("TMLT_Code"))
    order_type = format_space(tmlt_record.get("ORDER_TYPE"))
    result = {
        'code': code,
        'display': format_space(tmlt_record.get('TMLT_Name')),
        'designation': [
            {
                'use': {
                    'system': 'http://snomed.info/sct',
                    'code': '900000000000003001',
                    'display': 'Fully specified name'
                },
                'value': format_space(tmlt_record.get('TMLT_Name'))
            }
        ]
    }
    property = [
        {'code': 'ORDER_TYPE', 'valueCode': order_type},
        {
            'code': 'VersionlastRelease',
            'valueDateTime': format_date(format_space(tmlt_record.get('VersionlastRelease')), '%Y%m%d', '%Y-%m-%d'),
        },
    ]

    if pd.notnull(tmlt_record.get('COMPONENT')):
        property.append({'code': 'COMPONENT', 'valueCode': format_space(tmlt_record.get('COMPONENT'))})
    if pd.notnull(tmlt_record.get('SCALE')):
        property.append({'code': 'SCALE', 'valueCode': format_space(tmlt_record.get('SCALE'))})
    if pd.notnull(tmlt_record.get('UNIT')):
        property.append({'code': 'UNIT', 'valueCode': format_space(tmlt_record.get('UNIT'))})
    if pd.notnull(tmlt_record.get('SPECIMEN')):
        property.append({'code': 'SPECIMEN', 'valueCode': format_space(tmlt_record.get('SPECIMEN'))})
    
    if order_type == 'PANEL':
        childs = childs_item_of_panel.get(code)
        if childs:
            childs_fhir_format = [{'code': 'child', 'valueCode': c} for c in childs]
            property = [*property, *childs_fhir_format]
    elif order_type == 'ITEM':
        parent = parent_panel_of_item.get(code)
        if parent:
            parent_fhir_format = {'code': 'parent', 'valueCode': parent}
            property.append(parent_fhir_format)
    
    result['property'] = property
    return result


def sub_record_to_fhir_format(record, childs_vtm_of_sub):
    code = format_space(record.get('TMTID(SUBS)'))
    property = [
            {
                'code': 'class',
                'valueCode': 'SUBS'
            },
            {
                'code': 'change_date',
                'valueDateTime': format_date(format_space(record.get('CHANGEDATE')), '%Y%m%d', '%Y-%m-%d')
            }
        ]
    
    childs = childs_vtm_of_sub.get(code)
    if childs:
        childs_fhir_format = [{'code': 'child', 'valueCode': c} for c in childs]
        property = [*property, *childs_fhir_format]
    
    return {
        'code': code,
        'display': format_space(record.get('FSN')),
        'designation': [
            {
                'use': {
                    'system': 'http://snomed.info/sct',
                    'code': '900000000000003001',
                    'display': 'Fully specified name'
                },
                'value': format_space(record.get('FSN'))
            }
        ],
        'property': property
    }


def vtm_record_to_fhir_format(record, parent_sub_of_vtm, childs_gp_of_vtm):
    code = format_space(record.get('TMTID(VTM)'))
    property = [
            {
                'code': 'class',
                'valueCode': 'VTM'
            },
            {
                'code': 'change_date',
                'valueDateTime': format_date(format_space(record.get('CHANGEDATE')), '%Y%m%d', '%Y-%m-%d')
            }
        ]
    
    childs = childs_gp_of_vtm.get(code)
    if childs:
        childs_fhir_format = [{'code': 'child', 'valueCode': c} for c in childs]
        property = [*property, *childs_fhir_format]
    
    parent_sub = parent_sub_of_vtm.get(code)
    if parent_sub:
        parent_fhir_format = {'code': 'parent', 'valueCode': parent_sub}
        property.append(parent_fhir_format)

    return {
        'code': code,
        'display': format_space(record.get('FSN')),
        'designation': [
            {
                'use': {
                    'system': 'http://snomed.info/sct',
                    'code': '900000000000003001',
                    'display': 'Fully specified name'
                },
                'value': format_space(record.get('FSN'))
            }
        ],
        'property': property
    }


def gp_record_to_fhir_format(record, parent_vtm_of_gp, childs_gpu_of_gp, childs_tp_of_gp):
    code = format_space(record.get('TMTID(GP)'))
    property = [
            {
                'code': 'class',
                'valueCode': 'GP'
            },
            {
                'code': 'change_date',
                'valueDateTime': format_date(format_space(record.get('CHANGEDATE')), '%Y%m%d', '%Y-%m-%d')
            }
        ]
    childs_gpu = childs_gpu_of_gp.get(code)
    if childs_gpu:
        childs_fhir_format = [{'code': 'child', 'valueCode': c} for c in childs_gpu]
        property = [*property, *childs_fhir_format]
    
    childs_tp = childs_tp_of_gp.get(code)
    if childs_tp:
        childs_fhir_format = [{'code': 'child', 'valueCode': c} for c in childs_tp]
        property = [*property, *childs_fhir_format]

    parent_vtm = parent_vtm_of_gp.get(code)
    if parent_vtm:
        parent_fhir_format = {'code': 'parent', 'valueCode': parent_vtm}
        property.append(parent_fhir_format)
    return {
        'code': code,
        'display': format_space(record.get('FSN')),
        'designation': [
            {
                'use': {
                    'system': 'http://snomed.info/sct',
                    'code': '900000000000003001',
                    'display': 'Fully specified name'
                },
                'value': format_space(record.get('FSN'))
            }
        ],
        'property': property
    }


def gpu_record_to_fhir_format(record, parent_gp_of_gpu, childs_tpu_of_gpu):
    code = format_space(record.get('TMTID(GPU)'))
    property = [
            {
                'code': 'class',
                'valueCode': 'GPU'
            },
            {
                'code': 'change_date',
                'valueDateTime': format_date(format_space(record.get('CHANGEDATE')), '%Y%m%d', '%Y-%m-%d')
            }
        ]
    childs_tpu = childs_tpu_of_gpu.get(code)
    if childs_tpu:
        childs_fhir_format = [{'code': 'child', 'valueCode': c} for c in childs_tpu]
        property = [*property, *childs_fhir_format]

    parent_gp = parent_gp_of_gpu.get(code)
    if parent_gp:
        parent_fhir_format = {'code': 'parent', 'valueCode': parent_gp}
        property.append(parent_fhir_format)

    return {
        'code': code,
        'display': format_space(record.get('FSN')),
        'designation': [
            {
                'use': {
                    'system': 'http://snomed.info/sct',
                    'code': '900000000000003001',
                    'display': 'Fully specified name'
                },
                'value': format_space(record.get('FSN'))
            }
        ],
        'property': property
    }


def tp_record_to_fhir_format(record, parent_gp_of_tp, childs_tpu_of_tp):
    code = format_space(record.get('TMTID(TP)'))
    property = [
            {
                'code': 'class',
                'valueCode': 'TP'
            },
            {
                'code': 'change_date',
                'valueDateTime': format_date(format_space(record.get('CHANGEDATE')), '%Y%m%d', '%Y-%m-%d')
            }
        ]
    childs_tpu = childs_tpu_of_tp.get(code)
    if childs_tpu:
        childs_fhir_format = [{'code': 'child', 'valueCode': c} for c in childs_tpu]
        property = [*property, *childs_fhir_format]

    parent_gp = parent_gp_of_tp.get(code)
    if parent_gp:
        parent_fhir_format = {'code': 'parent', 'valueCode': parent_gp}
        property.append(parent_fhir_format)

    return {
        'code': code,
        'display': format_space(record.get('FSN')),
        'designation': [
            {
                'use': {
                    'system': 'http://snomed.info/sct',
                    'code': '900000000000003001',
                    'display': 'Fully specified name'
                },
                'value': format_space(record.get('FSN'))
            }
        ],
        'property': property
    }


def tpu_record_to_fhir_format(tmt_tpu_record, parent_gpu_of_tpu, parent_tp_of_tpu):
    code = format_space(tmt_tpu_record.get('TMTID(TPU)'))
    property = [
            {
                'code': 'class',
                'valueCode': 'TPU'
            },
            {
                'code': 'manufacturer',
                'valueString': format_space(tmt_tpu_record.get('MANUFACTURER'))
            },
            {
                'code': 'change_date',
                'valueDateTime': format_date(format_space(tmt_tpu_record.get('CHANGEDATE')), '%Y%m%d', '%Y-%m-%d')
            }
        ]
    parent_gpu = parent_gpu_of_tpu.get(code)
    if parent_gpu:
        parent_fhir_format = {'code': 'parent', 'valueCode': parent_gpu}
        property.append(parent_fhir_format)
    
    parent_tp = parent_tp_of_tpu.get(code)
    if parent_tp:
        parent_fhir_format = {'code': 'parent', 'valueCode': parent_tp}
        property.append(parent_fhir_format)
    return {
        'code': code,
        'display': format_space(tmt_tpu_record.get('FSN')),
        'designation': [
            {
                'use': {
                    'system': 'http://snomed.info/sct',
                    'code': '900000000000003001',
                    'display': 'Fully specified name'
                },
                'value': format_space(tmt_tpu_record.get('FSN'))
            }
        ],
        'property': property
    }

