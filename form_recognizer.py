from azure.ai.formrecognizer import (
    DocumentAnalysisClient,
    DocumentModelAdministrationClient,
    ModelBuildMode,
)
from azure.core.credentials import AzureKeyCredential
from form_recognizer_custom import analyse_invoice
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from dynaconf import Dynaconf
import time



def set_up_endpoint():
    params = Dynaconf(settings_files=['parameters.yaml'])
    API_KEY = params['API_KEY']
    ENDPOINT = params['ENDPOINT']
    credential = AzureKeyCredential(API_KEY)
    document_analysis_client = DocumentAnalysisClient(ENDPOINT, credential)

    return document_analysis_client

def get_supplier(document_data,document_analysis_client):
    # get supplier
    poller = document_analysis_client.begin_analyze_document(
            "prebuilt-invoice", document=document_data, locale="en-US"
        )
    invoice = poller.result().documents[0]
    supplier = invoice.fields.get("VendorName").value if invoice.fields.get("VendorName") else None

    return supplier

def get_data_from_invoice(document_data,document_analysis_client):
    """Depending on the supplier, apply the right azure form recognizer model and
    extract data"""

    supplier = get_supplier(document_data,document_analysis_client)
    res_dict = None

    if supplier and ('united' in supplier.lower()):
        res_dict = analyse_invoice('united_rentals_01', document_data,document_analysis_client)
    elif supplier and ('sunbelt' in supplier.lower()):
        res_dict = analyse_invoice('sunbelt_rentals_01', document_data,document_analysis_client)
    elif supplier and (('h&k' in supplier.lower())or('hk' in supplier.lower())) :
        res_dict = analyse_invoice('h_and_k_rentals_01', document_data,document_analysis_client)
    # if supplier and ('stevenson' in supplier.lower()):
    #     res_dict = analyse_invoice('stevenson_rentals_01', document_data,document_analysis_client)

    return res_dict
    
def get_document_data(document_path):
    with open(document_path, "rb") as document_file:
        document_data = document_file.read()
    return document_data

def parse_extracted_data(data):
    # Extract general information
    general_info = {
        'confidence': data['confidence'],
        'Incumbent Supplier': data['Incumbent Supplier'],
        'Receiving Plant': data['Receiving Plant'],
        'Receiving Plant City': data['Receiving Plant City'],
        'Receiving Plant State': data['Receiving Plant State'],
        'Receiving Plant Zip Code': data['Receiving Plant Zip Code'],
        'Total Amount': data['Total Amount'],
        'Invoice': data['Invoice']
    }

    # Create a list of dictionaries, one for each item
    records = []
    if data['Items']:
        for item in data['Items']:
            record = {**general_info, **item}  # Merge general info with item-specific info
            records.append(record)
    else:
        records.append(general_info)
        

    # Convert list of dictionaries to DataFrame
    new_df = pd.DataFrame(records)

    return new_df

def process_single_invoice(document_analysis_client,file_path):
    agg_res_df = pd.DataFrame()
    document_data = get_document_data(file_path)
    res_dict = get_data_from_invoice(document_data,document_analysis_client)
    if res_dict:
        agg_res_df = parse_extracted_data(res_dict)
    return agg_res_df


def process_multiple_invoices(document_analysis_client, file_path_list,agg_res_df = pd.DataFrame()):
    with ThreadPoolExecutor(max_workers=20) as executor:  # Adjust max_workers based on your system's capability
        futures = {executor.submit(process_single_invoice, document_analysis_client, file_path): file_path for file_path in file_path_list}
        for future in as_completed(futures):
            try:
                result_df = future.result()
                agg_res_df = pd.concat([agg_res_df, result_df], ignore_index=True)
            except Exception as e:
                print(f"Error processing file {futures[future]}: {e}")
    return agg_res_df


if __name__=="main":
    #file_path = "/Users/samuel_mbugua/Downloads/Rental Equipment Invoices-20240617/20231023082718499566_222275697-001.pdf"
  
    document_analysis_client = set_up_endpoint()
    processed_paths = []

    st_time = time.time()
    path = "/Users/samuel_mbugua/Downloads/Rental Equipment Invoices-20240617"
    dir_list = os.listdir(path)
    file_path_list = [os.path.join(path,x) for x in dir_list if os.path.join(path,x) not in processed_paths]
    len(file_path_list)

    agg_res_df = process_multiple_invoices(document_analysis_client, file_path_list)
    agg_res_df.to_csv("invoice.csv")
    duration = (time.time()-st_time)/60

    

    

