
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient

def analyse_invoice(model_id, document_data,document_analysis_client):
    # Make sure your document's type is included in the list of document types the custom model can analyze
    # consider last page only
    poller = document_analysis_client.begin_analyze_document(model_id, document_data)
    document = poller.result().documents[-1]

    invoice_dict = {}
    invoice_dict["confidence"] = document.confidence
    for name, field in document.fields.items():
        if name != "Items":
            field_value = field.value if field.value else field.content
            invoice_dict[name] = field_value
        elif name == "Items":
            invoice_dict[name] = []
            for line_item in field.value:
                line_item_dict = {}
                for field_name, field in line_item.value.items():
                    line_item_value = field.value if field.value else field.content
                    line_item_dict[field_name] = line_item_value
                invoice_dict[name].append(line_item_dict)

    return invoice_dict
