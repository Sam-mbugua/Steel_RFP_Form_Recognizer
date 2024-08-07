import pandas as pd
import numpy as np
import os 


stevenson_df = pd.read_csv("stevenson.csv")[['confidence', 'Incumbent Supplier', 'Receiving Plant', 'Receiving Plant City', 'Receiving Plant State', 'Receiving Plant Zip Code', 'Total Amount', 'Invoice', 'Equipment Type', 'Qty', 'Unit', 'Rate', 'Amount']]
extra_cols = ['Usage in Hours', 'Hourly', 'Daily', 'Weekly', 'Monthly']

stevenson_df['Usage in Hours'] = np.where(
    stevenson_df['Unit'].str.contains("Hour",na=False),
    stevenson_df['Qty'],
    None
    )

stevenson_df['Hourly'] = np.where(
    stevenson_df['Unit'].str.contains("Hour",na=False),
    stevenson_df['Rate'],
    None
    )

stevenson_df['Daily'] = np.where(
    stevenson_df['Unit'].str.contains("Day",na=False),
    stevenson_df['Rate'],
    None
    )
stevenson_df['Weekly'] = np.where(
    stevenson_df['Unit'].str.contains("Week",na=False),
    stevenson_df['Rate'],
    None
    )
stevenson_df['Monthly'] = np.where(
    stevenson_df['Unit'].str.contains("Month",na=False),
    stevenson_df['Rate'],
    None
    )

stevenson_df = stevenson_df[['confidence', 'Incumbent Supplier', 'Receiving Plant', 'Receiving Plant City', 'Receiving Plant State', 'Receiving Plant Zip Code', 'Total Amount', 'Invoice', 'Equipment Type', 'Qty',  'Usage in Hours', 'Hourly', 'Daily', 'Weekly', 'Monthly', 'Amount']].drop_duplicates()
stevenson_df.to_csv("processed_stevenson.csv")

processed_stevenson_df = pd.read_csv("data/processed_stevenson.csv")
hk_df = pd.read_csv("data/h_and_k_rental.csv")
sunbelt_df = pd.read_csv("data/sunbelt_rental.csv")
united_df = pd.read_csv("data/united_rental.csv")

all_invoices = processed_stevenson_df['Invoice'].unique().tolist() + hk_df['Invoice'].unique().tolist() + sunbelt_df['Invoice'].unique().tolist() + united_df['Invoice'].unique().tolist()
len(set(all_invoices))

path = "/Users/samuel_mbugua/Downloads/Rental Equipment Invoices-20240617"
dir_list = os.listdir(path)
un_processed_dir_list = [x for x in dir_list if x not in processed_files]
len(dir_list)
len(set(dir_list))
len(un_processed_dir_list)

processed_files = []
for invoice in set(all_invoices):
    for file_name in dir_list:
        if invoice and str(invoice) in file_name:
            processed_files.append(file_name)
len(set(processed_files))


processed_stevenson_df['confidence'].mean()
hk_df['confidence'].mean()
sunbelt_df['confidence'].mean()
united_df['confidence'].mean()
sum([0.90459,0.989393,0.96907181942,0.94350685])/4