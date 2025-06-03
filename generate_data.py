import requests
import pandas as pd

def extract_url(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        if "result" in data and "records" in data["result"]:
            df = pd.DataFrame(data["result"]["records"])
            print(f"Extracted {len(df)} records successfully.")
            return df
        else:
            print("No records found in the response.")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
        return pd.DataFrame()

    except ValueError as ve:
        print(f"Data parsing error: {ve}")
        return pd.DataFrame()
      
api_url = "https://data.gov.il/api/3/action/datastore_search?resource_id=e83f763b-b7d7-479e-b172-ae981ddc6de5"
flight_df = extract_url(api_url)

flight_df['CHSTOL'] = pd.to_datetime(flight_df['CHSTOL'])
flight_df['CHPTOL'] = pd.to_datetime(flight_df['CHPTOL'])

columns_to_drop = ['CHOPER', 'CHAORD', 'CHRMINH', 'CHLOC1', 'CHLOC1D', 'CHLOC1TH', 'CHLOC1CH']
flight_df2 = flight_df.drop(columns=columns_to_drop)

column_renames = {
    'CHFLTN': 'flight_number',
    'CHOPERD': 'airline_name',
    'CHSTOL': 'planned_date',
    'CHPTOL': 'actual_date',
    'CHLOC1T': 'destination_city',
    'CHLOCCT': 'destination_country',
    'CHTERM': 'terminal_number',
    'CHCINT': 'check_in_number',
    'CHCKZN': 'check_in_zone',
    'CHRMINE': 'status_flight'}

flight_df2.rename(columns=column_renames, inplace=True)

flight_df2['planned_date'] = flight_df2['planned_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
flight_df2['actual_date'] = flight_df2['actual_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

flight_df2.to_csv('output/processed_flight_data.csv', index=False)
