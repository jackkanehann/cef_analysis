import requests
import json
import pandas as pd
import numpy as np

# U.S. Treasury Fiscal Data API URL for Auction Results
# Endpoint: /v2/accounting/od/treasury_securities_auctions_data
url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query?"

# Parameters to get the most recent data (sorted by auction_date)
params = {
    "fields": "auction_date,security_type,security_term,cusip,bid_to_cover_ratio,total_tendered,soma_tendered,soma_accepted,soma_holdings",
    ## Additional Fields: comp_accepted, comp_tendered, comp_tenders_accepted, direct_bidder_accepted, direct_bidder_tendered, indirect_bidder_accepted, indirect_bidder_tendered, floating_rate
    ## noncomp_accepted, noncomp_tendered, noncomp_tenders_accepted,
    ## low/high discount and investment rates, offering_amt, primary_dealer_accepted/tendered
    ## soma_accepted, soma_holdings, soma_tendered
    "sort": "-auction_date",  # Sort by date descending
    "page[size]": "9999"          # Get the N most recent result(s)
}

try:
    response = requests.get(url, params=params)
    response.raise_for_status()  # Check for errors
    data = response.json()
    

    # Print the result
    #print(json.dumps(data, indent=2))
    
    

    # Create df to use elsewhere?
    df = pd.DataFrame(data['data'])
    ## print(df)
    

    # Example: Accessing the bid-to-cover ratio directly
    '''
    if data['data']:
        latest = data['data'][0]
        print(f"\nLatest Auction Date: {latest['auction_date']}")
        print(f"Security Type: {latest['security_type']}")
        print(f"Bid-to-Cover Ratio: {latest['bid_to_cover_ratio']}")
    '''

except Exception as e:
    print(f"Error fetching data: {e}")

## df cleaning
#remove rows with 'null' values--> corresponds to announced but not yet occurred auctions
df = df.replace('null', np.nan)
df_clean = df.dropna()

# Standardize datetime in df


print("CLEAN DF\n",df_clean)

