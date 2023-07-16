import logging
import pandas as pd
import azure.functions as func
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from io import BytesIO

def process_excel(myretaildata):
    try:
        logging.info("inside the procees_excel function + data cleaning started")

        logging.info("Stripping descriptions")
        myretaildata['Description'] = myretaildata['Description'].str.strip()

        logging.info("Dropping NaN values")
        myretaildata.dropna(axis=0, subset=['InvoiceNo'], inplace=True)

        logging.info("Converting invoice numbers to string")
        myretaildata['InvoiceNo'] = myretaildata['InvoiceNo'].astype('str')

        logging.info("Removing credit transactions")
        myretaildata = myretaildata[~myretaildata['InvoiceNo'].str.contains('C')]

        logging.info("Separating transactions for Germany")
        mybasket = (myretaildata[myretaildata['Country'] =="Germany"]
                .groupby(['InvoiceNo', 'Description'])['Quantity']
                .sum().unstack().reset_index().fillna(0)
                .set_index('InvoiceNo'))

        logging.info("Encoding units")
        def my_encode_units(x):
            if x <= 0:
                return 0
            if x >= 1:
                return 1

        my_basket_sets = mybasket.applymap(my_encode_units)

        # check if 'POSTAGE' is in the DataFrame and drop it if present
        if 'POSTAGE' in my_basket_sets.columns:
            logging.info("Dropping 'POSTAGE' column")
            my_basket_sets.drop('POSTAGE', inplace=True, axis=1) 

        # my_basket_sets.drop('POSTAGE', inplace=True, axis=1) #Remove "postage" as an item

        logging.info("Generating frequent itemsets")
        my_frequent_itemsets = apriori(my_basket_sets, min_support=0.07, use_colnames=True)

        logging.info("Generating rules")
        my_rules = association_rules(my_frequent_itemsets, metric="lift", min_threshold=1)

        logging.info("Filtering rules")
        res = my_rules[ (my_rules['lift'] >= 3) &
            (my_rules['confidence'] >= 0.3) ]

        logging.info("data cleaning and processing completed, exiting from the process_excel function")

        return res.to_json(orient='records')  # convert pandas dataframe to json
    except Exception as e:
        logging.error(f"Exception occurred: {e}")
        raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # get the file from the request body
    body = req.get_body()
    logging.info("body received")

    # create a BytesIO object from the body and read it into a DataFrame
    logging.info("Creating dataframe")
    data = pd.read_excel(BytesIO(body))
    logging.info("dataframe created and converted from bytes to excel")

    # process data
    logging.info("Starting process_excel")
    result = process_excel(data)
    logging.info("Returning the result")

    return func.HttpResponse(result)
