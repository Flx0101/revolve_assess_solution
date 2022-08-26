import argparse
import csv
import pandas as pd
import glob
import os


def get_params() -> dict:
    parser = argparse.ArgumentParser(description="DataTest")
    parser.add_argument(
        "--customers_location",
        required=False,
        default="./input_data/starter/customers.csv",
    )
    parser.add_argument(
        "--products_location",
        required=False,
        default="./input_data/starter/products.csv",
    )
    parser.add_argument(
        "--transactions_location",
        required=False,
        default="./input_data/starter/transactions/",
    )
    parser.add_argument(
        "--output_location", required=False, default="./output_data/outputs/"
    )
    return vars(parser.parse_args())


def occurence_generate(values, base):
    """
    This function flattens list of different product a customer buys on a date to new rows.
    It accepts values which is a group of groubByDataframe object for each customer_id and base which is to store product purchase count for a week.
    """

    # We create list of each records
    values = values.values.tolist()

    # Fetching customer key from values
    customer_key = values[0][0]

    # It stores all the products that a particular customer buys for a weekly dataframe 
    each_instance = []
    for products in values:
        each_basket_products = [
            each_product["product_id"] for each_product in products[1]
        ]
        each_instance.extend(each_basket_products)

    # For each customer we count different product occurence using value_counts and then store it as a dictionary
    base[customer_key] = pd.Series(each_instance).value_counts().to_dict()


def read_transactions(transaction_filepath, customers_op, products_op):
    """
    Input -> (Transactions filepath, customers dataframe, products dataframe)
    Read_transaction function helps to read all transaction.json files of a week.
    It then forms a combined weekly dataframe for computation of purchase count of each product.
    After each weekly computation, the output is capture in output folder with file naming convention - week_*.json
    """

    # Glob.glob generates list of all folders present in ./input_data/transactions/
    all_files = glob.glob(transaction_filepath + "*")
    count = 0
    cummulative_week=[]

    # Creates output folder
    output_location = f"./output/"
    os.makedirs(output_location, exist_ok=True)

    # Stepper for for loop is 7 because we consider data for a week
    for filecount in range(0, len(all_files), 7):
        # base dictionary is used as a Hashmap to contain customer_id as key and product_id & product count dictionary as a value for entire week
        base = {}

        # In order to concat each files dataframe of a week to a single weekly dataframe we use temporary_df which stores each dataframe
        temporary_df = [] 

        # Iterating over all transactions.json files of each week
        for file in all_files[filecount : filecount + 7]:

            transaction_df = pd.read_json(file + "\\transactions.json", lines=True)
            temporary_df.append(transaction_df)

        # Grouping by customer_id for the weekly dataframe
        final_transaction_df = (
            pd.concat(temporary_df).groupby("customer_id").apply(occurence_generate, base)
        )
        count += 1
        # print("WEEK : ", count)

        # Preparing the required weekly output of customer_id, loyalty_score, product_id, product_category, purchase_count using list comprehension

        final_week = [
            [
                customer,
                customers_op.loc[customer]["loyalty_score"],
                product,
                products_op.loc[product]["product_category"],
                base[customer][product],
            ]
            for customer in base
            for product in base[customer]
        ]

        # Converting cummulative final_week value to a dataframe so that we can directly output it as a json
        weekly_df=pd.DataFrame(final_week,columns=["customer_id", "loyalty_score", "product_id", "product_category", "purchase_count"])

        # cummulative_week.append(weekly_df)

        # Here we generate weekly json and the lines=True parameters is assigned to make output records in same format as input records
        weekly_df.to_json(f"./output/week_{count}.json",orient="records",lines=True)

    # Using Cummulative records we can generate a single json with all weeks in it if required

    # cummulative_week_df=pd.concat(cummulative_week)
    # cummulative_week_df.to_json(f"./output/cummilative_weeks.json",orient="records",lines=True)

def read_products(products_filepath):
    """
    Reads products.csv file and converts it to pandas dataframe
    """
    products_df = pd.read_csv(
        products_filepath, delimiter=",", quotechar='"', index_col=[0]
    )
    # print(products_df.head(10))
    return products_df


def read_customer(customer_filepath):
    """
    Reads customer.csv file and converts it to pandas dataframe
    """
    customer_df = pd.read_csv(
        customer_filepath, delimiter=",", quotechar='"', index_col=[0]
    )
    return customer_df


def main():
    params = get_params()
    # Here reading products.csv, customers.csv can be done in multiprocessing, multithreading, async manner as well to read both the files simultaneously.
    products_op = read_products(params["products_location"])
    customers_op = read_customer(params["customers_location"])
    transactions_op = read_transactions(
        params["transactions_location"], customers_op, products_op
    )

    # print(transactions_op)
    # print(products_op)
    # print(customers_op)


if __name__ == "__main__":
    main()

    # The solution is ready and the pull request can be created by using git add . -> git commit -m <commit message> -> git push <remote> <branch>