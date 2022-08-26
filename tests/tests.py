# from REVOLVE_ASSIGNMENT.solution.solution_start import *
import unittest
import sys
sys.path.insert(0, './solution/')
import solution_start

import unittest

class DataFrameSum(unittest.TestCase):

    def test_customers(self):
        expected_op=["customer_id","loyalty_score"]
        input_df=solution_start.read_customer("./input_data/starter/customers.csv")
        self.assertEqual([input_df.index.name,input_df.columns[0]],expected_op)
    
    def test_products(self):
        expected_op=["product_id","product_description","product_category"]
        input_df=solution_start.read_customer("./input_data/starter/products.csv")
        code_input=[input_df.index.name]
        code_input.extend(list(input_df.columns.values))
        self.assertEqual(code_input,expected_op)

if __name__ == '__main__':
    unittest.main()
