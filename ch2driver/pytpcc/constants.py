# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

MONEY_DECIMALS = 2

#  Item constants
NUM_ITEMS = 100000
MIN_IM = 1
MAX_IM = 10000
MIN_PRICE = 1.00
MAX_PRICE = 100.00
MIN_I_NAME = 14
MAX_I_NAME = 24
MIN_I_DATA = 26
MAX_I_DATA = 50

#  Warehouse constants
MIN_TAX = 0
MAX_TAX = 0.2000
TAX_DECIMALS = 4
INITIAL_W_YTD = 300000.00
MIN_NAME = 6
MAX_NAME = 10
MIN_STREET = 10
MAX_STREET = 20
MIN_CITY = 10
MAX_CITY = 20
STATE = 2
ZIP_LENGTH = 9
ZIP_SUFFIX = "11111"

# Warehouse constants
STARTING_WAREHOUSE = 1

#  Stock constants
MIN_QUANTITY = 10
MAX_QUANTITY = 100
DIST = 24
STOCK_PER_WAREHOUSE = 100000

#  District constants
DISTRICTS_PER_WAREHOUSE = 10
INITIAL_D_YTD = 30000.00  #  different from Warehouse
INITIAL_NEXT_O_ID = 3001

#  Customer constants
CUSTOMERS_PER_DISTRICT = 3000
INITIAL_CREDIT_LIM = 50000.00
MIN_DISCOUNT = 0.0000
MAX_DISCOUNT = 0.5000
DISCOUNT_DECIMALS = 4
INITIAL_BALANCE = -10.00
MIN_BALANCE = -50.00
MAX_BALANCE = 50.00
INITIAL_YTD_PAYMENT = 10.00
INITIAL_PAYMENT_CNT = 1
INITIAL_DELIVERY_CNT = 0
MIN_FIRST = 6
MAX_FIRST = 10
MIDDLE = "OE"
PHONE = 16
MIN_C_DATA = 300
MAX_C_DATA = 500
GOOD_CREDIT = "GC"
BAD_CREDIT = "BC"

#  Order constants
MIN_CARRIER_ID = 1
MAX_CARRIER_ID = 10
#  HACK: This is not strictly correct, but it works
NULL_CARRIER_ID = 0
#  o_id < than this value, carrier != null, >= -> carrier == null
NULL_CARRIER_LOWER_BOUND = 2101
MIN_OL_CNT = 5
MAX_OL_CNT = 15
INITIAL_ALL_LOCAL = 1
INITIAL_ORDERS_PER_DISTRICT = 3000

#  Used to generate initial orderline quantities and new order orderline amounts
MIN_OL_QUANTITY = 1
MAX_OL_QUANTITY = 50

#  Order line constants
INITIAL_QUANTITY = 5
MIN_AMOUNT = 0.01

#  History constants
MIN_DATA = 12
MAX_DATA = 24
INITIAL_AMOUNT = 10.00

#  New order constants
INITIAL_NEW_ORDERS_PER_DISTRICT = 900

# Supplier constants
NUM_SUPPLIERS = 10000
NUM_LEADING_ZEROS = 9
MIN_SUPPLIER_ADDRESS = 10
MAX_SUPPLIER_ADDRESS = 40
MIN_SUPPLIER_ACCTBAL = -999.99
MAX_SUPPLIER_ACCTBAL = 9999.99
MIN_SUPPLIER_COMMENT = 25
MAX_SUPPLIER_COMMENT = 100

# Nation constants
NUM_NATIONS = 62
MIN_NATION_COMMENT = 31
MAX_NATION_COMMENT = 114

# Region constants
NUM_REGIONS = 5
MIN_REGION_COMMENT = 31
MAX_REGION_COMMENT = 115

#  TPC-C 2.4.3.4 (page 31) says this must be displayed when new order rolls back.
INVALID_ITEM_MESSAGE = "Item number is not valid"

#  Used to generate stock level transactions
MIN_STOCK_LEVEL_THRESHOLD = 10
MAX_STOCK_LEVEL_THRESHOLD = 20

#  Used to generate payment transactions
MIN_PAYMENT = 1.0
MAX_PAYMENT = 5000.0

#  Indicates "brand" items and stock in i_data and s_data.
ORIGINAL_STRING = "ORIGINAL"

CH2_NAMESPACE = "default"
CH2_BUCKET = "bench"

NUM_LOAD_RETRIES = 10
CH2_DRIVER_LOAD_MODE = {
    "NOT_SET": -1,
    "DATASVC_BULKLOAD": 0,
    "DATASVC_LOAD": 1,
    "QRYSVC_LOAD": 2,
    "DOCGEN_LOAD": 3,
}
CH2_DRIVER_LOAD_FORMAT = {
    "JSON":0,
    "CSV":1
}
CH2_DRIVER_SCHEMA = {
    "CH2":"ch2",
    "CH2P":"ch2p",
    "CH2PP":"ch2pp",
    "CH2PPF":"ch2ppf"
}
CH2_DRIVER_ANALYTICAL_QUERIES = {
    "HAND_OPTIMIZED_QUERIES":0,
    "NON_OPTIMIZED_QUERIES":1
}

MAX_EXTRA_FIELDS = 128
CH2_CUSTOMER_EXTRA_FIELDS = {
    "NOT_SET":-1,
    CH2_DRIVER_SCHEMA["CH2"]:0,
    CH2_DRIVER_SCHEMA["CH2P"]:0,
    CH2_DRIVER_SCHEMA["CH2PP"]:128,
    CH2_DRIVER_SCHEMA["CH2PPF"]:128,
}
CH2_ORDERS_EXTRA_FIELDS = {
    "NOT_SET":-1,
    CH2_DRIVER_SCHEMA["CH2"]:0,
    CH2_DRIVER_SCHEMA["CH2P"]:0,
    CH2_DRIVER_SCHEMA["CH2PP"]:128,
    CH2_DRIVER_SCHEMA["CH2PPF"]:128,
}
CH2_ITEM_EXTRA_FIELDS = {
    "NOT_SET":-1,
    CH2_DRIVER_SCHEMA["CH2"]:0,
    CH2_DRIVER_SCHEMA["CH2P"]:0,
    CH2_DRIVER_SCHEMA["CH2PP"]:128,
    CH2_DRIVER_SCHEMA["CH2PPF"]:128,
}

CH2_DATAGEN_SEED_NOT_SET = -1

CH2_DRIVER_KV_TIMEOUT = 10
CH2_DRIVER_BULKLOAD_BATCH_SIZE = 1024 * 256 # 256K

# Table Names
TABLENAME_ITEM       = "item"
TABLENAME_ITEM_CATEGORIES_FLAT = "item_categories"
TABLENAME_WAREHOUSE  = "warehouse"
TABLENAME_DISTRICT   = "district"
TABLENAME_CUSTOMER   = "customer"
TABLENAME_STOCK      = "stock"
TABLENAME_ORDERS     = "orders"
TABLENAME_NEWORDER   = "neworder"
TABLENAME_ORDERLINE  = "orderline_nested"
TABLENAME_ORDERLINE_FLAT  = "orders_orderline"
TABLENAME_HISTORY    = "history"
TABLENAME_SUPPLIER   = "supplier"
TABLENAME_NATION     = "nation"
TABLENAME_REGION     = "region"
TABLENAME_WAREHOUSE_ADDRESS  = "warehouse_address_nested"
TABLENAME_DISTRICT_ADDRESS  = "district_address_nested"
TABLENAME_CUSTOMER_NAME  = "customer_name_nested"
TABLENAME_CUSTOMER_ADDRESSES  = "customer_addresses_nested"
TABLENAME_CUSTOMER_ADDRESSES_FLAT  = "customer_addresses"
TABLENAME_CUSTOMER_PHONES  = "customer_phones_nested"
TABLENAME_CUSTOMER_PHONES_FLAT  = "customer_phones"
TABLENAME_CUSTOMER_ITEM_CATEGORIES_FLAT = "customer_item_categories"
TABLENAME_SUPPLIER_ADDRESS  = "supplier_address_nested"

COLLECTIONS_DICT = {
    TABLENAME_ITEM:"item",
    TABLENAME_ITEM_CATEGORIES_FLAT:"item_categories",
    TABLENAME_WAREHOUSE:"warehouse",
    TABLENAME_DISTRICT:"district",
    TABLENAME_CUSTOMER:"customer",
    TABLENAME_STOCK:"stock",
    TABLENAME_ORDERS:"orders",
    TABLENAME_NEWORDER:"neworder",
    TABLENAME_ORDERLINE:"orderline_nested",
    TABLENAME_ORDERLINE_FLAT:"orders_orderline",
    TABLENAME_HISTORY:"history",
    TABLENAME_SUPPLIER:"supplier",
    TABLENAME_NATION:"nation",
    TABLENAME_REGION:"region",
    TABLENAME_CUSTOMER_ADDRESSES_FLAT:"customer_addresses",
    TABLENAME_CUSTOMER_PHONES_FLAT:"customer_phones",
    TABLENAME_CUSTOMER_ITEM_CATEGORIES_FLAT:"customer_item_categories"}

ALL_TABLES = [
    TABLENAME_ITEM,
    TABLENAME_ITEM_CATEGORIES_FLAT,
    TABLENAME_WAREHOUSE,
    TABLENAME_DISTRICT,
    TABLENAME_CUSTOMER,
    TABLENAME_CUSTOMER_ADDRESSES_FLAT,
    TABLENAME_CUSTOMER_PHONES_FLAT,
    TABLENAME_CUSTOMER_ITEM_CATEGORIES_FLAT,
    TABLENAME_STOCK,
    TABLENAME_ORDERS,
    TABLENAME_ORDERLINE,
    TABLENAME_ORDERLINE_FLAT,
    TABLENAME_NEWORDER,
    TABLENAME_HISTORY,
    TABLENAME_SUPPLIER,
    TABLENAME_NATION,
    TABLENAME_REGION
]

NATIONS = [
    [48, "Algeria", 0],
    [49, "Argentina", 1],
    [50, "Brazil", 1],
    [51, "Canada", 1],
    [52, "Egypt", 4],
    [53, "Ethiopia", 0],
    [54, "France", 3],
    [55, "Germany", 3],
    [56, "India", 2],
    [57, "Indonesia", 2],

    [65, "Iran", 4],
    [66, "Iraq", 4],
    [67, "Japan", 2],
    [68, "Jordan", 4],
    [69, "Kenya", 0],
    [70, "Morocco", 0],
    [71, "Mozambique", 0],
    [72, "Peru", 1],
    [73, "China", 2],
    [74, "Kuwait", 4],
    [75, "Saudi Arabia", 4],
    [76, "Vietnam", 2],
    [77, "Russia", 3],
    [78, "United Kingdom", 3],
    [79, "United States", 1],
    [80, "Lebanon", 4],
    [81, "Oman", 4],
    [82, "Qatar", 4],
    [83, "Mexico", 1],
    [84, "Turkey", 4],
    [85, "Chile", 1],
    [86, "Italy", 3],
    [87, "South Africa", 0],
    [88, "South Korea", 2],
    [89, "Colombia", 1],
    [90, "Spain", 3],

    [97, "Ukraine", 3],
    [98, "Ecuador", 1],
    [99, "Sudan", 0],
    [100, "Uzbekistan", 2],
    [101, "Malaysia", 2],
    [102, "Venezuela", 1],
    [103, "Tanzania", 0],
    [104, "Afghanistan", 2],
    [105, "North Korea", 2],
    [106, "Taiwan", 2],
    [107, "Ghana", 0],
    [108, "Ivory Coast", 0],
    [109, "Syria", 4],
    [110, "Madagascar", 0],
    [111, "Cameroon", 0],
    [112, "Nigeria", 0],
    [113, "Bolivia", 1],
    [114, "Netherlands", 3],
    [115, "Cambodia", 2],
    [116, "Belgium", 3],
    [117, "Greece", 3],
    [118, "Uruguay", 1],
    [119, "Israel", 4],
    [120, "Finland", 3],
    [121, "Singapore", 2],
    [122, "Norway", 3]
]

REGIONS = ["Africa", "America", "Asia", "Europe", "Middle East"]

KEYNAMES = {
        TABLENAME_ITEM:         [0],  # INTEGER
        TABLENAME_ITEM_CATEGORIES_FLAT: [0, 1],
        TABLENAME_WAREHOUSE:    [0],  # INTEGER
        TABLENAME_DISTRICT:     [1, 0],  # INTEGER
        TABLENAME_CUSTOMER:     [2, 1, 0], # INTEGER
        TABLENAME_STOCK:        [1, 0],  # INTEGER
        TABLENAME_ORDERS:       [3, 2, 0], # INTEGER
        TABLENAME_NEWORDER:     [2, 1, 0], # INTEGER
        TABLENAME_ORDERLINE:    [2, 1, 0, 3], # INTEGER
        TABLENAME_ORDERLINE_FLAT: [2, 1, 0, 3], # INTEGER
        TABLENAME_HISTORY:      [2, 1, 0], # INTEGER
        TABLENAME_SUPPLIER:     [0],  # INTEGER
        TABLENAME_NATION:       [0],  # INTEGER
        TABLENAME_REGION:       [0],  # INTEGER
        TABLENAME_CUSTOMER_ADDRESSES_FLAT: [2, 1, 0, 3],
        TABLENAME_CUSTOMER_PHONES_FLAT: [2, 1, 0, 4],
        TABLENAME_CUSTOMER_ITEM_CATEGORIES_FLAT: [2, 1, 0, 3],
}

CH2_TABLE_COLUMNS = {
    TABLENAME_ITEM: [
        "i_id", # INTEGER
        "i_name", # VARCHAR
        "i_price", # FLOAT
        "i_extra", # Extra unused fields
        "i_categories", # ARRAY
        "i_data", # VARCHAR
        "i_im_id", # INTEGER
    ],
    TABLENAME_WAREHOUSE: [
        "w_id", # SMALLINT
        "w_ytd", # FLOAT
        "w_tax", # FLOAT
        "w_name", # VARCHAR
        "w_street_1", # VARCHAR
        "w_street_2", # VARCHAR
        "w_city", # VARCHAR
        "w_state", # VARCHAR
        "w_zip", # VARCHAR
    ],
    TABLENAME_DISTRICT: [
        "d_id", # TINYINT
        "d_w_id", # SMALLINT
        "d_ytd", # FLOAT
        "d_tax", # FLOAT
        "d_next_o_id", # INT
        "d_name", # VARCHAR
        "d_street_1", # VARCHAR
        "d_street_2", # VARCHAR
        "d_city", # VARCHAR
        "d_state", # VARCHAR
        "d_zip", # VARCHAR
    ],
    TABLENAME_CUSTOMER:   [
        "c_id", # INTEGER
        "c_d_id", # TINYINT
        "c_w_id", # SMALLINT
        "c_discount", # FLOAT
        "c_credit", # VARCHAR
        "c_first", # VARCHAR
        "c_middle", # VARCHAR
        "c_last", # VARCHAR
        "c_credit_lim", # FLOAT
        "c_balance", # FLOAT
        "c_ytd_payment", # FLOAT
        "c_payment_cnt", # INTEGER
        "c_delivery_cnt", # INTEGER
        "c_extra", # Extra unused fields
        "c_street_1", # VARCHAR
        "c_street_2", # VARCHAR
        "c_city", # VARCHAR
        "c_state", # VARCHAR
        "c_zip", # VARCHAR
        "c_phone", # VARCHAR
        "c_since", # TIMESTAMP
        "c_item_categories", # ARRAY
        "c_data", # VARCHAR
    ],
    TABLENAME_STOCK:      [
        "s_i_id", # INTEGER
        "s_w_id", # SMALLINT
        "s_quantity", # INTEGER
        "s_ytd", # INTEGER
        "s_order_cnt", # INTEGER
        "s_remote_cnt", # INTEGER
        "s_data", # VARCHAR
        "s_dist_01", # VARCHAR
        "s_dist_02", # VARCHAR
        "s_dist_03", # VARCHAR
        "s_dist_04", # VARCHAR
        "s_dist_05", # VARCHAR
        "s_dist_06", # VARCHAR
        "s_dist_07", # VARCHAR
        "s_dist_08", # VARCHAR
        "s_dist_09", # VARCHAR
        "s_dist_10", # VARCHAR
    ],
    TABLENAME_ORDERS:     [
        "o_id", # INTEGER
        "o_c_id", # INTEGER
        "o_d_id", # TINYINT
        "o_w_id", # SMALLINT
        "o_carrier_id", # INTEGER
        "o_ol_cnt", # INTEGER
        "o_all_local", # INTEGER
        "o_entry_d", # TIMESTAMP
        "o_extra", # Extra unused fields
        "o_orderline", # ARRAY
    ],
    TABLENAME_NEWORDER:  [
        "no_o_id", # INTEGER
        "no_d_id", # TINYINT
        "no_w_id", # SMALLINT
    ],
    TABLENAME_ORDERLINE: [
#        "ol_o_id", # INTEGER
#        "ol_d_id", # TINYINT
#        "ol_w_id", # SMALLINT
        "ol_number", # INTEGER
        "ol_i_id", # INTEGER
        "ol_supply_w_id", # SMALLINT
        "ol_delivery_d", # TIMESTAMP
        "ol_quantity", # INTEGER
        "ol_amount", # FLOAT
        "ol_dist_info", # VARCHAR
    ],
    TABLENAME_HISTORY:    [
        "h_c_id", # INTEGER
        "h_c_d_id", # TINYINT
        "h_c_w_id", # SMALLINT
        "h_d_id", # TINYINT
        "h_w_id", # SMALLINT
        "h_date", # TIMESTAMP
        "h_amount", # FLOAT
        "h_data", # VARCHAR
    ],
    TABLENAME_SUPPLIER:    [
        "su_suppkey", # INTEGER
        "su_name", # VARCHAR
        "su_address", # VARCHAR
        "su_nationkey", # INTEGER
        "su_phone", # VARCHAR
        "su_acctbal", # FLOAT
        "su_comment", # VARCHAR
    ],
    TABLENAME_NATION:    [
        "n_nationkey", # INTEGER
        "n_name", # VARCHAR
        "n_regionkey", # INTEGER
        "n_comment", # VARCHAR
    ],
    TABLENAME_REGION:    [
        "r_regionkey", # INTEGER
        "r_name", # VARCHAR
        "r_comment", # VARCHAR
    ],
}

CH2PP_TABLE_COLUMNS = {
    TABLENAME_ITEM: [
        "i_id", # INTEGER
        "i_name", # VARCHAR
        "i_price", # FLOAT
        "i_extra", # Extra unused fields
        "i_categories", # ARRAY
        "i_data", # VARCHAR
        "i_im_id", # INTEGER
    ],
    TABLENAME_ITEM_CATEGORIES_FLAT:    [
        "i_id", # INTEGER
        "i_category", # VARCHAR
    ],
    TABLENAME_WAREHOUSE: [
        "w_id", # SMALLINT
        "w_ytd", # FLOAT
        "w_tax", # FLOAT
        "w_name", # VARCHAR
        "w_address", # JSON
    ],
    TABLENAME_DISTRICT: [
        "d_id", # TINYINT
        "d_w_id", # SMALLINT
        "d_ytd", # FLOAT
        "d_tax", # FLOAT
        "d_next_o_id", # INT
        "d_name", # VARCHAR
        "d_address", # JSON
    ],
    TABLENAME_CUSTOMER:   [
        "c_id", # INTEGER
        "c_d_id", # TINYINT
        "c_w_id", # SMALLINT
        "c_discount", # FLOAT
        "c_credit", # VARCHAR
        "c_name", # JSON OBJECT
        "c_credit_lim", # FLOAT
        "c_balance", # FLOAT
        "c_ytd_payment", # FLOAT
        "c_payment_cnt", # INTEGER
        "c_delivery_cnt", # INTEGER
        "c_extra", # Extra unused fields
        "c_addresses", # ARRAY
        "c_phones", # ARRAY
        "c_since", # TIMESTAMP
        "c_item_categories", # ARRAY
        "c_data", # VARCHAR
    ],
    TABLENAME_STOCK:      [
        "s_i_id", # INTEGER
        "s_w_id", # SMALLINT
        "s_quantity", # INTEGER
        "s_ytd", # INTEGER
        "s_order_cnt", # INTEGER
        "s_remote_cnt", # INTEGER
        "s_data", # VARCHAR
        "s_dists", # ARRAY
    ],
    TABLENAME_ORDERS:     [
        "o_id", # INTEGER
        "o_c_id", # INTEGER
        "o_d_id", # TINYINT
        "o_w_id", # SMALLINT
        "o_carrier_id", # INTEGER
        "o_ol_cnt", # INTEGER
        "o_all_local", # INTEGER
        "o_entry_d", # TIMESTAMP
        "o_extra", # Extra unused fields
        "o_orderline", # ARRAY
    ],
    TABLENAME_NEWORDER:  [
        "no_o_id", # INTEGER
        "no_d_id", # TINYINT
        "no_w_id", # SMALLINT
    ],
    TABLENAME_ORDERLINE: [
#        "ol_o_id", # INTEGER
#        "ol_d_id", # TINYINT
#        "ol_w_id", # SMALLINT
        "ol_number", # INTEGER
        "ol_i_id", # INTEGER
        "ol_supply_w_id", # SMALLINT
        "ol_delivery_d", # TIMESTAMP
        "ol_quantity", # INTEGER
        "ol_amount", # FLOAT
        "ol_dist_info", # VARCHAR
    ],
    TABLENAME_ORDERLINE_FLAT: [
        "o_id", # INTEGER
        "o_d_id", # TINYINT
        "o_w_id", # SMALLINT
        "ol_number", # INTEGER
        "ol_i_id", # INTEGER
        "ol_supply_w_id", # SMALLINT
        "ol_delivery_d", # TIMESTAMP
        "ol_quantity", # INTEGER
        "ol_amount", # FLOAT
        "ol_dist_info", # VARCHAR
    ],
    TABLENAME_HISTORY:    [
        "h_c_id", # INTEGER
        "h_c_d_id", # TINYINT
        "h_c_w_id", # SMALLINT
        "h_d_id", # TINYINT
        "h_w_id", # SMALLINT
        "h_date", # TIMESTAMP
        "h_amount", # FLOAT
        "h_data", # VARCHAR
    ],
    TABLENAME_SUPPLIER:    [
        "su_suppkey", # INTEGER
        "su_name", # VARCHAR
        "su_address", # JSON
        "su_nationkey", # INTEGER
        "su_phone", # VARCHAR
        "su_acctbal", # FLOAT
        "su_comment", # VARCHAR
    ],
    TABLENAME_NATION:    [
        "n_nationkey", # INTEGER
        "n_name", # VARCHAR
        "n_regionkey", # INTEGER
        "n_comment", # VARCHAR
    ],
    TABLENAME_REGION:    [
        "r_regionkey", # INTEGER
        "r_name", # VARCHAR
        "r_comment", # VARCHAR
    ],
    TABLENAME_WAREHOUSE_ADDRESS:    [
        "w_street_1", # VARCHAR
        "w_street_2", # VARCHAR
        "w_city", # VARCHAR
        "w_state", # VARCHAR
        "w_zip", # VARCHAR
    ],
    TABLENAME_DISTRICT_ADDRESS:    [
        "d_street_1", # VARCHAR
        "d_street_2", # VARCHAR
        "d_city", # VARCHAR
        "d_state", # VARCHAR
        "d_zip", # VARCHAR
    ],
    TABLENAME_CUSTOMER_NAME:    [
        "c_first", # VARCHAR
        "c_middle", # VARCHAR
        "c_last", # VARCHAR
    ],
    TABLENAME_CUSTOMER_ADDRESSES:    [
        "c_address_kind", # VARCHAR
        "c_street_1", # VARCHAR
        "c_street_2", # VARCHAR
        "c_city", # VARCHAR
        "c_state", # VARCHAR
        "c_zip", # VARCHAR
    ],
    TABLENAME_CUSTOMER_ADDRESSES_FLAT:    [
        "c_id", # INTEGER
        "c_d_id", # TINYINT
        "c_w_id", # SMALLINT
        "c_address_kind", # VARCHAR
        "c_street_1", # VARCHAR
        "c_street_2", # VARCHAR
        "c_city", # VARCHAR
        "c_state", # VARCHAR
        "c_zip", # VARCHAR
    ],
    TABLENAME_CUSTOMER_PHONES:    [
        "c_phone_kind", # VARCHAR
        "c_phone_number", # VARCHAR
    ],
    TABLENAME_CUSTOMER_PHONES_FLAT:    [
        "c_id", # INTEGER
        "c_d_id", # TINYINT
        "c_w_id", # SMALLINT
        "c_phone_kind", # VARCHAR
        "c_phone_number", # VARCHAR
    ],
    TABLENAME_CUSTOMER_ITEM_CATEGORIES_FLAT:    [
        "c_id", # INTEGER
        "c_d_id", # TINYINT
        "c_w_id", # SMALLINT
        "c_item_category", # VARCHAR
    ],
    TABLENAME_SUPPLIER_ADDRESS:    [
        "su_street_1", # VARCHAR
        "su_street_2", # VARCHAR
        "su_city", # VARCHAR
        "su_state", # VARCHAR
        "su_zip", # VARCHAR
    ],
}

TABLE_INDEXES = {
    TABLENAME_ITEM: [
        "i_id",
    ],
    TABLENAME_ITEM_CATEGORIES_FLAT: [
        "i_id",
        "i_category",
    ],
    TABLENAME_WAREHOUSE: [
        "w_id",
    ],
    TABLENAME_DISTRICT: [
        "d_id",
        "d_w_id",
    ],
    TABLENAME_CUSTOMER:   [
        "c_id",
        "c_d_id",
        "c_w_id",
    ],
    TABLENAME_CUSTOMER_ADDRESSES_FLAT:   [
        "c_id",
        "c_d_id",
        "c_w_id",
        "c_address_kind",
    ],
    TABLENAME_CUSTOMER_PHONES_FLAT:   [
        "c_id",
        "c_d_id",
        "c_w_id",
        "c_phone_number",
    ],
    TABLENAME_CUSTOMER_ITEM_CATEGORIES_FLAT:   [
        "c_id",
        "c_d_id",
        "c_w_id",
        "c_item_category",
    ],
    TABLENAME_STOCK:      [
        "s_i_id",
        "s_w_id",
    ],
    TABLENAME_ORDERS:     [
        "o_id",
        "o_d_id",
        "o_w_id",
        "o_c_id",
    ],
    TABLENAME_NEWORDER:  [
        "no_o_id",
        "no_d_id",
        "no_w_id",
    ],
    TABLENAME_ORDERLINE: [
        "ol_o_id",
        "ol_d_id",
        "ol_w_id",
    ],
    TABLENAME_ORDERLINE_FLAT: [
        "ol_o_id",
        "ol_d_id",
        "ol_w_id",
        "ol_number",
    ],
    TABLENAME_SUPPLIER:    [
        "su_suppkey",
    ],
    TABLENAME_NATION:    [
        "n_nationkey",
    ],
    TABLENAME_REGION:    [
        "r_regionkey",
    ],
}

# Transaction Types
def enum(*sequential, **named):
    enums = dict(map(lambda x: (x, x), sequential))
    # dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)
TransactionTypes = enum(
    "DELIVERY",
    "NEW_ORDER",
    "ORDER_STATUS",
    "PAYMENT",
    "STOCK_LEVEL",
)
QueryTypes = enum(
    "CH2",
)

NUM_CH2_QUERIES = 22

CH2_QUERIES_PERM = [
    ["Q14", "Q02", "Q09", "Q20", "Q06", "Q17", "Q18", "Q08", "Q21", "Q13", "Q03", "Q22", "Q16", "Q04", "Q11", "Q15", "Q01", "Q10", "Q19", "Q05", "Q07", "Q12"],

    ["Q21", "Q03", "Q18", "Q05", "Q11", "Q07", "Q06", "Q20", "Q17", "Q12", "Q16", "Q15", "Q13", "Q10", "Q02", "Q08", "Q14", "Q19", "Q09", "Q22", "Q01", "Q04"],

    ["Q06", "Q17", "Q14", "Q16", "Q19", "Q10", "Q09", "Q02", "Q15", "Q08", "Q05", "Q22", "Q12", "Q07", "Q13", "Q18", "Q01", "Q04", "Q20", "Q03", "Q11", "Q21"],

    ["Q08", "Q05", "Q04", "Q06", "Q17", "Q07", "Q01", "Q18", "Q22", "Q14", "Q09", "Q10", "Q15", "Q11", "Q20", "Q02", "Q21", "Q19", "Q13", "Q16", "Q12", "Q03"],

    ["Q05", "Q21", "Q14", "Q19", "Q15", "Q17", "Q12", "Q06", "Q04", "Q09", "Q08", "Q16", "Q11", "Q02", "Q10", "Q18", "Q01", "Q13", "Q07", "Q22", "Q03", "Q20"],

    ["Q21", "Q15", "Q04", "Q06", "Q07", "Q16", "Q19", "Q18", "Q14", "Q22", "Q11", "Q13", "Q03", "Q01", "Q02", "Q05", "Q08", "Q20", "Q12", "Q17", "Q10", "Q09"],

    ["Q10", "Q03", "Q15", "Q13", "Q06", "Q08", "Q09", "Q07", "Q04", "Q11", "Q22", "Q18", "Q12", "Q01", "Q05", "Q16", "Q02", "Q14", "Q19", "Q20", "Q17", "Q21"],

    ["Q18", "Q08", "Q20", "Q21", "Q02", "Q04", "Q22", "Q17", "Q01", "Q11", "Q09", "Q19", "Q03", "Q13", "Q05", "Q07", "Q10", "Q16", "Q06", "Q14", "Q15", "Q12"],

    ["Q19", "Q01", "Q15", "Q17", "Q05", "Q08", "Q09", "Q12", "Q14", "Q07", "Q04", "Q03", "Q20", "Q16", "Q06", "Q22", "Q10", "Q13", "Q02", "Q21", "Q18", "Q11"],

    ["Q08", "Q13", "Q02", "Q20", "Q17", "Q03", "Q06", "Q21", "Q18", "Q11", "Q19", "Q10", "Q15", "Q04", "Q22", "Q01", "Q07", "Q12", "Q09", "Q14", "Q05", "Q16"],

    ["Q06", "Q15", "Q18", "Q17", "Q12", "Q01", "Q07", "Q02", "Q22", "Q13", "Q21", "Q10", "Q14", "Q09", "Q03", "Q16", "Q20", "Q19", "Q11", "Q04", "Q08", "Q05"],

    ["Q15", "Q14", "Q18", "Q17", "Q10", "Q20", "Q16", "Q11", "Q01", "Q08", "Q04", "Q22", "Q05", "Q12", "Q03", "Q09", "Q21", "Q02", "Q13", "Q06", "Q19", "Q07"],

    ["Q01", "Q07", "Q16", "Q17", "Q18", "Q22", "Q12", "Q06", "Q08", "Q09", "Q11", "Q04", "Q02", "Q05", "Q20", "Q21", "Q13", "Q10", "Q19", "Q03", "Q14", "Q15"],

    ["Q21", "Q17", "Q07", "Q03", "Q01", "Q10", "Q12", "Q22", "Q09", "Q16", "Q06", "Q11", "Q02", "Q04", "Q05", "Q14", "Q08", "Q20", "Q13", "Q18", "Q15", "Q19"],

    ["Q02", "Q09", "Q05", "Q04", "Q18", "Q01", "Q20", "Q15", "Q16", "Q17", "Q07", "Q21", "Q13", "Q14", "Q19", "Q08", "Q22", "Q11", "Q10", "Q03", "Q12", "Q06"],

    ["Q16", "Q09", "Q17", "Q08", "Q14", "Q11", "Q10", "Q12", "Q06", "Q21", "Q07", "Q03", "Q15", "Q05", "Q22", "Q20", "Q01", "Q13", "Q19", "Q02", "Q04", "Q18"],

    ["Q01", "Q03", "Q06", "Q05", "Q02", "Q16", "Q14", "Q22", "Q17", "Q20", "Q04", "Q09", "Q10", "Q11", "Q15", "Q08", "Q12", "Q19", "Q18", "Q13", "Q07", "Q21"],

    ["Q03", "Q16", "Q05", "Q11", "Q21", "Q09", "Q02", "Q15", "Q10", "Q18", "Q17", "Q07", "Q08", "Q19", "Q14", "Q13", "Q01", "Q04", "Q22", "Q20", "Q06", "Q12"],

    ["Q14", "Q04", "Q13", "Q05", "Q21", "Q11", "Q08", "Q06", "Q03", "Q17", "Q02", "Q20", "Q01", "Q19", "Q10", "Q09", "Q12", "Q18", "Q15", "Q07", "Q22", "Q16"],

    ["Q04", "Q12", "Q22", "Q14", "Q05", "Q15", "Q16", "Q02", "Q08", "Q10", "Q17", "Q09", "Q21", "Q07", "Q03", "Q06", "Q13", "Q18", "Q11", "Q20", "Q19", "Q01"],

    ["Q16", "Q15", "Q14", "Q13", "Q04", "Q22", "Q18", "Q19", "Q07", "Q01", "Q12", "Q17", "Q05", "Q10", "Q20", "Q03", "Q09", "Q21", "Q11", "Q02", "Q06", "Q08"],

    ["Q20", "Q14", "Q21", "Q12", "Q15", "Q17", "Q04", "Q19", "Q13", "Q10", "Q11", "Q01", "Q16", "Q05", "Q18", "Q07", "Q08", "Q22", "Q09", "Q06", "Q03", "Q02"],

    ["Q16", "Q14", "Q13", "Q02", "Q21", "Q10", "Q11", "Q04", "Q01", "Q22", "Q18", "Q12", "Q19", "Q05", "Q07", "Q08", "Q06", "Q03", "Q15", "Q20", "Q09", "Q17"],

    ["Q18", "Q15", "Q09", "Q14", "Q12", "Q02", "Q08", "Q11", "Q22", "Q21", "Q16", "Q01", "Q06", "Q17", "Q05", "Q10", "Q19", "Q04", "Q20", "Q13", "Q03", "Q07"],

    ["Q07", "Q03", "Q10", "Q14", "Q13", "Q21", "Q18", "Q06", "Q20", "Q04", "Q09", "Q08", "Q22", "Q15", "Q02", "Q01", "Q05", "Q12", "Q19", "Q17", "Q11", "Q16"],

    ["Q18", "Q01", "Q13", "Q07", "Q16", "Q10", "Q14", "Q02", "Q19", "Q05", "Q21", "Q11", "Q22", "Q15", "Q08", "Q17", "Q20", "Q03", "Q04", "Q12", "Q06", "Q09"],

    ["Q13", "Q02", "Q22", "Q05", "Q11", "Q21", "Q20", "Q14", "Q07", "Q10", "Q04", "Q09", "Q19", "Q18", "Q06", "Q03", "Q01", "Q08", "Q15", "Q12", "Q17", "Q16"],

    ["Q14", "Q17", "Q21", "Q08", "Q02", "Q09", "Q06", "Q04", "Q05", "Q13", "Q22", "Q07", "Q15", "Q03", "Q01", "Q18", "Q16", "Q11", "Q10", "Q12", "Q20", "Q19"],

    ["Q10", "Q22", "Q01", "Q12", "Q13", "Q18", "Q21", "Q20", "Q02", "Q14", "Q16", "Q07", "Q15", "Q03", "Q04", "Q17", "Q05", "Q19", "Q06", "Q08", "Q09", "Q11"],

    ["Q10", "Q08", "Q09", "Q18", "Q12", "Q06", "Q01", "Q05", "Q20", "Q11", "Q17", "Q22", "Q16", "Q03", "Q13", "Q02", "Q15", "Q21", "Q14", "Q19", "Q07", "Q04"],

    ["Q07", "Q17", "Q22", "Q05", "Q03", "Q10", "Q13", "Q18", "Q09", "Q01", "Q14", "Q15", "Q21", "Q19", "Q16", "Q12", "Q08", "Q06", "Q11", "Q20", "Q04", "Q02"],

    ["Q02", "Q09", "Q21", "Q03", "Q04", "Q07", "Q01", "Q11", "Q16", "Q05", "Q20", "Q19", "Q18", "Q08", "Q17", "Q13", "Q10", "Q12", "Q15", "Q06", "Q14", "Q22"],

    ["Q15", "Q12", "Q08", "Q04", "Q22", "Q13", "Q16", "Q17", "Q18", "Q03", "Q07", "Q05", "Q06", "Q01", "Q09", "Q11", "Q21", "Q10", "Q14", "Q20", "Q19", "Q02"],

    ["Q15", "Q16", "Q02", "Q11", "Q17", "Q07", "Q05", "Q14", "Q20", "Q04", "Q21", "Q03", "Q10", "Q09", "Q12", "Q08", "Q13", "Q06", "Q18", "Q19", "Q22", "Q01"],

    ["Q01", "Q13", "Q11", "Q03", "Q04", "Q21", "Q06", "Q14", "Q15", "Q22", "Q18", "Q09", "Q07", "Q05", "Q10", "Q20", "Q12", "Q16", "Q17", "Q08", "Q19", "Q02"],

    ["Q14", "Q17", "Q22", "Q20", "Q08", "Q16", "Q05", "Q10", "Q01", "Q13", "Q02", "Q21", "Q12", "Q09", "Q04", "Q18", "Q03", "Q07", "Q06", "Q19", "Q15", "Q11"],

    ["Q09", "Q17", "Q07", "Q04", "Q05", "Q13", "Q21", "Q18", "Q11", "Q03", "Q22", "Q01", "Q06", "Q16", "Q20", "Q14", "Q15", "Q10", "Q08", "Q02", "Q12", "Q19"],

    ["Q13", "Q14", "Q05", "Q22", "Q19", "Q11", "Q09", "Q06", "Q18", "Q15", "Q08", "Q10", "Q07", "Q04", "Q17", "Q16", "Q03", "Q01", "Q12", "Q02", "Q21", "Q20"],

    ["Q20", "Q05", "Q04", "Q14", "Q11", "Q01", "Q06", "Q16", "Q08", "Q22", "Q07", "Q03", "Q02", "Q12", "Q21", "Q19", "Q17", "Q13", "Q10", "Q15", "Q18", "Q09"],

    ["Q03", "Q07", "Q14", "Q15", "Q06", "Q05", "Q21", "Q20", "Q18", "Q10", "Q04", "Q16", "Q19", "Q01", "Q13", "Q09", "Q08", "Q17", "Q11", "Q12", "Q22", "Q02"],

    ["Q13", "Q15", "Q17", "Q01", "Q22", "Q11", "Q03", "Q04", "Q07", "Q20", "Q14", "Q21", "Q09", "Q08", "Q02", "Q18", "Q16", "Q06", "Q10", "Q12", "Q05",  "Q19"]
]

