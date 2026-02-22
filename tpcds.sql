CREATE EXTERNAL TABLE catalog_sales (
    cs_sold_date_sk          INT,
    cs_sold_time_sk          INT,
    cs_ship_date_sk          INT,
    cs_bill_customer_sk      INT,
    cs_bill_cdemo_sk         INT,
    cs_bill_hdemo_sk         INT,
    cs_bill_addr_sk          INT,
    cs_ship_customer_sk      INT,
    cs_ship_cdemo_sk         INT,
    cs_ship_hdemo_sk         INT,
    cs_ship_addr_sk          INT,
    cs_call_center_sk        INT,
    cs_catalog_page_sk       INT,
    cs_ship_mode_sk          INT,
    cs_warehouse_sk          INT,
    cs_item_sk               INT NOT NULL,
    cs_promo_sk              INT,
    cs_order_number          INT NOT NULL,
    cs_quantity              INT,
    cs_wholesale_cost        DECIMAL(7,2),
    cs_list_price            DECIMAL(7,2),
    cs_sales_price           DECIMAL(7,2),
    cs_ext_discount_amt      DECIMAL(7,2),
    cs_ext_sales_price       DECIMAL(7,2),
    cs_ext_wholesale_cost    DECIMAL(7,2),
    cs_ext_list_price        DECIMAL(7,2),
    cs_ext_tax               DECIMAL(7,2),
    cs_coupon_amt            DECIMAL(7,2),
    cs_ext_ship_cost         DECIMAL(7,2),
    cs_net_paid              DECIMAL(7,2),
    cs_net_paid_inc_tax      DECIMAL(7,2),
    cs_net_paid_inc_ship     DECIMAL(7,2),
    cs_net_paid_inc_ship_tax DECIMAL(7,2),
    cs_net_profit            DECIMAL(7,2)
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/catalog_sales.parquet';

CREATE EXTERNAL TABLE catalog_returns (
    cr_returned_date_sk      INT,
    cr_returned_time_sk      INT,
    cr_item_sk               INT NOT NULL,
    cr_refunded_customer_sk  INT,
    cr_refunded_cdemo_sk     INT,
    cr_refunded_hdemo_sk     INT,
    cr_refunded_addr_sk      INT,
    cr_returning_customer_sk INT,
    cr_returning_cdemo_sk    INT,
    cr_returning_hdemo_sk    INT,
    cr_returning_addr_sk     INT,
    cr_call_center_sk        INT,
    cr_catalog_page_sk       INT,
    cr_ship_mode_sk          INT,
    cr_warehouse_sk          INT,
    cr_reason_sk             INT,
    cr_order_number          INT NOT NULL,
    cr_return_quantity        INT,
    cr_return_amount         DECIMAL(7,2),
    cr_return_tax            DECIMAL(7,2),
    cr_return_amt_inc_tax    DECIMAL(7,2),
    cr_fee                   DECIMAL(7,2),
    cr_return_ship_cost      DECIMAL(7,2),
    cr_refunded_cash         DECIMAL(7,2),
    cr_reversed_charge       DECIMAL(7,2),
    cr_store_credit          DECIMAL(7,2),
    cr_net_loss              DECIMAL(7,2)
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/catalog_returns.parquet';

CREATE EXTERNAL TABLE store_sales (
    ss_sold_date_sk          INT,
    ss_sold_time_sk          INT,
    ss_item_sk               INT NOT NULL,
    ss_customer_sk           INT,
    ss_cdemo_sk              INT,
    ss_hdemo_sk              INT,
    ss_addr_sk               INT,
    ss_store_sk              INT,
    ss_promo_sk              INT,
    ss_ticket_number         INT NOT NULL,
    ss_quantity              INT,
    ss_wholesale_cost        DECIMAL(7,2),
    ss_list_price            DECIMAL(7,2),
    ss_sales_price           DECIMAL(7,2),
    ss_ext_discount_amt      DECIMAL(7,2),
    ss_ext_sales_price       DECIMAL(7,2),
    ss_ext_wholesale_cost    DECIMAL(7,2),
    ss_ext_list_price        DECIMAL(7,2),
    ss_ext_tax               DECIMAL(7,2),
    ss_coupon_amt            DECIMAL(7,2),
    ss_net_paid              DECIMAL(7,2),
    ss_net_paid_inc_tax      DECIMAL(7,2),
    ss_net_profit            DECIMAL(7,2)
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/store_sales.parquet';

CREATE EXTERNAL TABLE store_returns (
    sr_returned_date_sk      INT,
    sr_return_time_sk        INT,
    sr_item_sk               INT NOT NULL,
    sr_customer_sk           INT,
    sr_cdemo_sk              INT,
    sr_hdemo_sk              INT,
    sr_addr_sk               INT,
    sr_store_sk              INT,
    sr_reason_sk             INT,
    sr_ticket_number         INT NOT NULL,
    sr_return_quantity        INT,
    sr_return_amt            DECIMAL(7,2),
    sr_return_tax            DECIMAL(7,2),
    sr_return_amt_inc_tax    DECIMAL(7,2),
    sr_fee                   DECIMAL(7,2),
    sr_return_ship_cost      DECIMAL(7,2),
    sr_refunded_cash         DECIMAL(7,2),
    sr_reversed_charge       DECIMAL(7,2),
    sr_store_credit          DECIMAL(7,2),
    sr_net_loss              DECIMAL(7,2)
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/store_returns.parquet';

CREATE EXTERNAL TABLE date_dim (
    d_date_sk                INT NOT NULL,
    d_date_id                VARCHAR NOT NULL,
    d_date                   DATE,
    d_month_seq              INT,
    d_week_seq               INT,
    d_quarter_seq            INT,
    d_year                   INT,
    d_dow                    INT,
    d_moy                    INT,
    d_dom                    INT,
    d_qoy                    INT,
    d_fy_year                INT,
    d_fy_quarter_seq         INT,
    d_fy_week_seq            INT,
    d_day_name               VARCHAR,
    d_quarter_name           VARCHAR,
    d_holiday                VARCHAR,
    d_weekend                VARCHAR,
    d_following_holiday      VARCHAR,
    d_first_dom              INT,
    d_last_dom               INT,
    d_same_day_1y            INT,
    d_same_day_1q            INT,
    d_current_day            VARCHAR,
    d_current_week           VARCHAR,
    d_current_month          VARCHAR,
    d_current_quarter        VARCHAR,
    d_current_year           VARCHAR
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/date_dim.parquet';

CREATE EXTERNAL TABLE store (
    s_store_sk               INT NOT NULL,
    s_store_id               VARCHAR NOT NULL,
    s_rec_start_date         DATE,
    s_rec_end_date           DATE,
    s_closed_date_sk         INT,
    s_store_name             VARCHAR,
    s_number_employees       INT,
    s_floor_space            INT,
    s_hours                  VARCHAR,
    s_manager                VARCHAR,
    s_market_id              INT,
    s_geography_class        VARCHAR,
    s_market_desc            VARCHAR,
    s_market_manager         VARCHAR,
    s_division_id            INT,
    s_division_name          VARCHAR,
    s_company_id             INT,
    s_company_name           VARCHAR,
    s_street_number          VARCHAR,
    s_street_name            VARCHAR,
    s_street_type            VARCHAR,
    s_suite_number           VARCHAR,
    s_city                   VARCHAR,
    s_county                 VARCHAR,
    s_state                  VARCHAR,
    s_zip                    VARCHAR,
    s_country                VARCHAR,
    s_gmt_offset             DECIMAL(5,2),
    s_tax_percentage         DECIMAL(5,2)
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/store.parquet';

CREATE EXTERNAL TABLE customer (
    c_customer_sk            INT NOT NULL,
    c_customer_id            VARCHAR NOT NULL,
    c_current_cdemo_sk       INT,
    c_current_hdemo_sk       INT,
    c_current_addr_sk        INT,
    c_first_shipto_date_sk   INT,
    c_first_sales_date_sk    INT,
    c_salutation             VARCHAR,
    c_first_name             VARCHAR,
    c_last_name              VARCHAR,
    c_preferred_cust_flag    VARCHAR,
    c_birth_day              INT,
    c_birth_month            INT,
    c_birth_year             INT,
    c_birth_country          VARCHAR,
    c_login                  VARCHAR,
    c_email_address          VARCHAR,
    c_last_review_date_sk    INT
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/customer.parquet';

CREATE EXTERNAL TABLE customer_demographics (
    cd_demo_sk               INT NOT NULL,
    cd_gender                VARCHAR,
    cd_marital_status        VARCHAR,
    cd_education_status      VARCHAR,
    cd_purchase_estimate     INT,
    cd_credit_rating         VARCHAR,
    cd_dep_count             INT,
    cd_dep_employed_count    INT,
    cd_dep_college_count     INT
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/customer_demographics.parquet';

CREATE EXTERNAL TABLE promotion (
    p_promo_sk               INT NOT NULL,
    p_promo_id               VARCHAR NOT NULL,
    p_start_date_sk          INT,
    p_end_date_sk            INT,
    p_item_sk                INT,
    p_cost                   DECIMAL(15,2),
    p_response_target        INT,
    p_promo_name             VARCHAR,
    p_channel_dmail          VARCHAR,
    p_channel_email          VARCHAR,
    p_channel_catalog        VARCHAR,
    p_channel_tv             VARCHAR,
    p_channel_radio          VARCHAR,
    p_channel_press          VARCHAR,
    p_channel_event          VARCHAR,
    p_channel_demo           VARCHAR,
    p_channel_details        VARCHAR,
    p_purpose                VARCHAR,
    p_discount_active        VARCHAR
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/promotion.parquet';

CREATE EXTERNAL TABLE household_demographics (
    hd_demo_sk               INT NOT NULL,
    hd_income_band_sk        INT,
    hd_buy_potential         VARCHAR,
    hd_dep_count             INT,
    hd_vehicle_count         INT
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/household_demographics.parquet';

CREATE EXTERNAL TABLE customer_address (
    ca_address_sk            INT NOT NULL,
    ca_address_id            VARCHAR NOT NULL,
    ca_street_number         VARCHAR,
    ca_street_name           VARCHAR,
    ca_street_type           VARCHAR,
    ca_suite_number          VARCHAR,
    ca_city                  VARCHAR,
    ca_county                VARCHAR,
    ca_state                 VARCHAR,
    ca_zip                   VARCHAR,
    ca_country               VARCHAR,
    ca_gmt_offset            DECIMAL(5,2),
    ca_location_type         VARCHAR
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/customer_address.parquet';

CREATE EXTERNAL TABLE income_band (
    ib_income_band_sk        INT NOT NULL,
    ib_lower_bound           INT,
    ib_upper_bound           INT
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/income_band.parquet';

CREATE EXTERNAL TABLE item (
    i_item_sk                INT NOT NULL,
    i_item_id                VARCHAR NOT NULL,
    i_rec_start_date         DATE,
    i_rec_end_date           DATE,
    i_item_desc              VARCHAR,
    i_current_price          DECIMAL(7,2),
    i_wholesale_cost         DECIMAL(7,2),
    i_brand_id               INT,
    i_brand                  VARCHAR,
    i_class_id               INT,
    i_class                  VARCHAR,
    i_category_id            INT,
    i_category               VARCHAR,
    i_manufact_id            INT,
    i_manufact               VARCHAR,
    i_size                   VARCHAR,
    i_formulation            VARCHAR,
    i_color                  VARCHAR,
    i_units                  VARCHAR,
    i_container              VARCHAR,
    i_manager_id             INT,
    i_product_name           VARCHAR
) STORED AS PARQUET LOCATION 'benchmarks/data/tpcds_sf1/item.parquet';

with cs_ui as
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales
      ,catalog_returns
  where cs_item_sk = cr_item_sk
    and cs_order_number = cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
cross_sales as
 (select i_product_name product_name
     ,i_item_sk item_sk
     ,s_store_name store_name
     ,s_zip store_zip
     ,ad1.ca_street_number b_street_number
     ,ad1.ca_street_name b_street_name
     ,ad1.ca_city b_city
     ,ad1.ca_zip b_zip
     ,ad2.ca_street_number c_street_number
     ,ad2.ca_street_name c_street_name
     ,ad2.ca_city c_city
     ,ad2.ca_zip c_zip
     ,d1.d_year as syear
     ,d2.d_year as fsyear
     ,d3.d_year s2year
     ,count(*) cnt
     ,sum(ss_wholesale_cost) s1
     ,sum(ss_list_price) s2
     ,sum(ss_coupon_amt) s3
  FROM   store_sales
        ,store_returns
        ,cs_ui
        ,date_dim d1
        ,date_dim d2
        ,date_dim d3
        ,store
        ,customer
        ,customer_demographics cd1
        ,customer_demographics cd2
        ,promotion
        ,household_demographics hd1
        ,household_demographics hd2
        ,customer_address ad1
        ,customer_address ad2
        ,income_band ib1
        ,income_band ib2
        ,item
  WHERE  ss_store_sk = s_store_sk AND
         ss_sold_date_sk = d1.d_date_sk AND
         ss_customer_sk = c_customer_sk AND
         ss_cdemo_sk= cd1.cd_demo_sk AND
         ss_hdemo_sk = hd1.hd_demo_sk AND
         ss_addr_sk = ad1.ca_address_sk and
         ss_item_sk = i_item_sk and
         ss_item_sk = sr_item_sk and
         ss_ticket_number = sr_ticket_number and
         ss_item_sk = cs_ui.cs_item_sk and
         c_current_cdemo_sk = cd2.cd_demo_sk AND
         c_current_hdemo_sk = hd2.hd_demo_sk AND
         c_current_addr_sk = ad2.ca_address_sk and
         c_first_sales_date_sk = d2.d_date_sk and
         c_first_shipto_date_sk = d3.d_date_sk and
         ss_promo_sk = p_promo_sk and
         hd1.hd_income_band_sk = ib1.ib_income_band_sk and
         hd2.hd_income_band_sk = ib2.ib_income_band_sk and
         cd1.cd_marital_status <> cd2.cd_marital_status and
         i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
         i_current_price between 35 and 35 + 10 and
         i_current_price between 35 + 1 and 35 + 15
group by i_product_name
       ,i_item_sk
       ,s_store_name
       ,s_zip
       ,ad1.ca_street_number
       ,ad1.ca_street_name
       ,ad1.ca_city
       ,ad1.ca_zip
       ,ad2.ca_street_number
       ,ad2.ca_street_name
       ,ad2.ca_city
       ,ad2.ca_zip
       ,d1.d_year
       ,d2.d_year
       ,d3.d_year
)
select cs1.product_name
     ,cs1.store_name
     ,cs1.store_zip
     ,cs1.b_street_number
     ,cs1.b_street_name
     ,cs1.b_city
     ,cs1.b_zip
     ,cs1.c_street_number
     ,cs1.c_street_name
     ,cs1.c_city
     ,cs1.c_zip
     ,cs1.syear
     ,cs1.cnt
     ,cs1.s1 as s11
     ,cs1.s2 as s21
     ,cs1.s3 as s31
     ,cs2.s1 as s12
     ,cs2.s2 as s22
     ,cs2.s3 as s32
     ,cs2.syear
     ,cs2.cnt
from cross_sales cs1,cross_sales cs2
where cs1.item_sk=cs2.item_sk and
     cs1.syear = 2000 and
     cs2.syear = 2000 + 1 and
     cs2.cnt <= cs1.cnt and
     cs1.store_name = cs2.store_name and
     cs1.store_zip = cs2.store_zip
order by cs1.product_name
       ,cs1.store_name
       ,cs2.cnt
       ,cs1.s1
       ,cs2.s1;
