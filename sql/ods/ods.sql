DROP TABLE IF EXISTS ods_shop_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_shop_full
(
    `id`           STRING COMMENT '商店ID',
    `create_time`  STRING COMMENT '创建时间',
    `update_time`  STRING COMMENT '修改时间',
    `name`         STRING COMMENT '商铺名称',
    `phone_number` STRING COMMENT '联系电话',
    `type`         STRING COMMENT '商铺类型：1.直营 2加盟',
    `region_id`    STRING COMMENT '地区'
) COMMENT '店铺全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_shop_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_region_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_region_full
(
    `id`              STRING COMMENT '地区ID',
    `create_time`     STRING COMMENT '创建时间',
    `update_time`     STRING COMMENT '修改时间',
    `level`           STRING COMMENT '行政级别: 1.省级 2.地市级',
    `name`            STRING COMMENT '区划名称',
    `superior_region` STRING COMMENT '上级区划',
    `zip_code`        STRING COMMENT '邮编'
) COMMENT '行政区划全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_region_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_promotion_full;
create external table ods_promotion_full(
    id bigint,
    create_time string,
    update_time string,
    company_share decimal(16,2),
    name string,
    reduce_amount decimal(16,2),
    threshold_amount decimal(16,2)
) comment "优惠活动表"
    partitioned by (dt string)
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/fast_food/ods/ods_promotion_full'
TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');



DROP TABLE IF EXISTS ods_product_sku_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_sku_full
(
    `id`                  STRING COMMENT '菜品规格ID',
    `create_time`         STRING COMMENT '创建时间',
    `update_time`         STRING COMMENT '修改时间',
    `name`                STRING COMMENT '规格名称',
    `price`               DECIMAL(16, 2) COMMENT '价格',
    `product_category_id` STRING COMMENT '所属分类',
    `product_spu_id`      STRING COMMENT '所属菜品'
) COMMENT '菜品规格全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_product_sku_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_product_spu_attr_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_spu_attr_full
(
    `id`             STRING COMMENT '菜品属性ID',
    `create_time`    STRING COMMENT '创建时间',
    `update_time`    STRING COMMENT '修改时间',
    `attr_name`      STRING COMMENT '属性名称',
    `product_spu_id` STRING COMMENT '所属菜品'
) COMMENT '菜品属性全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_product_spu_attr_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_product_spu_attr_value_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_spu_attr_value_full
(
    `id`                  STRING COMMENT '菜品属性值ID',
    `create_time`         STRING COMMENT '创建时间',
    `update_time`         STRING COMMENT '修改时间',
    `attr_value`          STRING COMMENT '属性值',
    `product_spu_id`      STRING COMMENT '所属菜品',
    `product_spu_attr_id` STRING COMMENT '属性ID'
) COMMENT '菜品属性值全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_product_spu_attr_value_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_product_spu_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_spu_full
(
    `id`                  STRING COMMENT '菜品ID',
    `create_time`         STRING COMMENT '创建时间',
    `update_time`         STRING COMMENT '修改时间',
    `description`         STRING COMMENT '菜品描述',
    `name`                STRING COMMENT '菜品名称',
    `product_category_id` STRING COMMENT '所属分类'
) COMMENT '菜品全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_product_spu_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_product_group_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_group_full
(
    `id`             STRING COMMENT '套餐ID',
    `create_time`    STRING COMMENT '创建时间',
    `update_time`    STRING COMMENT '修改时间',
    `name`           STRING COMMENT '套餐名称',
    `original_price` DECIMAL(16, 2) COMMENT '套餐原价',
    `price`          DECIMAL(16, 2) COMMENT '套餐价格'
) COMMENT '套餐全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_product_group_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_product_group_sku_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_group_sku_full
(
    `product_group_id` STRING COMMENT '套餐ID',
    `product_sku_id`   STRING COMMENT '菜品规格ID'
) COMMENT '套餐菜品关联全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_product_group_sku_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_product_category_full;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_category_full
(
    `id`          STRING COMMENT '菜品分类ID',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '修改时间',
    `description` STRING COMMENT '描述',
    `name`        STRING COMMENT '分类名称'
) COMMENT '菜品分类全量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        NULL DEFINED AS ''
    LOCATION '/warehouse/fast_food/ods/ods_product_category_full'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_customer_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_customer_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id`:STRING,
                  `create_time`:STRING,
                  `update_time`:STRING,
                  `phone_number`:STRING,
                  `username`:STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '顾客增量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/fast_food/ods/ods_customer_inc'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_order_info_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id`:STRING,
                  `create_time`:STRING,
                  `update_time`:STRING,
                  `actual_amount`:DECIMAL(16,2),
                  `comment`:STRING,
                  `original_amount`:DECIMAL(16,2),
                  `rating`:TINYINT,
                  `reduce_amount`:DECIMAL(16,2),
                  `status`:STRING,
                  `customer_id`:STRING,
                  `promotion_id`:STRING,
                  `shop_id`:STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单增量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/fast_food/ods/ods_order_info_inc'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_order_detail_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_detail_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id`:STRING,
                  `create_time`:STRING,
                  `update_time`:STRING,
                  `amount`:DECIMAL(16,2),
                  `sku_num`:INT,
                  `customer_id`:STRING,
                  `order_info_id`:STRING,
                  `product_group_id`:STRING,
                  `product_sku_id`:STRING,
                  `shop_id`:STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细增量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/fast_food/ods/ods_order_detail_inc'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_order_status_log_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_order_status_log_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id`:STRING,
                  `create_time`:STRING,
                  `update_time`:STRING,
                  `status`:STRING,
                  `order_info_id`:STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单状态流水增量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/fast_food/ods/ods_order_status_log_inc'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

DROP TABLE IF EXISTS ods_payment_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_payment_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<`id`:STRING,
                  `create_time`:STRING,
                  `update_time`:STRING,
                  `amount`:DECIMAL(16,2),
                  `status`:STRING,
                  `customer_id`:STRING,
                  `order_info_id`:STRING,
                  `shop_id`:STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '支付增量表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/fast_food/ods/ods_payment_inc'
    TBLPROPERTIES ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');