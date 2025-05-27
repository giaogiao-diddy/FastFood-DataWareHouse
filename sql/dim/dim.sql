DROP TABLE IF EXISTS dim_customer_zip;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_customer_zip
(
    `id`           STRING COMMENT '顾客ID',
    `phone_number` STRING COMMENT '手机号',
    `username`     STRING COMMENT '用户名',
    `start_date`   STRING COMMENT '起始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '顾客维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dim/dim_customer_zip'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dim_customer_zip
    partition (dt = '9999-12-31')
select data.id,
       concat(substr(data.phone_number, 0, 3), '*')              phone_number,
       concat(substr(data.username, length(data.username)), '*') username,
       '2023-06-14'                                              start_date,
       '9999-12-31'                                              end_date
from ods_customer_inc
where dt = '2023-06-14'
  and type = 'bootstrap-insert';


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_customer_zip
    partition (dt)
select id,
       phone_number,
       username,
       start_date,
       if(rn = 1, end_date, date_add('2023-06-15', -1)) end_date,
       if(rn = 1, end_date, date_add('2023-06-15', -1)) dt
from (select id,
             phone_number,
             username,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rn
      from (select id,
                   phone_number,
                   username,
                   start_date,
                   end_date
            from dim_customer_zip
            where dt = '9999-12-31'
            union all
            select id,
                   concat(substr(phone_number, 0, 3), '*')         phone_number,
                   concat(substr(username, length(username)), '*') username,
                   '2023-06-15'                                    start_date,
                   '9999-12-31'                                    end_date
            from (select data.id,
                         data.phone_number,
                         data.username,
                         row_number() over (partition by data.id order by ts desc) rk
                  from ods_customer_inc
                  where dt = '2023-06-15') t1
            where rk = 1) t2) t3;

DROP TABLE IF EXISTS dim_product_sku_full;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_product_sku_full
(
    `id`                           STRING COMMENT '菜品规格ID',
    `name`                         STRING COMMENT '规格名称',
    `price`                        DECIMAL(16, 2) COMMENT '价格',
    `product_category_id`          STRING COMMENT '所属分类',
    `product_category_description` STRING COMMENT '所属分类描述',
    `product_category_name`        STRING COMMENT '所属分类名称',
    `product_spu_id`               STRING COMMENT '所属菜品',
    `product_spu_description`      STRING COMMENT '所属菜品描述',
    `product_spu_name`             STRING COMMENT '所属菜品名称',
    `product_spu_attr`             ARRAY<STRUCT<
                                       product_spu_attr_id:STRING,
                                       product_spu_attr_name:STRING,
                                       attr_values:ARRAY<STRUCT<
                                           product_spu_attr_value_id:STRING,
                                           product_spu_attr_value:STRING>>>> COMMENT '菜品属性值'
) COMMENT '菜品维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dim/dim_product_sku_full'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dim_product_sku_full
    partition (dt = '2023-06-14')
select sku.id,
       sku.name,
       sku.price,
       sku.product_category_id,
       category.description product_category_description,
       category.name        product_category_name,
       sku.product_spu_id,
       spu_full.product_spu_description,
       spu_full.product_spu_name,
       spu_full.product_spu_attr
from (select id,
             name,
             price,
             product_category_id,
             product_spu_id
      from ods_product_sku_full
      where dt = '2023-06-14') sku
         left join
     (select id,
             description,
             name
      from ods_product_category_full
      where dt = '2023-06-14') category
     on sku.product_category_id = category.id
         left join
     (select spu.product_spu_id,
             description product_spu_description,
             name        product_spu_name,
             collect_set(named_struct(
                     'product_spu_attr_id', product_spu_attr_id,
                     'product_spu_attr_name', product_spu_attr_name,
                     'attr_values', attr_values
                 ))      product_spu_attr
      from (select id product_spu_id,
                   description,
                   name
            from ods_product_spu_full
            where dt = '2023-06-14') spu
               left join
           (select spu_attr.product_spu_attr_id,
                   product_spu_attr_name,
                   product_spu_id,
                   collect_set(
                           named_struct('product_spu_attr_value_id', product_spu_attr_value_id,
                                        'product_spu_attr_value', product_spu_attr_value)
                       ) attr_values
            from (select id        product_spu_attr_id,
                         attr_name product_spu_attr_name,
                         product_spu_id
                  from ods_product_spu_attr_full
                  where dt = '2023-06-14') spu_attr
                     left join
                 (select id         product_spu_attr_value_id,
                         attr_value product_spu_attr_value,
                         product_spu_attr_id
                  from ods_product_spu_attr_value_full
                  where dt = '2023-06-14') spu_attr_value
                 on spu_attr.product_spu_attr_id = spu_attr_value.product_spu_attr_id
            group by spu_attr.product_spu_attr_id,
                     product_spu_attr_name,
                     product_spu_id) spu_attr_full
           on spu.product_spu_id = spu_attr_full.product_spu_id
      group by spu.product_spu_id,
               description,
               name) spu_full
     on sku.product_spu_id = spu_full.product_spu_id;

DROP TABLE IF EXISTS dim_product_group_full;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_product_group_full
(
    `id`              STRING COMMENT '套餐ID',
    `name`            STRING COMMENT '套餐名称',
    `original_price`  DECIMAL(16, 2) COMMENT '套餐原价',
    `price`           DECIMAL(16, 2) COMMENT '套餐价格',
    `product_sku_ids` ARRAY<STRING> COMMENT '菜品规格ID列表'
) COMMENT '套餐维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dim/dim_product_group_full'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert overwrite table dim_product_group_full
    partition (dt = '2023-06-14')
select id,
       name,
       original_price,
       price,
       collect_set(product_sku_id) product_sku_ids
from (select id,
             name,
             original_price,
             price
      from ods_product_group_full
      where dt = '2023-06-14') group_info
         left join
     (select product_group_id,
             product_sku_id
      from ods_product_group_sku_full
      where dt = '2023-06-14') group_sku
     on group_info.id = product_group_id
group by id,
         name,
         original_price,
         price;


DROP TABLE IF EXISTS dim_promotion_full;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_promotion_full
(
    `id`               STRING COMMENT '优惠活动ID',
    `company_share`    DECIMAL(16, 2) COMMENT '公司负担比例',
    `name`             STRING COMMENT '活动名称',
    `reduce_amount`    DECIMAL(16, 2) COMMENT '满减金额',
    `threshold_amount` DECIMAL(16, 2) COMMENT '满减门槛'
) COMMENT '优惠活动维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dim/dim_promotion_full'
    TBLPROPERTIES ('orc.compress' = 'snappy');

DROP TABLE IF EXISTS dim_region_full;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_region_full
(
    `id`              STRING COMMENT '地区ID',
    `level`           STRING COMMENT '行政级别: 1.省级 2.地市级',
    `name`            STRING COMMENT '区划名称',
    `superior_region` STRING COMMENT '上级区划',
    `zip_code`        STRING COMMENT '邮编'
) COMMENT '行政区划维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dim/dim_region_full'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_region_full
    partition (dt = '2023-06-14')
select id,
       level,
       name,
       superior_region,
       zip_code
from ods_region_full
where dt = '2023-06-14';


DROP TABLE IF EXISTS dim_shop_full;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_shop_full
(
    `id`           STRING COMMENT '商店ID',
    `name`         STRING COMMENT '商铺名称',
    `phone_number` STRING COMMENT '联系电话',
    `type`         STRING COMMENT '商铺类型：1.直营 2加盟',
    `region_id`    STRING COMMENT '地区'
) COMMENT '店铺维度表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dim/dim_shop_full'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dim_shop_full
    partition (dt = '2023-06-14')
select id,
       md5(name) name,
       concat(substr(phone_number, 0, 3), '*') phone_number,
       type,
       region_id
from ods_shop_full
where dt = '2023-06-14';


