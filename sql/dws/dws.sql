DROP TABLE IF EXISTS dws_trade_shop_order_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_shop_order_1d
(
    `shop_id`             STRING COMMENT '店铺ID',
    `shop_name`           STRING COMMENT '店铺名称',
    `shop_type`           STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `city_id`             STRING COMMENT '地区ID',
    `city_name`           STRING COMMENT '地区名称',
    `province_id`         STRING COMMENT '省份ID',
    `province_name`       STRING COMMENT '省份名称',
    `order_amount`        DECIMAL(16, 2) COMMENT '下单金额',
    `order_count`         BIGINT COMMENT '下单次数',
    `order_user_count`    BIGINT COMMENT '下单人数',
    `order_reduce_amount` DECIMAL(16, 2) COMMENT '下单减免金额',
    `order_reduce_count`  BIGINT COMMENT '参与活动订单数'
) COMMENT '交易域店铺粒度用户下单最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_shop_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_shop_order_1d
    partition (dt)
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       order_amount,
       order_count,
       order_user_count,
       order_reduce_amount,
       order_reduce_count,
       dt
from (select dt,
             shop_id,
             sum(split_actual_amount)                                          order_amount,
             count(distinct order_info_id)                                     order_count,
             count(distinct customer_id)                                       order_user_count,
             sum(split_reduce_amount)                                          order_reduce_amount,
             count(distinct if(promotion_id is not null, order_info_id, null)) order_reduce_count
      from dwd_trade_order_detail_inc
      group by dt, shop_id) agg
         left join (select id,
                           name      shop_name,
                           type      shop_type,
                           region_id city_id
                    from dim_shop_full
                    where dt = '2023-06-14') shop
                   on agg.shop_id = shop.id
         left join (select id,
                           name            city_name,
                           superior_region province_id
                    from dim_region_full
                    where dt = '2023-06-14'
                      and level = '2') city
                   on shop.city_id = city.id
         left join (select id,
                           name province_name
                    from dim_region_full
                    where dt = '2023-06-14'
                      and level = '1') province
                   on province_id = province.id;

insert overwrite table dws_trade_shop_order_1d
    partition (dt = '2023-06-15')
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       order_amount,
       order_count,
       order_user_count,
       order_reduce_amount,
       order_reduce_count
from (select shop_id,
             sum(split_actual_amount)                                          order_amount,
             count(distinct order_info_id)                                     order_count,
             count(distinct customer_id)                                       order_user_count,
             sum(split_reduce_amount)                                          order_reduce_amount,
             count(distinct if(promotion_id is not null, order_info_id, null)) order_reduce_count
      from dwd_trade_order_detail_inc
      where dt = '2023-06-15'
      group by shop_id) agg
         left join (select id,
                           name      shop_name,
                           type      shop_type,
                           region_id city_id
                    from dim_shop_full
                    where dt = '2023-06-15') shop
                   on agg.shop_id = shop.id
         left join (select id,
                           name            city_name,
                           superior_region province_id
                    from dim_region_full
                    where dt = '2023-06-15'
                      and level = '2') city
                   on shop.city_id = city.id
         left join (select id,
                           name province_name
                    from dim_region_full
                    where dt = '2023-06-15'
                      and level = '1') province
                   on province_id = province.id;

DROP TABLE IF EXISTS dws_trade_shop_pay_suc_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_shop_pay_suc_1d
(
    `shop_id`                     STRING COMMENT '店铺ID',
    `shop_name`                   STRING COMMENT '店铺名称',
    `shop_type`                   STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `city_id`                     STRING COMMENT '地区ID',
    `city_name`                   STRING COMMENT '地区名称',
    `province_id`                 STRING COMMENT '省份ID',
    `province_name`               STRING COMMENT '省份名称',
    `pay_suc_amount`              DECIMAL(16, 2) COMMENT '支付成功金额',
    `reduce_shop_share_amount`    DECIMAL(16, 2) COMMENT '店铺优惠分摊支出金额',
    `reduce_company_share_amount` DECIMAL(16, 2) COMMENT '总公司优惠分摊支出金额'
) COMMENT '交易域店铺粒度用户支付成功最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_shop_pay_suc_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_shop_pay_suc_1d
    partition (dt)
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       pay_suc_amount,
       reduce_shop_share_amount,
       reduce_company_share_amount,
       dt
from (select dt,
             shop_id,
             sum(split_actual_amount)                                 pay_suc_amount,
             sum(split_reduce_amount * (1 - nvl(company_share, 0.0))) reduce_shop_share_amount,
             sum(split_reduce_amount * nvl(company_share, 0.0))       reduce_company_share_amount
      from (select dt,
                   shop_id,
                   split_actual_amount,
                   split_reduce_amount,
                   promotion_id
            from dwd_trade_pay_suc_detail_inc) detail
               left join
           (select id,
                   company_share
            from dim_promotion_full
            where dt = '2023-06-14') promotion
           on detail.promotion_id = promotion.id
      group by dt,
               shop_id) agg
         left join
     (select id,
             name      shop_name,
             type      shop_type,
             region_id city_id
      from dim_shop_full
      where dt = '2023-06-14') shop
     on agg.shop_id = shop.id
         left join
     (select id,
             name            city_name,
             superior_region province_id
      from dim_region_full
      where dt = '2023-06-14'
        and level = '2') city
     on city_id = city.id
         left join
     (select id,
             name province_name
      from dim_region_full
      where dt = '2023-06-14'
        and level = '1') province
     on province_id = province.id;
set hive.execution.engine=spark;
insert overwrite table dws_trade_shop_pay_suc_1d
    partition (dt = '2023-06-15')
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       pay_suc_amount,
       reduce_shop_share_amount,
       reduce_company_share_amount
from (select shop_id,
             sum(split_actual_amount)                                 pay_suc_amount,
             sum(split_reduce_amount * (1 - nvl(company_share, 0.0))) reduce_shop_share_amount,
             sum(split_reduce_amount * nvl(company_share, 0.0))       reduce_company_share_amount
      from (select shop_id,
                   split_actual_amount,
                   split_reduce_amount,
                   promotion_id
            from dwd_trade_pay_suc_detail_inc
            where dt = '2023-06-15') detail
               left join
           (select id,
                   company_share
            from dim_promotion_full
            where dt = '2023-06-15') promotion
           on detail.promotion_id = promotion.id
      group by shop_id) agg
         left join
     (select id,
             name      shop_name,
             type      shop_type,
             region_id city_id
      from dim_shop_full
      where dt = '2023-06-15') shop
     on agg.shop_id = shop.id
         left join
     (select id,
             name            city_name,
             superior_region province_id
      from dim_region_full
      where dt = '2023-06-15'
        and level = '2') city
     on city_id = city.id
         left join
     (select id,
             name province_name
      from dim_region_full
      where dt = '2023-06-15'
        and level = '1') province
     on province_id = province.id;

DROP TABLE IF EXISTS dws_trade_shop_refund_pay_suc_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_shop_refund_pay_suc_1d
(
    `shop_id`               STRING COMMENT '店铺ID',
    `shop_name`             STRING COMMENT '店铺名称',
    `shop_type`             STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `city_id`               STRING COMMENT '地区ID',
    `city_name`             STRING COMMENT '地区名称',
    `province_id`           STRING COMMENT '省份ID',
    `province_name`         STRING COMMENT '省份名称',
    `refund_pay_suc_amount` DECIMAL(16, 2) COMMENT '退款成功金额'
) COMMENT '交易域店铺粒度退款成功最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_shop_refund_pay_suc_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_shop_refund_pay_suc_1d
    partition (dt)
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       refund_pay_suc_amount,
       dt
from (select dt,
             shop_id,
             sum(split_actual_amount) refund_pay_suc_amount
      from dwd_trade_refund_payment_inc
      group by dt,
               shop_id) agg
         left join (select id,
                           name      shop_name,
                           type      shop_type,
                           region_id city_id
                    from dim_shop_full
                    where dt = '2023-06-14') shop
                   on agg.shop_id = shop.id
         left join (select id,
                           name            city_name,
                           superior_region province_id
                    from dim_region_full
                    where dt = '2023-06-14'
                      and level = '2') city
                   on shop.city_id = city.id
         left join (select id,
                           name province_name
                    from dim_region_full
                    where dt = '2023-06-14'
                      and level = '1') province
                   on province_id = province.id;

insert overwrite table dws_trade_shop_refund_pay_suc_1d
    partition (dt = '2023-06-15')
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       refund_pay_suc_amount
from (select shop_id,
             sum(split_actual_amount) refund_pay_suc_amount
      from dwd_trade_refund_payment_inc
      where dt = '2023-06-15'
      group by shop_id) agg
         left join (select id,
                           name      shop_name,
                           type      shop_type,
                           region_id city_id
                    from dim_shop_full
                    where dt = '2023-06-15') shop
                   on agg.shop_id = shop.id
         left join (select id,
                           name            city_name,
                           superior_region province_id
                    from dim_region_full
                    where dt = '2023-06-15'
                      and level = '2') city
                   on shop.city_id = city.id
         left join (select id,
                           name province_name
                    from dim_region_full
                    where dt = '2023-06-15'
                      and level = '1') province
                   on province_id = province.id;

DROP TABLE IF EXISTS dws_trade_promotion_order_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_promotion_order_1d
(
    `promotion_id`               STRING COMMENT '营销活动ID',
    `company_share`              DECIMAL COMMENT '活动支出公司负担比例',
    `promotion_name`             STRING COMMENT '营销活动名称',
    `promotion_reduce_amount`    DECIMAL(16, 2) COMMENT '活动满减金额',
    `promotion_threshold_amount` DECIMAL(16, 2) COMMENT '活动满减门槛',
    `total_reduce_amount`        DECIMAL(16, 2) COMMENT '活动累计减免金额',
    `total_activity_order_count` BIGINT COMMENT '参与活动订单数',
    `total_activity_user_count`  BIGINT COMMENT '活动参与人数'
) COMMENT '交易域营销活动粒度用户下单最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_promotion_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_promotion_order_1d
    partition (dt)
select promotion_id,
       company_share,
       promotion_name,
       promotion_reduce_amount,
       promotion_threshold_amount,
       total_reduce_amount,
       total_activity_order_count,
       total_activity_user_count,
       dt
from (select dt,
             promotion_id,
             sum(split_reduce_amount)                                          total_reduce_amount,
             count(distinct if(promotion_id is not null, order_info_id, null)) total_activity_order_count,
             count(distinct if(promotion_id is not null, customer_id, null))   total_activity_user_count
      from dwd_trade_order_detail_inc
      where promotion_id is not null
      group by dt,
               promotion_id) agg
         left join
     (select id,
             company_share,
             name             promotion_name,
             reduce_amount    promotion_reduce_amount,
             threshold_amount promotion_threshold_amount
      from dim_promotion_full
      where dt = '2023-06-14') promotion
     on agg.promotion_id = promotion.id;

DROP TABLE IF EXISTS dws_trade_product_sku_order_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_product_sku_order_1d
(
    `product_sku_id`               STRING COMMENT '菜品规格ID',
    `product_sku_name`             STRING COMMENT '菜品规格名称',
    `product_sku_price`            STRING COMMENT '菜品规格定价',
    `product_spu_id`               STRING COMMENT '菜品ID',
    `product_spu_description`      STRING COMMENT '菜品描述',
    `product_spu_name`             STRING COMMENT '菜品名称',
    `product_category_id`          STRING COMMENT '菜品分类ID',
    `product_category_description` STRING COMMENT '菜品分类描述',
    `product_category_name`        STRING COMMENT '菜品分类名称',
    `order_amount`                 DECIMAL(16, 2) COMMENT '下单金额',
    `order_user_count`             BIGINT COMMENT '下单人数',
    `order_reduce_count`            BIGINT COMMENT '参与活动订单数',
    `order_reduce_amount`          DECIMAL(16, 2) COMMENT '下单活动减免金额'
) COMMENT '交易域菜品规格粒度用户下单最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_product_sku_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_product_sku_order_1d
    partition (dt)
select product_sku_id,
       product_sku_name,
       product_sku_price,
       product_spu_id,
       product_spu_description,
       product_spu_name,
       product_category_id,
       product_category_description,
       product_category_name,
       order_amount,
       order_user_count,
       order_reduce_count,
       order_reduce_amount,
       dt
from (select dt,
             product_sku_id,
             sum(split_actual_amount)                                          order_amount,
             count(distinct customer_id)                                       order_user_count,
             count(distinct if(promotion_id is not null, order_info_id, null)) order_reduce_count,
             sum(split_reduce_amount)                                          order_reduce_amount
      from dwd_trade_order_detail_inc WHERE dt<='2023-06-14' and product_sku_id is not null
      group by dt,
               product_sku_id) agg
         left join
     (select id,
             name  product_sku_name,
             price product_sku_price,
             product_spu_id,
             product_spu_description,
             product_spu_name,
             product_category_id,
             product_category_description,
             product_category_name
      from dim_product_sku_full
      where dt = '2023-06-14') sku
     on agg.product_sku_id = sku.id;

insert overwrite table dws_trade_product_sku_order_1d
    partition (dt = '2023-06-15')
select product_sku_id,
       product_sku_name,
       product_sku_price,
       product_spu_id,
       product_spu_description,
       product_spu_name,
       product_category_id,
       product_category_description,
       product_category_name,
       order_amount,
       order_user_count,
       order_reduce_count,
       order_reduce_amount
from (select product_sku_id,
             sum(split_actual_amount)                                          order_amount,
             count(distinct customer_id)                                       order_user_count,
             count(distinct if(promotion_id is not null, order_info_id, null)) order_reduce_count,
             sum(split_reduce_amount)                                          order_reduce_amount
      from dwd_trade_order_detail_inc
      where dt = '2023-06-15' and product_sku_id is not null
      group by product_sku_id) agg
         left join
     (select id,
             name  product_sku_name,
             price product_sku_price,
             product_spu_id,
             product_spu_description,
             product_spu_name,
             product_category_id,
             product_category_description,
             product_category_name
      from dim_product_sku_full
      where dt = '2023-06-15') sku
     on agg.product_sku_id = sku.id;

DROP TABLE IF EXISTS dws_trade_product_group_order_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_product_group_order_1d
(
    `product_group_id`             STRING COMMENT '套餐ID',
    `product_group_name`           STRING COMMENT '套餐名称',
    `product_group_original_price` DECIMAL(16, 2) COMMENT '套餐原始价格',
    `product_group_price`          DECIMAL(16, 2) COMMENT '套餐当前价格',
    `product_group_sku_ids`        ARRAY<STRING> COMMENT '套餐包含菜品规格ID列表',
    `order_amount`                 DECIMAL(16, 2) COMMENT '下单金额',
    `order_user_count`             BIGINT COMMENT '下单人数',
    `order_reduce_amount`          DECIMAL(16, 2) COMMENT '下单活动减免金额'
) COMMENT '交易域套餐粒度用户下单最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_product_group_order_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_product_group_order_1d
    partition (dt)
select product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_user_count,
       order_reduce_amount,
       dt
from (select dt,
             product_group_id,
             sum(split_actual_amount)    order_amount,
             count(distinct customer_id) order_user_count,
             sum(split_reduce_amount)    order_reduce_amount
      from dwd_trade_order_detail_inc
      where product_group_id is not null
      group by dt,
               product_group_id) agg
         left join
     (select id,
             name            product_group_name,
             original_price  product_group_original_price,
             price           product_group_price,
             product_sku_ids product_group_sku_ids
      from dim_product_group_full
      where dt = '2023-06-14') group_info
     on agg.product_group_id = group_info.id;

insert overwrite table dws_trade_product_group_order_1d
    partition (dt = '2023-06-15')
select product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_user_count,
       order_reduce_amount
from (select product_group_id,
             sum(split_actual_amount)    order_amount,
             count(distinct customer_id) order_user_count,
             sum(split_reduce_amount)    order_reduce_amount
      from dwd_trade_order_detail_inc
      where dt = '2023-06-15'
        and product_group_id is not null
      group by product_group_id) agg
         left join
     (select id,
             name            product_group_name,
             original_price  product_group_original_price,
             price           product_group_price,
             product_sku_ids product_group_sku_ids
      from dim_product_group_full
      where dt = '2023-06-15') group_info
     on agg.product_group_id = group_info.id;

DROP TABLE IF EXISTS dws_interaction_product_spu_comment_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_interaction_product_spu_comment_1d
(
    `product_spu_id`          STRING COMMENT '菜品ID',
    `product_spu_description` STRING COMMENT '菜品描述',
    `product_spu_name`        STRING COMMENT '菜品名称',
    `comment_count`           BIGINT COMMENT '评价次数',
    `good_comment_count`      BIGINT COMMENT '好评次数'
) COMMENT '互动域菜品粒度用户评价最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_interaction_product_spu_comment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_interaction_product_spu_comment_1d
    partition (dt)
select product_spu_id,
       product_spu_description,
       product_spu_name,
       count(distinct order_info_id)                       comment_count,
       count(distinct if(rating = 5, order_info_id, null)) good_comment_count,
       dt
from (select dt,
             product_sku_id,
             order_info_id,
             rating
      from dwd_interaction_comment_inc where dt<='2023-06-14' and product_sku_id is not null) detail
         left join(select id,
                          product_spu_id,
                          product_spu_description,
                          product_spu_name
                   from dim_product_sku_full
                   where dt = '2023-06-14') sku
                  on detail.product_sku_id = sku.id
group by dt,
         product_spu_id,
         product_spu_description,
         product_spu_name;

insert overwrite table dws_interaction_product_spu_comment_1d
    partition (dt = '2023-06-15')
select product_spu_id,
       product_spu_description,
       product_spu_name,
       count(distinct order_info_id)                       comment_count,
       count(distinct if(rating = 5, order_info_id, null)) good_comment_count
from (select product_sku_id,
             order_info_id,
             rating
      from dwd_interaction_comment_inc
      where dt = '2023-06-15') detail
         left join(select id,
                          product_spu_id,
                          product_spu_description,
                          product_spu_name
                   from dim_product_sku_full
                   where dt = '2023-06-15') sku
                  on detail.product_sku_id = sku.id
group by product_spu_id,
         product_spu_description,
         product_spu_name;

DROP TABLE IF EXISTS dws_interaction_product_group_comment_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_interaction_product_group_comment_1d
(
    `product_group_id`             STRING COMMENT '套餐ID',
    `product_group_name`           STRING COMMENT '套餐名称',
    `product_group_original_price` DECIMAL(16, 2) COMMENT '套餐原始价格',
    `product_group_price`          DECIMAL(16, 2) COMMENT '套餐当前价格',
    `product_group_sku_ids`        ARRAY<STRING> COMMENT '套餐包含菜品规格ID列表',
    `comment_count`                BIGINT COMMENT '评价次数',
    `good_comment_count`           BIGINT COMMENT '好评次数',
    `total_comment_rating`         BIGINT COMMENT '总评分'
) COMMENT '互动域套餐粒度用户评价最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_interaction_product_group_comment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_interaction_product_group_comment_1d
    partition (dt)
select agg.product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       comment_count,
       good_comment_count,
       total_comment_rating,
       dt
from (select dt,
             product_group_id,
             count(order_info_id)                                comment_count,
             count(distinct if(rating = 5, order_info_id, null)) good_comment_count,
             sum(rating)                                         total_comment_rating
      from dwd_interaction_comment_inc
      where product_group_id is not null
      group by dt,
               product_group_id) agg
         left join
     (select id              product_group_id,
             name            product_group_name,
             original_price  product_group_original_price,
             price           product_group_price,
             product_sku_ids product_group_sku_ids
      from dim_product_group_full
      where dt = '2023-06-14') product_group
     on agg.product_group_id = product_group.product_group_id;

insert overwrite table dws_interaction_product_group_comment_1d
    partition (dt = '2023-06-15')
select agg.product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       comment_count,
       good_comment_count,
       total_comment_rating
from (select product_group_id,
             count(order_info_id)                                comment_count,
             count(distinct if(rating = 5, order_info_id, null)) good_comment_count,
             sum(rating)                                         total_comment_rating
      from dwd_interaction_comment_inc
      where dt = '2023-06-15'
        and product_group_id is not null
      group by product_group_id) agg
         left join
     (select id              product_group_id,
             name            product_group_name,
             original_price  product_group_original_price,
             price           product_group_price,
             product_sku_ids product_group_sku_ids
      from dim_product_group_full
      where dt = '2023-06-15') product_group
     on agg.product_group_id = product_group.product_group_id;

DROP TABLE IF EXISTS dws_interaction_shop_comment_1d;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_interaction_shop_comment_1d
(
    `shop_id`            STRING COMMENT '店铺ID',
    `shop_name`          STRING COMMENT '店铺名称',
    `shop_type`          STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `comment_count`      BIGINT COMMENT '评价次数',
    `good_comment_count` BIGINT COMMENT '好评次数'
) COMMENT '互动域店铺粒度用户评价最近 1 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_interaction_shop_comment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_interaction_shop_comment_1d
    partition (dt)
select shop_id,
       shop_name,
       shop_type,
       comment_count,
       good_comment_count,
       dt
from (select dt,
             shop_id,
             count(distinct order_info_id)                       comment_count,
             count(distinct if(rating = 5, order_info_id, null)) good_comment_count
      from dwd_interaction_comment_inc
      group by dt,
               shop_id) agg
         left join
     (select id,
             name shop_name,
             type shop_type
      from dim_shop_full
      where dt = '2023-06-14') shop
     on agg.shop_id = shop.id;

insert overwrite table dws_interaction_shop_comment_1d
    partition (dt = '2023-06-15')
select shop_id,
       shop_name,
       shop_type,
       comment_count,
       good_comment_count
from (select shop_id,
             count(distinct order_info_id)                       comment_count,
             count(distinct if(rating = 5, order_info_id, null)) good_comment_count
      from dwd_interaction_comment_inc
      where dt = '2023-06-15'
      group by shop_id) agg
         left join
     (select id,
             name shop_name,
             type shop_type
      from dim_shop_full
      where dt = '2023-06-15') shop
     on agg.shop_id = shop.id;

--最近n日汇总表
DROP TABLE IF EXISTS dws_trade_shop_order_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_shop_order_nd
(
    `shop_id`             STRING COMMENT '店铺ID',
    `shop_name`           STRING COMMENT '店铺名称',
    `shop_type`           STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `city_id`             STRING COMMENT '地区ID',
    `city_name`           STRING COMMENT '地区名称',
    `province_id`         STRING COMMENT '省份ID',
    `province_name`       STRING COMMENT '省份名称',
    `order_amount`        DECIMAL(16, 2) COMMENT '下单金额',
    `order_count`         BIGINT COMMENT '下单次数',
    `order_user_count`    BIGINT COMMENT '下单人数',
    `order_reduce_amount` DECIMAL(16, 2) COMMENT '下单减免金额',
    `order_reduce_count`  BIGINT COMMENT '参与活动订单数',
    `recent_days`         TINYINT COMMENT '最近天数: 7,30'
) COMMENT '交易域店铺粒度用户下单最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_shop_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_shop_order_nd
    partition (dt = '2023-06-14')
select agg.shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       order_amount,
       order_count,
       order_user_count,
       order_reduce_amount,
       order_reduce_count,
       agg.recent_days
from (select shop_id,
             shop_name,
             shop_type,
             city_id,
             city_name,
             province_id,
             province_name,
             sum(order_amount)        order_amount,
             sum(order_count)         order_count,
             sum(order_reduce_amount) order_reduce_amount,
             sum(order_reduce_count)  order_reduce_count,
             recent_days
      from dws_trade_shop_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by shop_id,
               shop_name,
               shop_type,
               city_id,
               city_name,
               province_id,
               province_name,
               recent_days) agg
         left join
     (select shop_id,
             recent_days,
             count(distinct customer_id) order_user_count
      from dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by shop_id,
               recent_days) user_count
     on agg.shop_id = user_count.shop_id
         and agg.recent_days = user_count.recent_days;

DROP TABLE IF EXISTS dws_trade_shop_pay_suc_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_shop_pay_suc_nd
(
    `shop_id`                     STRING COMMENT '店铺ID',
    `shop_name`                   STRING COMMENT '店铺名称',
    `shop_type`                   STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `city_id`                     STRING COMMENT '地区ID',
    `city_name`                   STRING COMMENT '地区名称',
    `province_id`                 STRING COMMENT '省份ID',
    `province_name`               STRING COMMENT '省份名称',
    `pay_suc_amount`              DECIMAL(16, 2) COMMENT '支付成功金额',
    `reduce_shop_share_amount`    DECIMAL(16, 2) COMMENT '店铺优惠分摊支出金额',
    `reduce_company_share_amount` DECIMAL(16, 2) COMMENT '总公司优惠分摊支出金额',
    `recent_days`                 TINYINT COMMENT '最近天数: 7,30'
) COMMENT '交易域店铺粒度用户支付成功最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_shop_pay_suc_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_shop_pay_suc_nd
    partition (dt = '2023-06-14')
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       sum(pay_suc_amount)              pay_suc_amount,
       sum(reduce_shop_share_amount)    reduce_shop_share_amount,
       sum(reduce_company_share_amount) reduce_company_share_amount,
       recent_days
from dws_trade_shop_pay_suc_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-06-14', -recent_days + 1)
group by shop_id,
         shop_name,
         shop_type,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;

DROP TABLE IF EXISTS dws_trade_shop_refund_pay_suc_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_shop_refund_pay_suc_nd
(
    `shop_id`               STRING COMMENT '店铺ID',
    `shop_name`             STRING COMMENT '店铺名称',
    `shop_type`             STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `city_id`               STRING COMMENT '地区ID',
    `city_name`             STRING COMMENT '地区名称',
    `province_id`           STRING COMMENT '省份ID',
    `province_name`         STRING COMMENT '省份名称',
    `refund_pay_suc_amount` DECIMAL(16, 2) COMMENT '退款成功金额',
    `recent_days`           TINYINT COMMENT '最近天数: 7,30'
) COMMENT '交易域店铺粒度退款成功最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_shop_refund_pay_suc_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_shop_refund_pay_suc_nd
    partition (dt = '2023-06-14')
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       sum(refund_pay_suc_amount) refund_pay_suc_amount,
       recent_days
from dws_trade_shop_refund_pay_suc_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-06-14', -recent_days + 1)
group by shop_id,
         shop_name,
         shop_type,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;

DROP TABLE IF EXISTS dws_trade_promotion_order_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_promotion_order_nd
(
    `promotion_id`               STRING COMMENT '营销活动ID',
    `company_share`              DECIMAL COMMENT '活动支出公司负担比例',
    `promotion_name`             STRING COMMENT '营销活动名称',
    `promotion_reduce_amount`    DECIMAL(16, 2) COMMENT '活动满减金额',
    `promotion_threshold_amount` DECIMAL(16, 2) COMMENT '活动满减门槛',
    `total_reduce_amount`        DECIMAL(16, 2) COMMENT '活动累计减免金额',
    `total_activity_order_count` BIGINT COMMENT '参与活动订单数',
    `total_activity_user_count`  BIGINT COMMENT '活动参与人数',
    `recent_days`                TINYINT COMMENT '最近天数: 7,30'
) COMMENT '交易域营销活动粒度用户下单最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_promotion_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_promotion_order_nd
    partition (dt = '2023-06-14')
select agg.promotion_id,
       company_share,
       promotion_name,
       promotion_reduce_amount,
       promotion_threshold_amount,
       total_reduce_amount,
       total_activity_order_count,
       total_activity_user_count,
       agg.recent_days
from (select promotion_id,
             company_share,
             promotion_name,
             promotion_reduce_amount,
             promotion_threshold_amount,
             sum(total_reduce_amount)        total_reduce_amount,
             sum(total_activity_order_count) total_activity_order_count,
             recent_days
      from dws_trade_promotion_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by promotion_id,
               company_share,
               promotion_name,
               promotion_reduce_amount,
               promotion_threshold_amount,
               recent_days) agg
         left join
     (select promotion_id,
             recent_days,
             count(distinct customer_id) total_activity_user_count
      from dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by promotion_id,
               recent_days) user_count
     on agg.promotion_id = user_count.promotion_id
         and agg.recent_days = user_count.recent_days;

DROP TABLE IF EXISTS dws_trade_product_sku_order_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_product_sku_order_nd
(
    `product_sku_id`               STRING COMMENT '菜品规格ID',
    `product_sku_name`             STRING COMMENT '菜品规格名称',
    `product_sku_price`            STRING COMMENT '菜品规格定价',
    `product_spu_id`               STRING COMMENT '菜品ID',
    `product_spu_description`      STRING COMMENT '菜品描述',
    `product_spu_name`             STRING COMMENT '菜品名称',
    `product_category_id`          STRING COMMENT '菜品分类ID',
    `product_category_description` STRING COMMENT '菜品分类描述',
    `product_category_name`        STRING COMMENT '菜品分类名称',
    `order_amount`                 DECIMAL(16, 2) COMMENT '下单金额',
    `order_user_count`             BIGINT COMMENT '下单人数',
    `order_reduce_count`            BIGINT COMMENT '参与活动订单数',
    `order_reduce_amount`          DECIMAL(16, 2) COMMENT '下单活动减免金额',
    `recent_days`                  TINYINT COMMENT '最近天数: 7,30'
) COMMENT '交易域菜品规格粒度用户下单最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_product_sku_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_product_sku_order_nd
    partition (dt = '2023-06-14')
select agg.product_sku_id,
       product_sku_name,
       product_sku_price,
       product_spu_id,
       product_spu_description,
       product_spu_name,
       product_category_id,
       product_category_description,
       product_category_name,
       order_amount,
       order_user_count,
       order_reduce_count,
       order_reduce_amount,
       agg.recent_days
from (select product_sku_id,
             product_sku_name,
             product_sku_price,
             product_spu_id,
             product_spu_description,
             product_spu_name,
             product_category_id,
             product_category_description,
             product_category_name,
             sum(order_amount)        order_amount,
             sum(order_reduce_count) order_reduce_count,
             sum(order_reduce_amount) order_reduce_amount,
             recent_days
      from dws_trade_product_sku_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by product_sku_id,
               product_sku_name,
               product_sku_price,
               product_spu_id,
               product_spu_description,
               product_spu_name,
               product_category_id,
               product_category_description,
               product_category_name,
               recent_days) agg
         left join
     (select product_sku_id,
             recent_days,
             count(distinct customer_id) order_user_count
      from dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by product_sku_id,
               recent_days) user_count
     on agg.product_sku_id = user_count.product_sku_id
         and agg.recent_days = user_count.recent_days;

DROP TABLE IF EXISTS dws_trade_product_group_order_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_trade_product_group_order_nd
(
    `product_group_id`             STRING COMMENT '套餐ID',
    `product_group_name`           STRING COMMENT '套餐名称',
    `product_group_original_price` DECIMAL(16, 2) COMMENT '套餐原始价格',
    `product_group_price`          DECIMAL(16, 2) COMMENT '套餐当前价格',
    `product_group_sku_ids`        ARRAY<STRING> COMMENT '套餐包含菜品规格ID列表',
    `order_amount`                 DECIMAL(16, 2) COMMENT '下单金额',
    `order_user_count`             BIGINT COMMENT '下单人数',
    `order_reduce_amount`          DECIMAL(16, 2) COMMENT '下单活动减免金额',
    `recent_days`                  TINYINT COMMENT '最近天数: 7,30'
) COMMENT '交易域套餐粒度用户下单最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_trade_product_group_order_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_trade_product_group_order_nd
    partition (dt = '2023-06-14')
select agg.product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_user_count,
       order_reduce_amount,
       agg.recent_days
from (select product_group_id,
             product_group_name,
             product_group_original_price,
             product_group_price,
             product_group_sku_ids,
             sum(order_amount)        order_amount,
             sum(order_reduce_amount) order_reduce_amount,
             recent_days
      from dws_trade_product_group_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by product_group_id,
               product_group_name,
               product_group_original_price,
               product_group_price,
               product_group_sku_ids,
               recent_days) agg
         left join
     (select product_group_id,
             recent_days,
             count(distinct customer_id) order_user_count
      from dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-06-14', -recent_days + 1)
      group by product_group_id,
               recent_days) user_count
     on agg.product_group_id = user_count.product_group_id
         and agg.recent_days = user_count.recent_days;


DROP TABLE IF EXISTS dws_interaction_product_spu_comment_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_interaction_product_spu_comment_nd
(
    `product_spu_id`          STRING COMMENT '菜品ID',
    `product_spu_description` STRING COMMENT '菜品描述',
    `product_spu_name`        STRING COMMENT '菜品名称',
    `comment_count`           BIGINT COMMENT '评价次数',
    `good_comment_count`      BIGINT COMMENT '好评次数',
    `recent_days`             TINYINT COMMENT '最近天数: 7,30'
) COMMENT '互动域菜品粒度用户评价最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_interaction_product_spu_comment_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert overwrite table dws_interaction_product_spu_comment_nd
    partition (dt = '2023-06-14')
select product_spu_id,
       product_spu_description,
       product_spu_name,
       sum(comment_count)      comment_count,
       sum(good_comment_count) good_comment_count,
       recent_days
from dws_interaction_product_spu_comment_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-06-14', -recent_days + 1)
group by recent_days,
         product_spu_id,
         product_spu_description,
         product_spu_name;

DROP TABLE IF EXISTS dws_interaction_product_group_comment_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_interaction_product_group_comment_nd
(
    `product_group_id`             STRING COMMENT '套餐ID',
    `product_group_name`           STRING COMMENT '套餐名称',
    `product_group_original_price` DECIMAL(16, 2) COMMENT '套餐原始价格',
    `product_group_price`          DECIMAL(16, 2) COMMENT '套餐当前价格',
    `product_group_sku_ids`        ARRAY<STRING> COMMENT '套餐包含菜品规格ID列表',
    `comment_count`                BIGINT COMMENT '评价次数',
    `good_comment_count`           BIGINT COMMENT '好评次数',
    `total_comment_rating`         BIGINT COMMENT '总评分',
    `recent_days`                  TINYINT COMMENT '最近天数: 7,30'
) COMMENT '互动域套餐粒度用户评价最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_interaction_product_group_comment_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_interaction_product_group_comment_nd
    partition (dt = '2023-06-14')
select product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       sum(comment_count)        comment_count,
       sum(good_comment_count)   good_comment_count,
       sum(total_comment_rating) total_comment_rating,
       recent_days
from dws_interaction_product_group_comment_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-06-14', -recent_days + 1)
group by product_group_id,
         product_group_name,
         product_group_original_price,
         product_group_price,
         product_group_sku_ids,
         recent_days;


DROP TABLE IF EXISTS dws_interaction_shop_comment_nd;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_interaction_shop_comment_nd
(
    `shop_id`            STRING COMMENT '店铺ID',
    `shop_name`          STRING COMMENT '店铺名称',
    `shop_type`          STRING COMMENT '店铺类型 1:直营, 2:加盟',
    `comment_count`      BIGINT COMMENT '评价次数',
    `good_comment_count` BIGINT COMMENT '好评次数',
    `recent_days`        TINYINT COMMENT '最近天数: 7,30'
) COMMENT '互动域店铺粒度用户评价最近 n 日汇总表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dws/dws_interaction_shop_comment_nd'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert overwrite table dws_interaction_shop_comment_nd
    partition (dt = '2023-06-14')
select shop_id,
       shop_name,
       shop_type,
       sum(comment_count)      comment_count,
       sum(good_comment_count) good_comment_count,
       recent_days
from dws_interaction_shop_comment_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-06-14', -recent_days + 1)
group by shop_id,
         shop_name,
         shop_type,
         recent_days;
