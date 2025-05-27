DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '订单明细ID',
    `order_time`            STRING COMMENT '下单时间',
    `order_date`            STRING COMMENT '下单日期',
    `split_original_amount` DECIMAL(16, 2) COMMENT '分摊原始金额',
    `split_reduce_amount`   DECIMAL(16, 2) COMMENT '分摊优惠金额',
    `split_actual_amount`   DECIMAL(16, 2) COMMENT '分摊实际金额',
    `sku_num`               INT COMMENT '数量',
    `customer_id`           STRING COMMENT '下单用户',
    `order_info_id`         STRING COMMENT '订单ID',
    `product_group_id`      STRING COMMENT '套餐ID',
    `product_sku_id`        STRING COMMENT '菜品ID',
    `shop_id`               STRING COMMENT '下单店铺',
    `promotion_id`          STRING COMMENT '关联优惠记录'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dwd/dwd_trade_order_detail_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc
    partition (dt)
select detail.id,
       order_time,
       order_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id,
       order_date
from (select data.id,
             data.create_time                            order_time,
             date_format(data.create_time, 'yyyy-MM-dd') order_date,
             data.amount                                 split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert') detail
         left join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert') info
     on detail.order_info_id = info.id;

insert overwrite table dwd_trade_order_detail_inc
    partition (dt = '2023-06-15')
select detail.id,
       order_time,
       order_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id
from (select data.id,
             data.create_time                            order_time,
             date_format(data.create_time, 'yyyy-MM-dd') order_date,
             data.amount                                 split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt = '2023-06-15'
        and type = 'insert') detail
         left join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '2023-06-15'
        and type = 'insert') info
     on detail.order_info_id = info.id;

DROP TABLE IF EXISTS dwd_trade_pay_suc_detail_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_trade_pay_suc_detail_inc
(
    `id`                    STRING COMMENT '订单明细ID',
    `pay_suc_time`          STRING COMMENT '支付成功时间',
    `pay_suc_date`          STRING COMMENT '支付成功日期',
    `split_original_amount` DECIMAL(16, 2) COMMENT '分摊原始金额',
    `split_reduce_amount`   DECIMAL(16, 2) COMMENT '分摊优惠金额',
    `split_actual_amount`   DECIMAL(16, 2) COMMENT '分摊实际金额',
    `sku_num`               INT COMMENT '数量',
    `customer_id`           STRING COMMENT '下单用户',
    `order_info_id`         STRING COMMENT '订单ID',
    `product_group_id`      STRING COMMENT '套餐ID',
    `product_sku_id`        STRING COMMENT '菜品ID',
    `shop_id`               STRING COMMENT '下单店铺',
    `promotion_id`          STRING COMMENT '关联优惠记录'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dwd/dwd_trade_pay_suc_detail_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_suc_detail_inc
    partition (dt)
select detail.id,
       pay_suc_time,
       pay_suc_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id,
       pay_suc_date
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and data.status <> '1') info
     on detail.order_info_id = info.id
         join
     (select data.create_time                            pay_suc_time,
             date_format(data.create_time, 'yyyy-MM-dd') pay_suc_date,
             data.order_info_id
      from ods_order_status_log_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and data.status = '2') log
     on detail.order_info_id = log.order_info_id;

insert overwrite table dwd_trade_pay_suc_detail_inc
    partition (dt = '2023-06-15')
select detail.id,
       pay_suc_time,
       pay_suc_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt >= date_add('2023-06-15', -1)
        and (type = 'insert' or type = 'bootstrap-insert')
        and date_format(data.create_time, 'yyyy-MM-dd') >= date_add('2023-06-15', -1)
     ) detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount,
             data.update_time                            pay_suc_time,
             date_format(data.update_time, 'yyyy-MM-dd') pay_suc_date
      from ods_order_info_inc
      where dt = '2023-06-15'
        and type = 'update'
        and data.status = '2') info
     on detail.order_info_id = info.id;

DROP TABLE IF EXISTS dwd_trade_refund_detail_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_trade_refund_detail_inc
(
    `id`                    STRING COMMENT '订单明细ID',
    `refund_time`           STRING COMMENT '退单发起时间',
    `refund_date`           STRING COMMENT '退单发起日期',
    `split_original_amount` DECIMAL(16, 2) COMMENT '分摊原始金额',
    `split_reduce_amount`   DECIMAL(16, 2) COMMENT '分摊优惠金额',
    `split_actual_amount`   DECIMAL(16, 2) COMMENT '分摊实际金额',
    `sku_num`               INT COMMENT '数量',
    `customer_id`           STRING COMMENT '下单用户',
    `order_info_id`         STRING COMMENT '订单ID',
    `product_group_id`      STRING COMMENT '套餐ID',
    `product_sku_id`        STRING COMMENT '菜品ID',
    `shop_id`               STRING COMMENT '下单店铺',
    `promotion_id`          STRING COMMENT '关联优惠记录'
) COMMENT '交易域退单事务事实表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dwd/dwd_trade_refund_detail_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_refund_detail_inc
    partition (dt)
select detail.id,
       refund_time,
       refund_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id,
       refund_date
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and (data.status = '5' or data.status = '4' or data.status = '6')) info
     on detail.order_info_id = info.id
         join
     (select data.create_time                            refund_time,
             date_format(data.create_time, 'yyyy-MM-dd') refund_date,
             data.order_info_id
      from ods_order_status_log_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and data.status = '5') log
     on detail.order_info_id = log.order_info_id;


insert overwrite table dwd_trade_refund_detail_inc
    partition (dt = '2023-06-15')
select detail.id,
       refund_time,
       refund_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt >= date_add('2023-06-15', -1)
        and (type = 'insert' or type = 'bootstrap-insert')
        and date_format(data.create_time, 'yyyy-MM-dd') >= date_add('2023-06-15', -1)) detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount,
             data.update_time                            refund_time,
             date_format(data.update_time, 'yyyy-MM-dd') refund_date
      from ods_order_info_inc
      where dt = '2023-06-15'
        and type = 'update'
        and data.status = '5') info
     on detail.order_info_id = info.id;

DROP TABLE IF EXISTS dwd_trade_refund_payment_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_trade_refund_payment_inc
(
    `id`                    STRING COMMENT '订单明细ID',
    `refund_pay_suc_time`   STRING COMMENT '退款成功时间',
    `refund_pay_suc_date`   STRING COMMENT '退款成功日期',
    `split_original_amount` DECIMAL(16, 2) COMMENT '分摊原始金额',
    `split_reduce_amount`   DECIMAL(16, 2) COMMENT '分摊优惠金额',
    `split_actual_amount`   DECIMAL(16, 2) COMMENT '分摊实际金额',
    `sku_num`               INT COMMENT '数量',
    `customer_id`           STRING COMMENT '下单用户',
    `order_info_id`         STRING COMMENT '订单ID',
    `product_group_id`      STRING COMMENT '套餐ID',
    `product_sku_id`        STRING COMMENT '菜品ID',
    `shop_id`               STRING COMMENT '下单店铺',
    `promotion_id`          STRING COMMENT '关联优惠记录'
) COMMENT '交易域退款成功事务事实表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dwd/dwd_trade_refund_payment_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_refund_payment_inc
    partition (dt)
select detail.id,
       refund_pay_suc_time,
       refund_pay_suc_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id,
       refund_pay_suc_date
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and (data.status = '6' or data.status = '4')) info
     on detail.order_info_id = info.id
         join
     (select data.create_time                            refund_pay_suc_time,
             date_format(data.create_time, 'yyyy-MM-dd') refund_pay_suc_date,
             data.order_info_id
      from ods_order_status_log_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and data.status = '6') log
     on detail.order_info_id = log.order_info_id;

insert overwrite table dwd_trade_refund_payment_inc
    partition (dt = '2023-06-15')
select detail.id,
       refund_pay_suc_time,
       refund_pay_suc_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt >= date_add('2023-06-15', -1)
        and (type = 'insert' or type = 'bootstrap-insert')
        and date_format(data.create_time, 'yyyy-MM-dd') >= date_add('2023-06-15', -1)) detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount,
             data.update_time                            refund_pay_suc_time,
             date_format(data.update_time, 'yyyy-MM-dd') refund_pay_suc_date
      from ods_order_info_inc
      where dt = '2023-06-15'
        and type = 'update'
        and data.status = '6') info
     on detail.order_info_id = info.id;


DROP TABLE IF EXISTS dwd_interaction_comment_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_interaction_comment_inc
(
    `id`                    STRING COMMENT '订单明细ID',
    `comment_time`          STRING COMMENT '评价时间',
    `comment_date`          STRING COMMENT '评价日期',
    `split_original_amount` DECIMAL(16, 2) COMMENT '分摊原始金额',
    `split_reduce_amount`   DECIMAL(16, 2) COMMENT '分摊优惠金额',
    `split_actual_amount`   DECIMAL(16, 2) COMMENT '分摊实际金额',
    `rating`                TINYINT COMMENT '评分',
    `sku_num`               INT COMMENT '数量',
    `customer_id`           STRING COMMENT '下单用户',
    `order_info_id`         STRING COMMENT '订单ID',
    `product_group_id`      STRING COMMENT '套餐ID',
    `product_sku_id`        STRING COMMENT '菜品ID',
    `shop_id`               STRING COMMENT '下单店铺',
    `promotion_id`          STRING COMMENT '关联优惠记录'
) COMMENT '互动域评价事务事实表'
    PARTITIONED BY (`dt` STRING COMMENT '分区')
    STORED AS ORC
    LOCATION '/warehouse/fast_food/dwd/dwd_interaction_comment_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_comment_inc
    partition (dt)
select detail.id,
       comment_time,
       comment_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       rating,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id,
       comment_date
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount,
             data.rating
      from ods_order_info_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and (data.status = '4'
          or data.status = '5'
          or data.status = '6')) info
     on detail.order_info_id = info.id
         join
     (select data.create_time                            comment_time,
             date_format(data.create_time, 'yyyy-MM-dd') comment_date,
             data.order_info_id
      from ods_order_status_log_inc
      where dt = '2023-06-14'
        and type = 'bootstrap-insert'
        and data.status = '4') log
     on detail.order_info_id = log.order_info_id;

insert overwrite table dwd_interaction_comment_inc
    partition (dt = '2023-06-15')
select detail.id,
       comment_time,
       comment_date,
       split_orginal_amount,
       reduce_amount * split_orginal_amount / original_amount split_reduce_amount,
       actual_amount * split_orginal_amount / original_amount split_actual_amount,
       rating,
       sku_num,
       customer_id,
       detail.order_info_id,
       product_group_id,
       product_sku_id,
       shop_id,
       promotion_id
from (select data.id,
             data.amount split_orginal_amount,
             data.sku_num,
             data.customer_id,
             data.order_info_id,
             data.product_group_id,
             data.product_sku_id,
             data.shop_id
      from ods_order_detail_inc
      where dt >= date_add('2023-06-15', -3)
        and (type = 'insert' or type = 'bootstrap-insert')
        and date_format(data.create_time, 'yyyy-MM-dd') >= date_add('2023-06-15', -3)) detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount,
             data.update_time                            comment_time,
             date_format(data.update_time, 'yyyy-MM-dd') comment_date,
             data.rating
      from ods_order_info_inc
      where dt = '2023-06-15'
        and type = 'update'
        and data.status = '4') info
     on detail.order_info_id = info.id;



