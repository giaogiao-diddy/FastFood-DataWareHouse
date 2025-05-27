#!/bin/bash
APP=fast_food

if [ $# -lt 2 ]
then 
	echo "必须传入all/表名以及数仓上线日期..."
	exit
fi

dwd_trade_order_detail_inc_sql="
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
      where dt = '$2'
        and type = 'bootstrap-insert') detail
         left join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '$2'
        and type = 'bootstrap-insert') info
     on detail.order_info_id = info.id;
"

dwd_trade_pay_suc_detail_inc_sql="
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
      where dt = '$2'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '$2'
        and type = 'bootstrap-insert'
        and data.status <> '1') info
     on detail.order_info_id = info.id
         join
     (select data.create_time                            pay_suc_time,
             date_format(data.create_time, 'yyyy-MM-dd') pay_suc_date,
             data.order_info_id
      from ods_order_status_log_inc
      where dt = '$2'
        and type = 'bootstrap-insert'
        and data.status = '2') log
     on detail.order_info_id = log.order_info_id;
"

dwd_trade_refund_detail_inc_sql="
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
      where dt = '$2'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '$2'
        and type = 'bootstrap-insert'
        and (data.status = '5' or data.status = '4' or data.status = '6')) info
     on detail.order_info_id = info.id
         join
     (select data.create_time                            refund_time,
             date_format(data.create_time, 'yyyy-MM-dd') refund_date,
             data.order_info_id
      from ods_order_status_log_inc
      where dt = '$2'
        and type = 'bootstrap-insert'
        and data.status = '5') log
     on detail.order_info_id = log.order_info_id;
"

dwd_trade_refund_payment_inc_sql="
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
      where dt = '$2'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount
      from ods_order_info_inc
      where dt = '$2'
        and type = 'bootstrap-insert'
        and (data.status = '6' or data.status = '4')) info
     on detail.order_info_id = info.id
         join
     (select data.create_time                            refund_pay_suc_time,
             date_format(data.create_time, 'yyyy-MM-dd') refund_pay_suc_date,
             data.order_info_id
      from ods_order_status_log_inc
      where dt = '$2'
        and type = 'bootstrap-insert'
        and data.status = '6') log
     on detail.order_info_id = log.order_info_id;
"

dwd_interaction_comment_inc_sql="
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
      where dt = '$2'
        and type = 'bootstrap-insert') detail
         join
     (select data.id,
             data.promotion_id,
             data.original_amount,
             data.reduce_amount,
             data.actual_amount,
             data.rating
      from ods_order_info_inc
      where dt = '$2'
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
      where dt = '$2'
        and type = 'bootstrap-insert'
        and data.status = '4') log
     on detail.order_info_id = log.order_info_id;
"

case $1 in
"all")
	/opt/module/hive/bin/hive -e "use fast_food;set hive.exec.dynamic.partition.mode=nonstrict;${dwd_interaction_comment_inc_sql};${dwd_trade_order_detail_inc_sql};${dwd_trade_pay_suc_detail_inc_sql};${dwd_trade_refund_detail_inc_sql};${dwd_trade_refund_payment_inc_sql}"
;;
"dwd_interaction_comment_inc")
    /opt/module/hive/bin/hive -e "use fast_food;set hive.exec.dynamic.partition.mode=nonstrict;${dwd_interaction_comment_inc_sql}"
;;
"dwd_trade_order_detail_inc")
    /opt/module/hive/bin/hive -e "use fast_food;set hive.exec.dynamic.partition.mode=nonstrict;${dwd_trade_order_detail_inc_sql}"
;;
"dwd_trade_pay_suc_detail_inc")
    /opt/module/hive/bin/hive -e "use fast_food;set hive.exec.dynamic.partition.mode=nonstrict;${dwd_trade_pay_suc_detail_inc_sql}"
;;
"dwd_trade_refund_detail_inc")
    /opt/module/hive/bin/hive -e "use fast_food;set hive.exec.dynamic.partition.mode=nonstrict;${dwd_trade_refund_detail_inc_sql}"
;;
"dwd_trade_refund_payment_inc")
    /opt/module/hive/bin/hive -e "use fast_food;set hive.exec.dynamic.partition.mode=nonstrict;${dwd_trade_refund_payment_inc_sql}"
;;
*)
	echo "表名输入错误..."
；；
esac
	

