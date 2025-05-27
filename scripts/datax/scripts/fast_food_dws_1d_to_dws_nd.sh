#!/bin/bash
APP=fast_food

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dws_trade_shop_order_nd="
insert overwrite table ${APP}.dws_trade_shop_order_nd
    partition (dt = '$do_date')
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
      from ${APP}.dws_trade_shop_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
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
      from ${APP}.dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by shop_id,
               recent_days) user_count
     on agg.shop_id = user_count.shop_id
         and agg.recent_days = user_count.recent_days;
"

dws_trade_shop_pay_suc_nd="
insert overwrite table ${APP}.dws_trade_shop_pay_suc_nd
    partition (dt = '$do_date')
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
from ${APP}.dws_trade_shop_pay_suc_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('$do_date', -recent_days + 1)
group by shop_id,
         shop_name,
         shop_type,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
"

dws_trade_shop_refund_pay_suc_nd="
insert overwrite table ${APP}.dws_trade_shop_refund_pay_suc_nd
    partition (dt = '$do_date')
select shop_id,
       shop_name,
       shop_type,
       city_id,
       city_name,
       province_id,
       province_name,
       sum(refund_pay_suc_amount) refund_pay_suc_amount,
       recent_days
from ${APP}.dws_trade_shop_refund_pay_suc_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('$do_date', -recent_days + 1)
group by shop_id,
         shop_name,
         shop_type,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
"

dws_trade_promotion_order_nd="
insert overwrite table ${APP}.dws_trade_promotion_order_nd
    partition (dt = '$do_date')
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
      from ${APP}.dws_trade_promotion_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
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
      from ${APP}.dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by promotion_id,
               recent_days) user_count
     on agg.promotion_id = user_count.promotion_id
         and agg.recent_days = user_count.recent_days;
"

dws_trade_product_sku_order_nd="
insert overwrite table ${APP}.dws_trade_product_sku_order_nd
    partition (dt = '$do_date')
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
      from ${APP}.dws_trade_product_sku_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
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
      from ${APP}.dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by product_sku_id,
               recent_days) user_count
     on agg.product_sku_id = user_count.product_sku_id
         and agg.recent_days = user_count.recent_days;
"

dws_trade_product_group_order_nd="
insert overwrite table ${APP}.dws_trade_product_group_order_nd
    partition (dt = '$do_date')
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
      from ${APP}.dws_trade_product_group_order_1d lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
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
      from ${APP}.dwd_trade_order_detail_inc lateral view explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by product_group_id,
               recent_days) user_count
     on agg.product_group_id = user_count.product_group_id
         and agg.recent_days = user_count.recent_days;
"

dws_interaction_product_spu_comment_nd="
insert overwrite table ${APP}.dws_interaction_product_spu_comment_nd
    partition (dt = '$do_date')
select product_spu_id,
       product_spu_description,
       product_spu_name,
       sum(comment_count)      comment_count,
       sum(good_comment_count) good_comment_count,
       recent_days
from ${APP}.dws_interaction_product_spu_comment_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('$do_date', -recent_days + 1)
group by recent_days,
         product_spu_id,
         product_spu_description,
         product_spu_name;
"

dws_interaction_product_group_comment_nd="
insert overwrite table ${APP}.dws_interaction_product_group_comment_nd
    partition (dt = '$do_date')
select product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       sum(comment_count)        comment_count,
       sum(good_comment_count)   good_comment_count,
       sum(total_comment_rating) total_comment_rating,
       recent_days
from ${APP}.dws_interaction_product_group_comment_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('$do_date', -recent_days + 1)
group by product_group_id,
         product_group_name,
         product_group_original_price,
         product_group_price,
         product_group_sku_ids,
         recent_days;
"

dws_interaction_shop_comment_nd="
insert overwrite table ${APP}.dws_interaction_shop_comment_nd
    partition (dt = '$do_date')
select shop_id,
       shop_name,
       shop_type,
       sum(comment_count)      comment_count,
       sum(good_comment_count) good_comment_count,
       recent_days
from ${APP}.dws_interaction_shop_comment_1d lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('$do_date', -recent_days + 1)
group by shop_id,
         shop_name,
         shop_type,
         recent_days;
"

case $1 in
dws_trade_shop_order_nd | dws_trade_shop_pay_suc_nd | dws_trade_shop_refund_pay_suc_nd | dws_trade_promotion_order_nd | dws_trade_product_sku_order_nd | dws_trade_product_group_order_nd | dws_interaction_product_spu_comment_nd | dws_interaction_product_group_comment_nd | dws_interaction_shop_comment_nd)
    hive -e "${!1}"
    ;;
all)
    hive -e "$dws_trade_shop_order_nd$dws_trade_shop_pay_suc_nd$dws_trade_shop_refund_pay_suc_nd$dws_trade_promotion_order_nd$dws_trade_product_sku_order_nd$dws_trade_product_group_order_nd$dws_interaction_product_spu_comment_nd$dws_interaction_product_group_comment_nd$dws_interaction_shop_comment_nd"
    ;;
esac
