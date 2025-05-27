#!/bin/bash
APP=fast_food

# 如果输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dws_trade_shop_order_1d="
insert overwrite table ${APP}.dws_trade_shop_order_1d
    partition (dt = '$do_date')
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
      from ${APP}.dwd_trade_order_detail_inc
      where dt = '$do_date'
      group by shop_id) agg
         left join (select id,
                           name      shop_name,
                           type      shop_type,
                           region_id city_id
                    from ${APP}.dim_shop_full
                    where dt = '$do_date') shop
                   on agg.shop_id = shop.id
         left join (select id,
                           name            city_name,
                           superior_region province_id
                    from ${APP}.dim_region_full
                    where dt = '$do_date'
                      and level = '2') city
                   on shop.city_id = city.id
         left join (select id,
                           name province_name
                    from ${APP}.dim_region_full
                    where dt = '$do_date'
                      and level = '1') province
                   on province_id = province.id;
"

dws_trade_shop_pay_suc_1d="
insert overwrite table ${APP}.dws_trade_shop_pay_suc_1d
    partition (dt = '$do_date')
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
            from ${APP}.dwd_trade_pay_suc_detail_inc
            where dt = '$do_date') detail
               left join
           (select id,
                   company_share
            from ${APP}.dim_promotion_full
            where dt = '$do_date') promotion
           on detail.promotion_id = promotion.id
      group by shop_id) agg
         left join
     (select id,
             name      shop_name,
             type      shop_type,
             region_id city_id
      from ${APP}.dim_shop_full
      where dt = '$do_date') shop
     on agg.shop_id = shop.id
         left join
     (select id,
             name            city_name,
             superior_region province_id
      from ${APP}.dim_region_full
      where dt = '$do_date'
        and level = '2') city
     on city_id = city.id
         left join
     (select id,
             name province_name
      from ${APP}.dim_region_full
      where dt = '$do_date'
        and level = '1') province
     on province_id = province.id;
"

dws_trade_shop_refund_pay_suc_1d="
insert overwrite table ${APP}.dws_trade_shop_refund_pay_suc_1d
    partition (dt = '$do_date')
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
      from ${APP}.dwd_trade_refund_payment_inc
      where dt = '$do_date'
      group by shop_id) agg
         left join (select id,
                           name      shop_name,
                           type      shop_type,
                           region_id city_id
                    from ${APP}.dim_shop_full
                    where dt = '$do_date') shop
                   on agg.shop_id = shop.id
         left join (select id,
                           name            city_name,
                           superior_region province_id
                    from ${APP}.dim_region_full
                    where dt = '$do_date'
                      and level = '2') city
                   on shop.city_id = city.id
         left join (select id,
                           name province_name
                    from ${APP}.dim_region_full
                    where dt = '$do_date'
                      and level = '1') province
                   on province_id = province.id;
"

dws_trade_promotion_order_1d="
insert overwrite table ${APP}.dws_trade_promotion_order_1d
    partition (dt = '$do_date')
select promotion_id,
       company_share,
       promotion_name,
       promotion_reduce_amount,
       promotion_threshold_amount,
       total_reduce_amount,
       total_activity_order_count,
       total_activity_user_count
from (select promotion_id,
             sum(split_reduce_amount)                                          total_reduce_amount,
             count(distinct if(promotion_id is not null, order_info_id, null)) total_activity_order_count,
             count(distinct if(promotion_id is not null, customer_id, null))   total_activity_user_count
      from ${APP}.dwd_trade_order_detail_inc
      where dt = '$do_date'
        and promotion_id is not null
      group by promotion_id) agg
         left join
     (select id,
             company_share,
             name             promotion_name,
             reduce_amount    promotion_reduce_amount,
             threshold_amount promotion_threshold_amount
      from ${APP}.dim_promotion_full
      where dt = '$do_date') promotion
     on agg.promotion_id = promotion.id;
"

dws_trade_product_sku_order_1d="
insert overwrite table ${APP}.dws_trade_product_sku_order_1d
    partition (dt = '$do_date')
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
      from ${APP}.dwd_trade_order_detail_inc
      where dt = '$do_date'
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
      from ${APP}.dim_product_sku_full
      where dt = '$do_date') sku
     on agg.product_sku_id = sku.id;
"

dws_trade_product_group_order_1d="
insert overwrite table ${APP}.dws_trade_product_group_order_1d
    partition (dt = '$do_date')
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
      from ${APP}.dwd_trade_order_detail_inc
      where dt = '$do_date'
        and product_group_id is not null
      group by product_group_id) agg
         left join
     (select id,
             name            product_group_name,
             original_price  product_group_original_price,
             price           product_group_price,
             product_sku_ids product_group_sku_ids
      from ${APP}.dim_product_group_full
      where dt = '$do_date') group_info
     on agg.product_group_id = group_info.id;
"

dws_interaction_product_spu_comment_1d="
insert overwrite table ${APP}.dws_interaction_product_spu_comment_1d
    partition (dt = '$do_date')
select product_spu_id,
       product_spu_description,
       product_spu_name,
       count(distinct order_info_id)                       comment_count,
       count(distinct if(rating = 5, order_info_id, null)) good_comment_count
from (select product_sku_id,
             order_info_id,
             rating
      from ${APP}.dwd_interaction_comment_inc
      where dt = '$do_date') detail
         left join(select id,
                          product_spu_id,
                          product_spu_description,
                          product_spu_name
                   from ${APP}.dim_product_sku_full
                   where dt = '$do_date') sku
                  on detail.product_sku_id = sku.id
group by product_spu_id,
         product_spu_description,
         product_spu_name;
"

dws_interaction_product_group_comment_1d="
insert overwrite table ${APP}.dws_interaction_product_group_comment_1d
    partition (dt = '$do_date')
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
      from ${APP}.dwd_interaction_comment_inc
      where dt = '$do_date'
        and product_group_id is not null
      group by product_group_id) agg
         left join
     (select id              product_group_id,
             name            product_group_name,
             original_price  product_group_original_price,
             price           product_group_price,
             product_sku_ids product_group_sku_ids
      from ${APP}.dim_product_group_full
      where dt = '$do_date') product_group
     on agg.product_group_id = product_group.product_group_id;
"

dws_interaction_shop_comment_1d="
insert overwrite table ${APP}.dws_interaction_shop_comment_1d
    partition (dt = '$do_date')
select shop_id,
       shop_name,
       shop_type,
       comment_count,
       good_comment_count
from (select shop_id,
             count(distinct order_info_id)                       comment_count,
             count(distinct if(rating = 5, order_info_id, null)) good_comment_count
      from ${APP}.dwd_interaction_comment_inc
      where dt = '$do_date'
      group by shop_id) agg
         left join
     (select id,
             name shop_name,
             type shop_type
      from ${APP}.dim_shop_full
      where dt = '$do_date') shop
     on agg.shop_id = shop.id;
"

case $1 in
dws_trade_shop_order_1d | dws_trade_shop_pay_suc_1d | dws_trade_shop_refund_pay_suc_1d | dws_trade_promotion_order_1d | dws_trade_product_sku_order_1d | dws_trade_product_group_order_1d | dws_interaction_product_spu_comment_1d | dws_interaction_product_group_comment_1d | dws_interaction_shop_comment_1d)
    hive -e "${!1}"
    ;;
all)
    hive -e "$dws_trade_shop_order_1d$dws_trade_shop_pay_suc_1d$dws_trade_shop_refund_pay_suc_1d$dws_trade_promotion_order_1d$dws_trade_product_sku_order_1d$dws_trade_product_group_order_1d$dws_interaction_product_spu_comment_1d$dws_interaction_product_group_comment_1d$dws_interaction_shop_comment_1d"
    ;;
esac
