#!/bin/bash

APP=fast_food
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

ads_shop_trade_stats="
insert overwrite table ${APP}.ads_shop_trade_stats
select dt,
       recent_days,
       shop_id,
       shop_name,
       shop_type,
       order_amount,
       order_count,
       order_user_count,
       pay_suc_amount,
       refund_pay_suc_amount
from ${APP}.ads_shop_trade_stats
union
select '$do_date'                                            dt,
       1                                                       recent_days,
       nvl(1d_inter.shop_id, refund_1d.shop_id)                shop_id,
       nvl(1d_inter.shop_name, refund_1d.shop_name)            shop_name,
       nvl(1d_inter.shop_type, refund_1d.shop_type)            shop_type,
       cast(nvl(order_amount, 0.0) as decimal(16, 2))          order_amount,
       nvl(order_count, 0.0)                                   order_count,
       nvl(order_user_count, 0.0)                              order_user_count,
       cast(nvl(pay_suc_amount, 0.0) as decimal(16, 2))        pay_suc_amount,
       cast(nvl(refund_pay_suc_amount, 0.0) as decimal(16, 2)) refund_pay_suc_amount
from (select nvl(order_1d.shop_id, pay_1d.shop_id)     shop_id,
             nvl(order_1d.shop_name, pay_1d.shop_name) shop_name,
             nvl(order_1d.shop_type, pay_1d.shop_type) shop_type,
             nvl(order_amount, 0.0)                    order_amount,
             nvl(order_count, 0.0)                     order_count,
             nvl(order_user_count, 0.0)                order_user_count,
             nvl(pay_suc_amount, 0.0)                  pay_suc_amount
      from (select shop_id,
                   shop_name,
                   shop_type,
                   order_amount,
                   order_count,
                   order_user_count
            from ${APP}.dws_trade_shop_order_1d
            where dt = '$do_date') order_1d
               full outer join
           (select shop_id,
                   shop_name,
                   shop_type,
                   pay_suc_amount
            from ${APP}.dws_trade_shop_pay_suc_1d
            where dt = '$do_date') pay_1d
           on order_1d.shop_id = pay_1d.shop_id) 1d_inter
         left join
     (select shop_id,
             shop_name,
             shop_type,
             refund_pay_suc_amount
      from ${APP}.dws_trade_shop_refund_pay_suc_1d
      where dt = '$do_date') refund_1d
     on 1d_inter.shop_id = refund_1d.shop_id
union
select '$do_date'                                            dt,
       nvl(nd_inter.recent_days, refund_nd.recent_days)        recent_days,
       nvl(nd_inter.shop_id, refund_nd.shop_id)                shop_id,
       nvl(nd_inter.shop_name, refund_nd.shop_name)            shop_name,
       nvl(nd_inter.shop_type, refund_nd.shop_type)            shop_type,
       cast(nvl(order_amount, 0.0) as decimal(16, 2))          order_amount,
       nvl(order_count, 0.0)                                   order_count,
       nvl(order_user_count, 0.0)                              order_user_count,
       cast(nvl(pay_suc_amount, 0.0) as decimal(16, 2))        pay_suc_amount,
       cast(nvl(refund_pay_suc_amount, 0.0) as decimal(16, 2)) refund_pay_suc_amount
from (select nvl(order_nd.shop_id, pay_nd.shop_id)         shop_id,
             nvl(order_nd.shop_name, pay_nd.shop_name)     shop_name,
             nvl(order_nd.shop_type, pay_nd.shop_type)     shop_type,
             nvl(order_amount, 0.0)                        order_amount,
             nvl(order_count, 0.0)                         order_count,
             nvl(order_user_count, 0.0)                    order_user_count,
             nvl(pay_suc_amount, 0.0)                      pay_suc_amount,
             nvl(order_nd.recent_days, pay_nd.recent_days) recent_days
      from (select shop_id,
                   shop_name,
                   shop_type,
                   order_amount,
                   order_count,
                   order_user_count,
                   recent_days
            from ${APP}.dws_trade_shop_order_nd
            where dt = '$do_date') order_nd
               full outer join
           (select shop_id,
                   shop_name,
                   shop_type,
                   pay_suc_amount,
                   recent_days
            from ${APP}.dws_trade_shop_pay_suc_nd
            where dt = '$do_date') pay_nd
           on order_nd.shop_id = pay_nd.shop_id
               and order_nd.recent_days = pay_nd.recent_days) nd_inter
         left join
     (select shop_id,
             shop_name,
             shop_type,
             refund_pay_suc_amount,
             recent_days
      from ${APP}.dws_trade_shop_refund_pay_suc_nd
      where dt = '$do_date') refund_nd
     on nd_inter.shop_id = refund_nd.shop_id
         and nd_inter.recent_days = refund_nd.recent_days;
"

ads_shop_type_trade_stats="
insert overwrite table ${APP}.ads_shop_type_trade_stats
select dt,
       recent_days,
       shop_type,
       order_amount,
       order_count,
       order_user_count,
       pay_suc_amount,
       refund_pay_suc_amount
from ${APP}.ads_shop_type_trade_stats
union
select '$do_date' dt,
       full_1.recent_days,
       full_1.shop_type,
       cast(order_amount as decimal(16, 2)),
       order_count,
       order_user_count,
       cast(pay_suc_amount as decimal(16, 2)),
       cast(refund_pay_suc_amount as decimal(16, 2))
from (select 1                                            recent_days,
             nvl(1d_inter.shop_type, refund_1d.shop_type) shop_type,
             nvl(order_amount, 0.0)                       order_amount,
             nvl(order_count, 0.0)                        order_count,
             nvl(pay_suc_amount, 0.0)                     pay_suc_amount,
             nvl(refund_pay_suc_amount, 0.0)              refund_pay_suc_amount
      from (select nvl(order_1d.shop_type, pay_1d.shop_type) shop_type,
                   nvl(order_amount, 0.0)                    order_amount,
                   nvl(order_count, 0.0)                     order_count,
                   nvl(pay_suc_amount, 0.0)                  pay_suc_amount
            from (select shop_type,
                         sum(order_amount) order_amount,
                         sum(order_count)  order_count
                  from ${APP}.dws_trade_shop_order_1d
                  where dt = '$do_date'
                  group by shop_type
                 ) order_1d
                     full outer join
                 (select shop_type,
                         sum(pay_suc_amount) pay_suc_amount
                  from ${APP}.dws_trade_shop_pay_suc_1d
                  where dt = '$do_date'
                  group by shop_type) pay_1d
                 on order_1d.shop_type = pay_1d.shop_type) 1d_inter
               left join
           (select shop_type,
                   sum(refund_pay_suc_amount) refund_pay_suc_amount
            from ${APP}.dws_trade_shop_refund_pay_suc_1d
            where dt = '$do_date'
            group by shop_type) refund_1d
           on 1d_inter.shop_type = refund_1d.shop_type
      union all
      select nvl(nd_inter.recent_days, refund_nd.recent_days) recent_days,
             nvl(nd_inter.shop_type, refund_nd.shop_type)     shop_type,
             nvl(order_amount, 0.0)                           order_amount,
             nvl(order_count, 0.0)                            order_count,
             nvl(pay_suc_amount, 0.0)                         pay_suc_amount,
             nvl(refund_pay_suc_amount, 0.0)                  refund_pay_suc_amount
      from (select nvl(order_nd.shop_type, pay_nd.shop_type)     shop_type,
                   nvl(order_amount, 0.0)                        order_amount,
                   nvl(order_count, 0.0)                         order_count,
                   nvl(pay_suc_amount, 0.0)                      pay_suc_amount,
                   nvl(order_nd.recent_days, pay_nd.recent_days) recent_days
            from (select shop_type,
                         sum(order_amount) order_amount,
                         sum(order_count)  order_count,
                         recent_days
                  from ${APP}.dws_trade_shop_order_nd
                  where dt = '$do_date'
                  group by shop_type,
                           recent_days) order_nd
                     full outer join
                 (select shop_type,
                         sum(pay_suc_amount) pay_suc_amount,
                         recent_days
                  from ${APP}.dws_trade_shop_pay_suc_nd
                  where dt = '$do_date'
                  group by shop_type,
                           recent_days) pay_nd
                 on order_nd.shop_type = pay_nd.shop_type
                     and order_nd.recent_days = pay_nd.recent_days) nd_inter
               left join
           (select shop_type,
                   sum(refund_pay_suc_amount) refund_pay_suc_amount,
                   recent_days
            from ${APP}.dws_trade_shop_refund_pay_suc_nd
            where dt = '$do_date'
            group by shop_type,
                     recent_days) refund_nd
           on nd_inter.shop_type = refund_nd.shop_type
               and nd_inter.recent_days = refund_nd.recent_days) full_1
         left join
     (select shop_type,
             recent_days,
             count(distinct customer_id) order_user_count
      from (select customer_id,
                   dt,
                   shop_type
            from (select shop_id,
                         customer_id,
                         dt
                  from ${APP}.dwd_trade_order_detail_inc
                  where dt >= date_add('$do_date', -29)) detail
                     left join (select id,
                                       type shop_type
                                from ${APP}.dim_shop_full
                                where dt = '$do_date') shop
                               on detail.shop_id = shop.id) t1
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by shop_type,
               recent_days) full_2
     on full_1.shop_type = full_2.shop_type
         and full_1.recent_days = full_2.recent_days;
"

ads_province_trade_stats="
insert overwrite table ${APP}.ads_province_trade_stats
select dt,
       recent_days,
       province_id,
       province_name,
       order_amount,
       order_count,
       order_user_count,
       pay_suc_amount,
       refund_pay_suc_amount
from ${APP}.ads_province_trade_stats
union
select '$do_date' dt,
       full_1.recent_days,
       full_1.province_id,
       full_1.province_name,
       cast(order_amount as decimal(16, 2)),
       order_count,
       order_user_count,
       cast(pay_suc_amount as decimal(16, 2)),
       cast(refund_pay_suc_amount as decimal(16, 2))
from (select 1                                                    recent_days,
             nvl(1d_inter.province_id, refund_1d.province_id)     province_id,
             nvl(1d_inter.province_name, refund_1d.province_name) province_name,
             nvl(order_amount, 0.0)                               order_amount,
             nvl(order_count, 0.0)                                order_count,
             nvl(pay_suc_amount, 0.0)                             pay_suc_amount,
             nvl(refund_pay_suc_amount, 0.0)                      refund_pay_suc_amount
      from (select nvl(order_1d.province_id, pay_1d.province_id)     province_id,
                   nvl(order_1d.province_name, pay_1d.province_name) province_name,
                   nvl(order_amount, 0.0)                            order_amount,
                   nvl(order_count, 0.0)                             order_count,
                   nvl(pay_suc_amount, 0.0)                          pay_suc_amount
            from (select province_id,
                         province_name,
                         sum(order_amount) order_amount,
                         sum(order_count)  order_count
                  from ${APP}.dws_trade_shop_order_1d
                  where dt = '$do_date'
                  group by province_id,
                           province_name) order_1d
                     full outer join
                 (select province_id,
                         province_name,
                         sum(pay_suc_amount) pay_suc_amount
                  from ${APP}.dws_trade_shop_pay_suc_1d
                  where dt = '$do_date'
                  group by province_id,
                           province_name) pay_1d
                 on order_1d.province_id = pay_1d.province_id
                     and order_1d.province_name = pay_1d.province_name) 1d_inter
               left join
           (select province_id,
                   province_name,
                   sum(refund_pay_suc_amount) refund_pay_suc_amount
            from ${APP}.dws_trade_shop_refund_pay_suc_1d
            where dt = '$do_date'
            group by province_id,
                     province_name) refund_1d
           on 1d_inter.province_id = refund_1d.province_id
               and 1d_inter.province_name = refund_1d.province_name
      union all
      select nvl(nd_inter.recent_days, refund_nd.recent_days)     recent_days,
             nvl(nd_inter.province_id, refund_nd.province_id)     province_id,
             nvl(nd_inter.province_name, refund_nd.province_name) province_name,
             nvl(order_amount, 0.0)                               order_amount,
             nvl(order_count, 0.0)                                order_count,
             nvl(pay_suc_amount, 0.0)                             pay_suc_amount,
             nvl(refund_pay_suc_amount, 0.0)                      refund_pay_suc_amount
      from (select nvl(order_nd.province_id, pay_nd.province_id)     province_id,
                   nvl(order_nd.province_name, pay_nd.province_name) province_name,
                   nvl(order_amount, 0.0)                            order_amount,
                   nvl(order_count, 0.0)                             order_count,
                   nvl(pay_suc_amount, 0.0)                          pay_suc_amount,
                   nvl(order_nd.recent_days, pay_nd.recent_days)     recent_days
            from (select province_id,
                         province_name,
                         sum(order_amount) order_amount,
                         sum(order_count)  order_count,
                         recent_days
                  from ${APP}.dws_trade_shop_order_nd
                  where dt = '$do_date'
                  group by province_id,
                           province_name,
                           recent_days) order_nd
                     full outer join
                 (select province_id,
                         province_name,
                         sum(pay_suc_amount) pay_suc_amount,
                         recent_days
                  from ${APP}.dws_trade_shop_pay_suc_nd
                  where dt = '$do_date'
                  group by province_id,
                           province_name,
                           recent_days) pay_nd
                 on order_nd.province_id = pay_nd.province_id
                     and order_nd.province_name = pay_nd.province_name
                     and order_nd.recent_days = pay_nd.recent_days) nd_inter
               left join
           (select province_id,
                   province_name,
                   sum(refund_pay_suc_amount) refund_pay_suc_amount,
                   recent_days
            from ${APP}.dws_trade_shop_refund_pay_suc_nd
            where dt = '$do_date'
            group by province_id,
                     province_name,
                     recent_days) refund_nd
           on nd_inter.province_id = refund_nd.province_id
               and nd_inter.province_name = refund_nd.province_name
               and nd_inter.recent_days = refund_nd.recent_days) full_1
         left join
     (select province_id,
             recent_days,
             count(distinct customer_id) order_user_count
      from (select customer_id,
                   dt,
                   province_id
            from (select shop_id,
                         customer_id,
                         dt
                  from ${APP}.dwd_trade_order_detail_inc
                  where dt >= date_add('$do_date', -29)) detail
                     left join (select id,
                                       region_id city_id
                                from ${APP}.dim_shop_full
                                where dt = '$do_date') shop
                               on detail.shop_id = shop.id
                     left join (select id,
                                       superior_region province_id
                                from ${APP}.dim_region_full
                                where dt = '$do_date') region
                               on city_id = region.id) t1
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by province_id,
               recent_days) full_2
     on full_1.province_id = full_2.province_id
         and full_1.recent_days = full_2.recent_days;
"

ads_city_trade_stats="
insert overwrite table ${APP}.ads_city_trade_stats
select dt,
       recent_days,
       city_id,
       city_name,
       order_amount,
       order_count,
       order_user_count,
       pay_suc_amount,
       refund_pay_suc_amount
from ${APP}.ads_city_trade_stats
union
select '$do_date' dt,
       full_1.recent_days,
       full_1.city_id,
       full_1.city_name,
       cast(order_amount as decimal(16, 2)),
       order_count,
       order_user_count,
       cast(pay_suc_amount as decimal(16, 2)),
       cast(refund_pay_suc_amount as decimal(16, 2))
from (select 1                                            recent_days,
             nvl(1d_inter.city_id, refund_1d.city_id)     city_id,
             nvl(1d_inter.city_name, refund_1d.city_name) city_name,
             nvl(order_amount, 0.0)                       order_amount,
             nvl(order_count, 0.0)                        order_count,
             nvl(pay_suc_amount, 0.0)                     pay_suc_amount,
             nvl(refund_pay_suc_amount, 0.0)              refund_pay_suc_amount
      from (select nvl(order_1d.city_id, pay_1d.city_id)     city_id,
                   nvl(order_1d.city_name, pay_1d.city_name) city_name,
                   nvl(order_amount, 0.0)                    order_amount,
                   nvl(order_count, 0.0)                     order_count,
                   nvl(pay_suc_amount, 0.0)                  pay_suc_amount
            from (select city_id,
                         city_name,
                         sum(order_amount) order_amount,
                         sum(order_count)  order_count
                  from ${APP}.dws_trade_shop_order_1d
                  where dt = '$do_date'
                  group by city_id,
                           city_name) order_1d
                     full outer join
                 (select city_id,
                         city_name,
                         sum(pay_suc_amount) pay_suc_amount
                  from ${APP}.dws_trade_shop_pay_suc_1d
                  where dt = '$do_date'
                  group by city_id,
                           city_name) pay_1d
                 on order_1d.city_id = pay_1d.city_id
                     and order_1d.city_name = pay_1d.city_name) 1d_inter
               left join
           (select city_id,
                   city_name,
                   sum(refund_pay_suc_amount) refund_pay_suc_amount
            from ${APP}.dws_trade_shop_refund_pay_suc_1d
            where dt = '$do_date'
            group by city_id,
                     city_name) refund_1d
           on 1d_inter.city_id = refund_1d.city_id
               and 1d_inter.city_name = refund_1d.city_name
      union all
      select nvl(nd_inter.recent_days, refund_nd.recent_days) recent_days,
             nvl(nd_inter.city_id, refund_nd.city_id)         city_id,
             nvl(nd_inter.city_name, refund_nd.city_name)     city_name,
             nvl(order_amount, 0.0)                           order_amount,
             nvl(order_count, 0.0)                            order_count,
             nvl(pay_suc_amount, 0.0)                         pay_suc_amount,
             nvl(refund_pay_suc_amount, 0.0)                  refund_pay_suc_amount
      from (select nvl(order_nd.city_id, pay_nd.city_id)         city_id,
                   nvl(order_nd.city_name, pay_nd.city_name)     city_name,
                   nvl(order_amount, 0.0)                        order_amount,
                   nvl(order_count, 0.0)                         order_count,
                   nvl(pay_suc_amount, 0.0)                      pay_suc_amount,
                   nvl(order_nd.recent_days, pay_nd.recent_days) recent_days
            from (select city_id,
                         city_name,
                         sum(order_amount) order_amount,
                         sum(order_count)  order_count,
                         recent_days
                  from ${APP}.dws_trade_shop_order_nd
                  where dt = '$do_date'
                  group by city_id,
                           city_name,
                           recent_days) order_nd
                     full outer join
                 (select city_id,
                         city_name,
                         sum(pay_suc_amount) pay_suc_amount,
                         recent_days
                  from ${APP}.dws_trade_shop_pay_suc_nd
                  where dt = '$do_date'
                  group by city_id,
                           city_name,
                           recent_days) pay_nd
                 on order_nd.city_id = pay_nd.city_id
                     and order_nd.city_name = pay_nd.city_name
                     and order_nd.recent_days = pay_nd.recent_days) nd_inter
               left join
           (select city_id,
                   city_name,
                   sum(refund_pay_suc_amount) refund_pay_suc_amount,
                   recent_days
            from ${APP}.dws_trade_shop_refund_pay_suc_nd
            where dt = '$do_date'
            group by city_id,
                     city_name,
                     recent_days) refund_nd
           on nd_inter.city_id = refund_nd.city_id
               and nd_inter.city_name = refund_nd.city_name
               and nd_inter.recent_days = refund_nd.recent_days) full_1
         left join
     (select city_id,
             recent_days,
             count(distinct customer_id) order_user_count
      from (select customer_id,
                   dt,
                   city_id
            from (select shop_id,
                         customer_id,
                         dt
                  from ${APP}.dwd_trade_order_detail_inc
                  where dt >= date_add('$do_date', -29)) detail
                     left join (select id,
                                       region_id city_id
                                from ${APP}.dim_shop_full
                                where dt = '$do_date') shop
                               on detail.shop_id = shop.id) t1
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by city_id,
               recent_days) full_2
     on full_1.city_id = full_2.city_id
         and full_1.recent_days = full_2.recent_days;
"

ads_hour_stats="
insert overwrite table ${APP}.ads_hour_stats
select dt,
       hour,
       order_amount,
       order_count,
       order_user_count
from ${APP}.ads_hour_stats
union
select dt,
       date_format(order_time, 'yyyy-MM-dd HH')         hour,
       cast(sum(split_actual_amount) as decimal(16, 2)) order_amount,
       count(distinct order_info_id)                    order_count,
       count(distinct customer_id)                      order_user_count
from ${APP}.dwd_trade_order_detail_inc
where dt = '$do_date'
group by dt,
         date_format(order_time, 'yyyy-MM-dd HH');
"

ads_order_reduce_stats="
insert overwrite table ${APP}.ads_order_reduce_stats
select dt,
       recent_days,
       order_reduce_amount
from ${APP}.ads_order_reduce_stats
union
select dt,
       1                                                recent_days,
       cast(sum(order_reduce_amount) as decimal(16, 2)) order_reduce_amount
from ${APP}.dws_trade_shop_order_1d
where dt = '$do_date'
group by dt
union
select dt,
       recent_days,
       cast(sum(order_reduce_amount) as decimal(16, 2)) order_reduce_amount
from ${APP}.dws_trade_shop_order_nd
where dt = '$do_date'
group by dt,
         recent_days;
"

ads_shop_order_reduce_stats="
insert overwrite table ${APP}.ads_shop_order_reduce_stats
select dt,
       recent_days,
       shop_id,
       shop_name,
       shop_type,
       order_reduce_amount
from ${APP}.ads_shop_order_reduce_stats
union
select dt,
       1 recent_days,
       shop_id,
       shop_name,
       shop_type,
       order_reduce_amount
from ${APP}.dws_trade_shop_order_1d
where dt = '$do_date'
union
select dt,
       recent_days,
       shop_id,
       shop_name,
       shop_type,
       order_reduce_amount
from ${APP}.dws_trade_shop_order_nd
where dt = '$do_date';
"

ads_reduce_share_stats="
insert overwrite table ${APP}.ads_reduce_share_stats
select dt,
       recent_days,
       order_reduce_direct_share_amount,
       order_reduce_franchise_share_amount,
       order_reduce_company_share_amount
from ${APP}.ads_reduce_share_stats
union
select dt,
       1 recent_days,
       cast(sum(if(shop_type = '1', reduce_shop_share_amount, 0.0)) as decimal(16, 2))
         order_reduce_direct_share_amount,
       cast(sum(if(shop_type = '2', reduce_shop_share_amount, 0.0)) as decimal(16, 2))
         order_reduce_franchise_share_amount,
       cast(sum(reduce_company_share_amount) as decimal(16, 2))
         reduce_company_share_amount
from ${APP}.dws_trade_shop_pay_suc_1d
where dt = '$do_date'
group by dt
union
select dt,
       recent_days,
       cast(sum(if(shop_type = '1', reduce_shop_share_amount, 0.0)) as decimal(16, 2))
           order_reduce_direct_share_amount,
       cast(sum(if(shop_type = '2', reduce_shop_share_amount, 0.0)) as decimal(16, 2))
           order_reduce_franchise_share_amount,
       cast(sum(reduce_company_share_amount) as decimal(16, 2))
           reduce_company_share_amount
from ${APP}.dws_trade_shop_pay_suc_nd
where dt = '$do_date'
group by dt,
         recent_days;
"

ads_promotion_trade_stats="
insert overwrite table ${APP}.ads_promotion_trade_stats
select dt,
       recent_days,
       promotion_id,
       company_share,
       promotion_name,
       promotion_reduce_amount,
       promotion_threshold_amount,
       total_reduce_amount,
       total_activity_order_count,
       total_activity_user_count
from ${APP}.ads_promotion_trade_stats
union
select dt,
       1 recent_days,
       promotion_id,
       company_share,
       promotion_name,
       promotion_reduce_amount,
       promotion_threshold_amount,
       total_reduce_amount,
       total_activity_order_count,
       total_activity_user_count
from ${APP}.dws_trade_promotion_order_1d
where dt = '$do_date'
union
select dt,
       recent_days,
       promotion_id,
       company_share,
       promotion_name,
       promotion_reduce_amount,
       promotion_threshold_amount,
       total_reduce_amount,
       total_activity_order_count,
       total_activity_user_count
from ${APP}.dws_trade_promotion_order_nd
where dt = '$do_date';
"

ads_product_sku_trade_stats="
insert overwrite table ${APP}.ads_product_sku_trade_stats
select dt,
       recent_days,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.ads_product_sku_trade_stats
union
select dt,
       1 recent_days,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.dws_trade_product_sku_order_1d
where dt = '$do_date'
union
select dt,
       recent_days,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.dws_trade_product_sku_order_nd
where dt = '$do_date';
"

ads_product_spu_trade_stats="
insert overwrite table ${APP}.ads_product_spu_trade_stats
select dt,
       recent_days,
       product_spu_id,
       product_spu_description,
       product_spu_name,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.ads_product_spu_trade_stats
union
select '$do_date'                                dt,
       full_1.recent_days,
       full_1.product_spu_id,
       product_spu_description,
       product_spu_name,
       cast(order_amount as decimal(16, 2))        order_amount,
       order_user_count,
       cast(order_reduce_amount as decimal(16, 2)) order_reduce_amount
from (select 1                        recent_days,
             product_spu_id,
             product_spu_description,
             product_spu_name,
             sum(order_amount)        order_amount,
             sum(order_reduce_amount) order_reduce_amount
      from ${APP}.dws_trade_product_sku_order_1d
      where dt = '$do_date'
      group by product_spu_id,
               product_spu_description,
               product_spu_name
      union all
      select recent_days,
             product_spu_id,
             product_spu_description,
             product_spu_name,
             sum(order_amount)        order_amount,
             sum(order_reduce_amount) order_reduce_amount
      from ${APP}.dws_trade_product_sku_order_nd
      where dt = '$do_date'
      group by recent_days,
               product_spu_id,
               product_spu_description,
               product_spu_name) full_1
         left join
     (select recent_days,
             product_spu_id,
             count(distinct customer_id) order_user_count
      from (select dt,
                   product_spu_id,
                   customer_id
            from (select dt,
                         product_sku_id,
                         customer_id
                  from ${APP}.dwd_trade_order_detail_inc
                  where dt >= date_add('$do_date', -29)) detail
                     left join
                 (select id,
                         product_spu_id
                  from ${APP}.dim_product_sku_full
                  where dt = '$do_date') sku
                 on detail.product_sku_id = sku.id) t1
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by recent_days,
               product_spu_id) full_2
     on full_1.recent_days = full_2.recent_days
         and full_1.product_spu_id = full_2.product_spu_id;
"

ads_product_category_trade_stats="
insert overwrite table ${APP}.ads_product_category_trade_stats
select dt,
       recent_days,
       product_category_id,
       product_category_description,
       product_category_name,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.ads_product_category_trade_stats
union
select '$do_date'                                dt,
       full_1.recent_days,
       full_1.product_category_id,
       product_category_description,
       product_category_name,
       cast(order_amount as decimal(16, 2))        order_amount,
       order_user_count,
       cast(order_reduce_amount as decimal(16, 2)) order_reduce_amount
from (select 1                        recent_days,
             product_category_id,
             product_category_description,
             product_category_name,
             sum(order_amount)        order_amount,
             sum(order_reduce_amount) order_reduce_amount
      from ${APP}.dws_trade_product_sku_order_1d
      where dt = '$do_date'
      group by product_category_id,
               product_category_description,
               product_category_name
      union all
      select recent_days,
             product_category_id,
             product_category_description,
             product_category_name,
             sum(order_amount)        order_amount,
             sum(order_reduce_amount) order_reduce_amount
      from ${APP}.dws_trade_product_sku_order_nd
      where dt = '$do_date'
      group by recent_days,
               product_category_id,
               product_category_description,
               product_category_name) full_1
         left join
     (select recent_days,
             product_category_id,
             count(distinct customer_id) order_user_count
      from (select dt,
                   product_category_id,
                   customer_id
            from (select dt,
                         product_sku_id,
                         customer_id
                  from ${APP}.dwd_trade_order_detail_inc
                  where dt >= date_add('$do_date', -29)) detail
                     left join
                 (select id,
                         product_category_id
                  from ${APP}.dim_product_sku_full
                  where dt = '$do_date') sku
                 on detail.product_sku_id = sku.id) t1
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by recent_days,
               product_category_id) full_2
     on full_1.recent_days = full_2.recent_days
         and full_1.product_category_id = full_2.product_category_id;
"

ads_product_sku_hour_stats="
insert overwrite table ${APP}.ads_product_sku_hour_stats
select dt,
       hour,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_amount,
       order_count,
       order_user_count
from ${APP}.ads_product_sku_hour_stats
union
select '$do_date'                         dt,
       hour,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       cast(order_amount as decimal(16, 2)) order_amount,
       order_count,
       order_user_count
from (select product_sku_id,
             date_format(order_time, 'yyyy-MM-dd HH') hour,
             sum(split_actual_amount)                 order_amount,
             count(distinct order_info_id)            order_count,
             count(distinct customer_id)              order_user_count
      from ${APP}.dwd_trade_order_detail_inc
      where dt = '$do_date'
      group by product_sku_id,
               date_format(order_time, 'yyyy-MM-dd HH')) agg
         left join
     (select id,
             name  product_sku_name,
             price product_sku_price
      from ${APP}.dim_product_sku_full
      where dt = '$do_date') sku
     on agg.product_sku_id = sku.id;
"

ads_product_group_trade_stats="
insert overwrite table ${APP}.ads_product_group_trade_stats
select dt,
       recent_days,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.ads_product_group_trade_stats
union
select dt,
       1 recent_days,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.dws_trade_product_group_order_1d
where dt = '$do_date'
union
select dt,
       recent_days,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_user_count,
       order_reduce_amount
from ${APP}.dws_trade_product_group_order_nd
where dt = '$do_date';
"

ads_product_group_hour_stats="
insert overwrite table ${APP}.ads_product_group_hour_stats
select dt,
       hour,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_count,
       order_user_count
from ${APP}.ads_product_group_hour_stats
union
select '$do_date' dt,
       hour,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       order_amount,
       order_count,
       order_user_count
from (select date_format(order_time, 'yyyy-MM-dd HH') hour,
             product_group_id,
             sum(split_actual_amount)                 order_amount,
             count(distinct order_info_id)            order_count,
             count(distinct customer_id)              order_user_count
      from ${APP}.dwd_trade_order_detail_inc
      where dt = '$do_date'
        and product_group_id is not null
      group by date_format(order_time, 'yyyy-MM-dd HH'),
               product_group_id) agg
         left join
     (select id,
             name            product_group_name,
             original_price  product_group_original_price,
             price           product_group_price,
             product_sku_ids product_group_sku_ids
      from ${APP}.dim_product_group_full
      where dt = '$do_date') product_group
     on agg.product_group_id = product_group.id;
"

ads_product_sku_reduce_amount_top10="
insert overwrite table ${APP}.ads_product_sku_reduce_amount_top10
select dt,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_reduce_amount,
       rk
from ${APP}.ads_product_sku_reduce_amount_top10
union
select '$do_date' dt,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_reduce_amount,
       rk
from (select product_sku_id,
             product_sku_name,
             product_sku_price,
             order_reduce_amount,
             rank() over (order by order_reduce_amount desc) rk
      from ${APP}.dws_trade_product_sku_order_1d
      where dt = '$do_date') with_rk
where rk <= 10;
"

ads_product_sku_reduce_order_count_top10="
insert overwrite table ${APP}.ads_product_sku_reduce_order_count_top10
select dt,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_reduce_count,
       rk
from ${APP}.ads_product_sku_reduce_order_count_top10
union
select '$do_date' dt,
       product_sku_id,
       product_sku_name,
       product_sku_price,
       order_reduce_count,
       rk
from (select product_sku_id,
             product_sku_name,
             product_sku_price,
             order_reduce_count,
             rank() over (order by order_reduce_count desc) rk
      from ${APP}.dws_trade_product_sku_order_1d
      where dt = '$do_date') with_rk
where rk <= 10;
"

ads_shop_reduce_amount_top10="
insert overwrite table ${APP}.ads_shop_reduce_amount_top10
select dt,
       shop_id,
       shop_name,
       shop_type,
       order_reduce_amount,
       rk
from ${APP}.ads_shop_reduce_amount_top10
union
select '$do_date' dt,
       shop_id,
       shop_name,
       shop_type,
       order_reduce_amount,
       rk
from (select shop_id,
             shop_name,
             shop_type,
             order_reduce_amount,
             rank() over (order by order_reduce_amount desc) rk
      from ${APP}.dws_trade_shop_order_1d
      where dt = '$do_date') with_rk
where rk <= 10;
"

ads_shop_reduce_order_count_top10="
insert overwrite table ${APP}.ads_shop_reduce_order_count_top10
select dt,
       shop_id,
       shop_name,
       shop_type,
       order_reduce_count,
       rk
from ${APP}.ads_shop_reduce_order_count_top10
union
select '$do_date' dt,
       shop_id,
       shop_name,
       shop_type,
       order_reduce_count,
       rk
from (select shop_id,
             shop_name,
             shop_type,
             order_reduce_count,
             rank() over (order by order_reduce_count desc) rk
      from ${APP}.dws_trade_shop_order_1d
      where dt = '$do_date') with_rk
where rk <= 10;
"

ads_shop_order_amount_top10="
insert overwrite table ${APP}.ads_shop_order_amount_top10
select dt,
       shop_id,
       shop_name,
       shop_type,
       order_amount,
       rk
from ${APP}.ads_shop_order_amount_top10
union
select '$do_date' dt,
       shop_id,
       shop_name,
       shop_type,
       order_amount,
       rk
from (select shop_id,
             shop_name,
             shop_type,
             order_amount,
             rank() over (order by order_amount desc) rk
      from ${APP}.dws_trade_shop_order_1d
      where dt = '$do_date') with_rk
where rk <= 10;
"

ads_direct_shop_order_amount_top10="
insert overwrite table ${APP}.ads_direct_shop_order_amount_top10
select dt,
       shop_id,
       shop_name,
       shop_type,
       order_amount,
       rk
from ${APP}.ads_direct_shop_order_amount_top10
union
select '$do_date' dt,
       shop_id,
       shop_name,
       shop_type,
       order_amount,
       rk
from (select shop_id,
             shop_name,
             shop_type,
             order_amount,
             rank() over (order by order_amount desc) rk
      from ${APP}.dws_trade_shop_order_1d
      where dt = '$do_date'
        and shop_type = '1') with_rk
where rk <= 10;
"

ads_join_shop_order_amount_top10="
insert overwrite table ${APP}.ads_join_shop_order_amount_top10
select dt,
       shop_id,
       shop_name,
       shop_type,
       order_amount,
       rk
from ${APP}.ads_join_shop_order_amount_top10
union
select '$do_date' dt,
       shop_id,
       shop_name,
       shop_type,
       order_amount,
       rk
from (select shop_id,
             shop_name,
             shop_type,
             order_amount,
             rank() over (order by order_amount desc) rk
      from ${APP}.dws_trade_shop_order_1d
      where dt = '$do_date'
        and shop_type = '2') with_rk
where rk <= 10;
"

ads_product_spu_comment_stats="
insert overwrite table ${APP}.ads_product_spu_comment_stats
select dt,
       recent_days,
       product_spu_id,
       product_spu_description,
       product_spu_name,
       comment_count,
       good_comment_count,
       avg_rating,
       good_comment_rate
from ${APP}.ads_product_spu_comment_stats
union
select '$do_date'                              dt,
       full_1.recent_days,
       full_1.product_spu_id,
       product_spu_description,
       product_spu_name,
       comment_count,
       good_comment_count,
       cast(avg_rating as decimal(16, 2))        avg_rating,
       cast(good_comment_rate as decimal(16, 2)) good_comment_rate
from (select 1                                  recent_days,
             product_spu_id,
             product_spu_description,
             product_spu_name,
             comment_count,
             good_comment_count,
             good_comment_count / comment_count good_comment_rate
      from ${APP}.dws_interaction_product_spu_comment_1d
      where dt = '$do_date'
      union all
      select recent_days,
             product_spu_id,
             product_spu_description,
             product_spu_name,
             comment_count,
             good_comment_count,
             good_comment_count / comment_count good_comment_rate
      from ${APP}.dws_interaction_product_spu_comment_nd
      where dt = '$do_date') full_1
         left join
     (select recent_days,
             product_spu_id,
             avg(rating) avg_rating
      from (select dt,
product_spu_id,
                   max(rating) rating
            from (select dt,
product_sku_id,
                         order_info_id,
                         rating
                  from ${APP}.dwd_interaction_comment_inc
                  where dt >= date_add('$do_date', -29)) detail
                     left join (select id,
                                       product_spu_id
                                from ${APP}.dim_product_sku_full
                                where dt = '$do_date') product_sku
                               on detail.product_sku_id = product_sku.id
            group by dt,
product_spu_id,
                     order_info_id) t1
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      where dt >= date_add('$do_date', -recent_days + 1)
      group by recent_days,
               product_spu_id) full_2
     on full_1.recent_days = full_2.recent_days
         and full_1.product_spu_id = full_2.product_spu_id;
"

ads_product_group_comment_stats="
insert overwrite table ${APP}.ads_product_group_comment_stats
select dt,
       recent_days,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       comment_count,
       good_comment_count,
       avg_rating,
       good_comment_rate
from ${APP}.ads_product_group_comment_stats
union
select '$do_date'                                                 dt,
       1                                                            recent_days,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       comment_count,
       good_comment_count,
       cast(total_comment_rating / comment_count as decimal(16, 2)) avg_rating,
       cast(good_comment_count / comment_count as decimal(16, 2))   good_comment_rate
from ${APP}.dws_interaction_product_group_comment_1d
where dt = '$do_date'
union
select '$do_date'                                                 dt,
       recent_days,
       product_group_id,
       product_group_name,
       product_group_original_price,
       product_group_price,
       product_group_sku_ids,
       comment_count,
       good_comment_count,
       cast(total_comment_rating / comment_count as decimal(16, 2)) avg_rating,
       cast(good_comment_count / comment_count as decimal(16, 2))   good_comment_rate
from ${APP}.dws_interaction_product_group_comment_nd
where dt = '$do_date';
"

ads_shop_comment_stats="
insert overwrite table ${APP}.ads_shop_comment_stats
select dt,
       recent_days,
       shop_id,
       shop_name,
       shop_type,
       comment_count,
       good_comment_count,
       avg_rating,
       good_comment_rate
from ${APP}.ads_shop_comment_stats
union
select '$do_date'                              dt,
       full_1.recent_days,
       full_1.shop_id,
       shop_name,
       shop_type,
       comment_count,
       good_comment_count,
       cast(avg_rating as decimal(16, 2))        avg_rating,
       cast(good_comment_rate as decimal(16, 2)) good_comment_rate
from (select 1                                  recent_days,
             shop_id,
             shop_name,
             shop_type,
             comment_count,
             good_comment_count,
             good_comment_count / comment_count good_comment_rate
      from ${APP}.dws_interaction_shop_comment_1d
      where dt = '$do_date'
      union all
      select recent_days,
             shop_id,
             shop_name,
             shop_type,
             comment_count,
             good_comment_count,
             good_comment_count / comment_count good_comment_rate
      from ${APP}.dws_interaction_shop_comment_nd
      where dt = '$do_date') full_1
         left join
     (select recent_days,
             shop_id,
             avg(rating) avg_rating
      from (select shop_id,
                   max(rating) rating
            from ${APP}.dwd_interaction_comment_inc
            where dt >= date_add('$do_date', -29)
            group by shop_id,
                     order_info_id) t1
               lateral view explode(array(1, 7, 30)) tmp as recent_days
      group by recent_days,
               shop_id) full_2
     on full_1.recent_days = full_2.recent_days
         and full_1.shop_id = full_2.shop_id;
"

case $1 in
ads_shop_trade_stats | ads_shop_type_trade_stats | ads_province_trade_stats | ads_city_trade_stats | ads_hour_stats | ads_order_reduce_stats | ads_shop_order_reduce_stats | ads_reduce_share_stats | ads_promotion_trade_stats | ads_product_sku_trade_stats | ads_product_spu_trade_stats | ads_product_category_trade_stats | ads_product_sku_hour_stats | ads_product_group_trade_stats | ads_product_group_hour_stats | ads_product_sku_reduce_amount_top10 | ads_product_sku_reduce_order_count_top10 | ads_shop_reduce_amount_top10 | ads_shop_reduce_order_count_top10 | ads_shop_order_amount_top10 | ads_direct_shop_order_amount_top10 | ads_join_shop_order_amount_top10 | ads_product_spu_comment_stats | ads_product_group_comment_stats | ads_shop_comment_stats)
    hive -e "${!1}"
    ;;
all)
    hive -e "$ads_shop_trade_stats$ads_shop_type_trade_stats$ads_province_trade_stats$ads_city_trade_stats$ads_hour_stats$ads_order_reduce_stats$ads_shop_order_reduce_stats$ads_reduce_share_stats$ads_promotion_trade_stats$ads_product_sku_trade_stats$ads_product_spu_trade_stats$ads_product_category_trade_stats$ads_product_sku_hour_stats$ads_product_group_trade_stats$ads_product_group_hour_stats$ads_product_sku_reduce_amount_top10$ads_product_sku_reduce_order_count_top10$ads_shop_reduce_amount_top10$ads_shop_reduce_order_count_top10$ads_shop_order_amount_top10$ads_direct_shop_order_amount_top10$ads_join_shop_order_amount_top10$ads_product_spu_comment_stats$ads_product_group_comment_stats$ads_shop_comment_stats"
    ;;
esac
