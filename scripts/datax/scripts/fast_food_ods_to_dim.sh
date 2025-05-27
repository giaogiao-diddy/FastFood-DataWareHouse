#!/bin/bash

APP=fast_food

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dim_customer_zip="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ${APP}.dim_customer_zip
    partition (dt)
select id,
       phone_number,
       username,
       start_date,
       if(rn = 1, end_date, date_add('$do_date', -1)) end_date,
       if(rn = 1, end_date, date_add('$do_date', -1)) dt
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
            from ${APP}.dim_customer_zip
            where dt = '9999-12-31'
            union all
            select id,
                   concat(substr(phone_number, 0, 3), '*')         phone_number,
                   concat(substr(username, length(username)), '*') username,
                   '$do_date'                                    start_date,
                   '9999-12-31'                                    end_date
            from (select data.id,
                         data.phone_number,
                         data.username,
                         row_number() over (partition by data.id order by ts desc) rk
                  from ${APP}.ods_customer_inc
                  where dt = '$do_date') t1
            where rk = 1) t2) t3;
"

dim_product_sku_full="
insert overwrite table ${APP}.dim_product_sku_full
    partition (dt = '$do_date')
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
      from ${APP}.ods_product_sku_full
      where dt = '$do_date') sku
         left join
     (select id,
             description,
             name
      from ${APP}.ods_product_category_full
      where dt = '$do_date') category
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
            from ${APP}.ods_product_spu_full
            where dt = '$do_date') spu
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
                  from ${APP}.ods_product_spu_attr_full
                  where dt = '$do_date') spu_attr
                     left join
                 (select id         product_spu_attr_value_id,
                         attr_value product_spu_attr_value,
                         product_spu_attr_id
                  from ${APP}.ods_product_spu_attr_value_full
                  where dt = '$do_date') spu_attr_value
                 on spu_attr.product_spu_attr_id = spu_attr_value.product_spu_attr_id
            group by spu_attr.product_spu_attr_id,
                     product_spu_attr_name,
                     product_spu_id) spu_attr_full
           on spu.product_spu_id = spu_attr_full.product_spu_id
      group by spu.product_spu_id,
               description,
               name) spu_full
     on sku.product_spu_id = spu_full.product_spu_id;
"

dim_product_group_full="
insert overwrite table ${APP}.dim_product_group_full
    partition (dt = '$do_date')
select id,
       name,
       original_price,
       price,
       collect_set(product_sku_id) product_sku_ids
from (select id,
             name,
             original_price,
             price
      from ${APP}.ods_product_group_full
      where dt = '$do_date') group_info
         left join
     (select product_group_id,
             product_sku_id
      from ${APP}.ods_product_group_sku_full
      where dt = '$do_date') group_sku
     on group_info.id = product_group_id
group by id,
         name,
         original_price,
         price;
"

dim_promotion_full="
insert overwrite table ${APP}.dim_promotion_full
    partition (dt = '$do_date')
select id,
       company_share,
       name,
       reduce_amount,
       threshold_amount
from ${APP}.ods_promotion_full
where dt = '$do_date';
"

dim_region_full="
insert overwrite table ${APP}.dim_region_full
    partition (dt = '$do_date')
select id,
       level,
       name,
       superior_region,
       zip_code
from ${APP}.ods_region_full
where dt = '$do_date';
"

dim_shop_full="
insert overwrite table ${APP}.dim_shop_full
    partition (dt = '$do_date')
select id,
       md5(name) name,
       concat(substr(phone_number, 0, 3), '*') phone_number,
       type,
       region_id
from ${APP}.ods_shop_full
where dt = '$do_date';
"

case $1 in
dim_customer_zip | dim_product_sku_full | dim_product_group_full | dim_promotion_full | dim_region_full | dim_shop_full)
    hive -e "${!1}"
;;
"all")
    hive -e "$dim_customer_zip$dim_product_sku_full$dim_product_group_full$dim_promotion_full$dim_region_full$dim_shop_full"
;;
esac
