#!/bin/bash

APP=fast_food

if [ -n "$2" ] ;then
   do_date=$2
else 
   do_date=`date -d '-1 day' +%F`
fi

load_data(){
    sql=""
    for i in $*; do
        #判断路径是否存在
        echo "正在校验源数据路径 /origin_data/$APP/db/${i:4}/$do_date..."
        hadoop fs -test -e /origin_data/$APP/db/${i:4}/$do_date
        #路径存在方可装载数据
        if [[ $? = 0 ]]
        then
            sql=$sql"load data inpath '/origin_data/$APP/db/${i:4}/$do_date' OVERWRITE into table ${APP}.$i partition(dt='$do_date');"
        else
            echo "数据源路径不存在，跳过" 
        fi
    done

    if [[ $sql != "" ]]
    then 
        echo "sql拼接完毕，开始执行..."
        hive -e "$sql"
        echo "执行完毕~"
    else 
        echo "sql拼接完毕，没有可执行的语句"
    fi
}

case $1 in
ods_shop_full | ods_region_full | ods_promotion_full | ods_product_sku_full | ods_product_spu_attr_full | ods_product_spu_attr_value_full | ods_product_spu_full | ods_product_group_full | ods_product_group_sku_full | ods_product_category_full | ods_customer_inc | ods_order_info_inc | ods_order_detail_inc | ods_order_status_log_inc | ods_payment_inc)
    load_data $1
    ;;
"all")
    load_data ods_shop_full ods_region_full ods_promotion_full ods_product_sku_full ods_product_spu_attr_full ods_product_spu_attr_value_full ods_product_spu_full ods_product_group_full ods_product_group_sku_full ods_product_category_full ods_customer_inc ods_order_info_inc ods_order_detail_inc ods_order_status_log_inc ods_payment_inc
    ;;
esac
