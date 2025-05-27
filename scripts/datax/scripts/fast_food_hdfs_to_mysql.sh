#! /bin/bash

DATAX_HOME=/opt/module/datax
DATAX_LOG=/opt/module/datax/fast_food_export.log

# DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_path(){
    export_dir=$1
    for target_file in `hadoop fs -ls -R $export_dir | awk '{print $8}'`
    do
        hadoop fs -test -z ${target_file}
        if [[ $? -eq 0 ]]
        then 
            echo "文件 ${target_file} 文件大小为0，正在删除......"
            hadoop fs -rm -f ${target_file}
        fi
    done
}

# 该函数用于导出数据
export_data(){
    for i in $*
    do
        export_dir="/warehouse/fast_food/ads/${i}"
        export_config="${DATAX_HOME}/job/fast_food/export/fast_food_report.${i}.json"
        # 判断路径是否存在
        hadoop fs -test -e ${export_dir}
        # 路径存在方可导出数据
        if [[ $? -eq 0 ]]
        then
            # 处理导出文件目录
            handle_path ${export_dir}
            # 处理完成后判断该目录下文件数量，若为0说明没有文件，不必导出
            count=$(hadoop fs -count ${export_dir} | awk '{print $2}')
            if [[ ${count} -eq 0 ]]
            then 
                echo "目录 ${export_dir} 为空！！！跳过~~~"
            else
                echo "正在处理 ${export_config}......"
                python ${DATAX_HOME}/bin/datax.py -p"-Dexportdir=${export_dir}" ${export_config} >${DATAX_LOG} 2>&1
                if [[ $? -eq 1 ]]
                then 
                    echo "处理出错，日志如下..."
                    cat ${DATAX_LOG}
                else
                    echo "处理完成~~~"
                fi
            fi
        else
            echo "目录 ${export_dir} 不存在！！！跳过~~~"
        fi
    done
}

case $1 in
ads_city_trade_stats | ads_direct_shop_order_amount_top10 | ads_hour_stats | ads_join_shop_order_amount_top10 | ads_order_reduce_stats | ads_product_category_trade_stats | ads_product_group_comment_stats | ads_product_group_hour_stats | ads_product_group_trade_stats | ads_product_sku_hour_stats | ads_product_sku_reduce_amount_top10 | ads_product_sku_reduce_order_count_top10 | ads_product_sku_trade_stats | ads_product_spu_comment_stats | ads_product_spu_trade_stats | ads_promotion_trade_stats | ads_province_trade_stats | ads_reduce_share_stats | ads_shop_comment_stats | ads_shop_order_amount_top10 | ads_shop_order_reduce_stats | ads_shop_reduce_amount_top10 | ads_shop_reduce_order_count_top10 | ads_shop_trade_stats | ads_shop_type_trade_stats)
    export_data $1
    ;;
all)
    export_data ads_city_trade_stats ads_direct_shop_order_amount_top10 ads_hour_stats ads_join_shop_order_amount_top10 ads_order_reduce_stats ads_product_category_trade_stats ads_product_group_comment_stats ads_product_group_hour_stats ads_product_group_trade_stats ads_product_sku_hour_stats ads_product_sku_reduce_amount_top10 ads_product_sku_reduce_order_count_top10 ads_product_sku_trade_stats ads_product_spu_comment_stats ads_product_spu_trade_stats ads_promotion_trade_stats ads_province_trade_stats ads_reduce_share_stats ads_shop_comment_stats ads_shop_order_amount_top10 ads_shop_order_reduce_stats ads_shop_reduce_amount_top10 ads_shop_reduce_order_count_top10 ads_shop_trade_stats ads_shop_type_trade_stats
    ;;
esac
