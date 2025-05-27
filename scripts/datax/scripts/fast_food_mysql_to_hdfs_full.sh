#!/bin/bash

DATAX_HOME=/opt/module/datax

# 如果传入日期则do_date等于传入的日期，否则等于前一天日期
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

# 处理目标路径，此处的处理逻辑是，如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
handle_targetdir() {
    hadoop fs -test -e $1
    if [[ $? -eq 1 ]]; then
        echo "路径$1不存在，正在创建......"
        hadoop fs -mkdir -p $1
    else
        echo "路径$1已经存在"
    fi
}

#数据同步
import_data() {
    for tab in $@
    do 
        datax_config=/opt/module/datax/job/import/fast_food.${tab}.json
        target_dir=/origin_data/fast_food/db/${tab}_full/$do_date

        echo "正在校验目录 $target_dir..."
        handle_targetdir $target_dir
        echo "将数据导入 $target_dir..."
        python $DATAX_HOME/bin/datax.py -p"-Dtargetdir=$target_dir" $datax_config >$DATAX_HOME/fast_food_import.log 2>&1

        if [[ $? -eq 1 ]]
        then 
            echo "数据导入出错，日志如下: "
            cat $DATAX_HOME/fast_food_import.log
        else
            echo "导入成功~"
        fi
    done
}

case $1 in
shop | region | promotion | product_spu_attr | product_spu_attr_value | product_spu | product_sku | product_group | product_group_sku | product_category)
    import_data $1
    ;;
all)
    import_data shop region promotion product_spu_attr product_spu_attr_value product_spu product_sku product_group product_group_sku product_category
    ;;
esac
